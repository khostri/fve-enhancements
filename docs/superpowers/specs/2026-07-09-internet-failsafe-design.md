# Internet Failsafe — Design Spec

Date: 2026-07-09
Status: Approved by user, pending write-up into implementation plan

## Problem

The Cerbo GX is remotely controlled by the electricity provider via VRM (grid setpoint, DC feed-in enable, and — by some mechanism not yet identified — PV production on/off), based on spot prices. This works well while the internet connection is up. When it drops, the last-received command persists indefinitely (Victron settings are not self-expiring), because ESS has no built-in heartbeat/timeout on remote control. This has caused a real incident: the system kept exporting from the battery at full power after a connection loss, until the battery was depleted, because no new command arrived to stop it.

## Goal

A standalone Node-RED flow that:
1. Continuously checks whether the path to Victron's remote-control infrastructure (VRM) is reachable.
2. After 5 minutes of continuous unreachability, forces the system into a safe state (no deliberate grid export/import, no forced DC feed-in), and restores PV production to normal if it happens to be curtailed.
3. Leaves control to the provider's remote commands again as soon as the connection recovers — takes no further action itself once reachability returns.
4. Is easy to extend with more "safe state" actions later, without touching existing wiring.

## Non-goals

- Does not attempt to replicate or second-guess the provider's price-based control logic.
- Does not modify `heatpump_relay_control.json` or share any node/group IDs with it.
- Does not try to distinguish *why* VRM is unreachable (local WAN down vs. Victron-side outage) — either way, the safe response is the same.

## Why past imports crashed Node-RED, and how this flow avoids it

Last time, Node-RED (now v4.1.1 on this Cerbo) crashed immediately on import, before the flow even rendered on the canvas — suggesting a structural/parse-time problem rather than a runtime resource issue. Root cause wasn't conclusively identified (possibly ID collisions from hand-edited IDs reused across an existing deployed flow). Mitigations for this flow:

- Entirely new tab, new groups, new node IDs — zero ID overlap with anything already deployed.
- Only uses node types already present in the installed palette (`@victronenergy/node-red-contrib-victron@1.6.60`) plus core Node-RED nodes (`inject`, `http request`, `catch`, `function`, `debug`) — no new palette modules to install.
- Small flow (~10 nodes) — resource exhaustion during deploy is not a realistic concern at this size.
- Delivered via **Import → new flow tab** in the editor, not merged into an existing tab.
- `flows.json` backed up over SSH before import, so a bad deploy is a restore-and-restart away from recovery.

## Architecture

```
[Tick 30s]──▶[HTTP HEAD vrm.victronenergy.com]──▶[Watchdog Evaluate]──┬──▶[Action 1: AcPowerSetPoint = 0]
                        │                              ▲              ├──▶[Action 2: OvervoltageFeedIn = 0]
                        └──(on network error)─▶[Catch node]           ├──▶[Action 3: restore PV — placeholder, unwired]
                                                                       └──▶[Status/Action debug log]
```

One new tab, "Internet Failsafe", containing:

| Node | Type | Purpose |
|---|---|---|
| Tick (30s) | `inject` (repeat) | Drives the check cycle |
| Check VRM | `http request` (core) | HEAD `https://vrm.victronenergy.com`, 5s timeout |
| VRM Check Error | `catch` (core, scoped to "Check VRM") | Catches network-level failures (DNS, timeout, connection refused) that the http request node would otherwise not emit on its normal output |
| Watchdog Evaluate | `function` | Counter, dry-run gate, fire-once/re-arm, produces action messages |
| Set Grid Setpoint | `victron-output-settings` → `/Settings/CGwacs/AcPowerSetPoint` | Action 1 |
| Set DC Feed-In Disabled | `victron-output-settings` → `/Settings/CGwacs/OvervoltageFeedIn` | Action 2 |
| *(unwired placeholder)* | TBD — `victron-output-custom` or a typed control node, once identified | Action 3 |
| Watchdog Status | `debug` (sidebar) | Logs every state transition and dry-run/live actions |

## Failure detection

Target: `https://vrm.victronenergy.com`, method `HEAD`, timeout 5s, checked every 30s. This is the actual domain the provider's remote commands are delivered through (per user: provider acts as an admin user on the VRM installation), so it's a closer proxy for "is remote control reachable" than a generic internet-connectivity check. Load is negligible (a HEAD request every 30s) and not a throttling risk.

**Important**: Node-RED's core `http request` node does not produce an output message on network-level failure (DNS failure, connection refused, timeout) — it only calls `node.error()`. Relying solely on its normal output would mean a full internet outage — the exact scenario this flow exists to catch — produces no signal at all. A `catch` node scoped to the HTTP request node is wired into the same evaluation point to close this gap.

Evaluation rule (applied uniformly to both the normal HTTP output and the caught-error message): success if `msg.statusCode` is present and in the 200–299 range; failure otherwise (this covers non-2xx responses, timeouts, and connection errors in one check, since caught-error messages have no `statusCode`).

## Watchdog logic

State kept in node context: `failCount` (integer), `fired` (boolean).

- On failure: `failCount += 1`.
- On success: `failCount = 0` immediately (no gradual decay/decrement — unlike the heatpump flow's oscillation-damping counter, this doesn't need hysteresis: a connection is either up or down, and a single successful check is a reliable signal that it's up).
- Threshold: `failCount >= 10` (30s interval × 10 = 5 minutes).
- **Fire-once**: when the threshold is first crossed and `fired` is `false`, emit the action messages and set `fired = true`. While still failing on subsequent ticks, do not re-emit (avoids repeated writes to persistent Victron settings storage, which causes flash/eMMC wear — a documented Victron caution against frequent dbus settings writes). Fires again only after a recovery resets `fired` to `false` and a later outage re-crosses the threshold.
- **Recovery**: on the first success after `fired` was `true`, log "recovered" to the status debug node and reset `failCount`/`fired` to 0/false. No values are written back on recovery — the provider's own remote control resumes issuing commands once reconnected.

**Dry-run safety gate**: a single `const DRY_RUN = true;` line at the top of the Watchdog Evaluate function. While `true`, a threshold-cross logs *"DRY-RUN: would set AcPowerSetPoint=0, OvervoltageFeedIn=0[, restore PV]"* to the status debug node and does not forward messages to the action nodes. Flipping to `false` and redeploying arms it for real. This is a hardcoded constant baked into the deployed flow, not a runtime/context toggle — a memory-only toggle would silently reset (or land in an undefined state) across any Node-RED restart, including the kind of power/connectivity blip this flow exists to survive. A code constant persists correctly across reboots since it's part of the deployed JSON.

## Actions

Confirmed via research and cross-checked against the existing heatpump flow's own read of this path:

1. **`/Settings/CGwacs/AcPowerSetPoint = 0`** — ESS grid setpoint (W), range −32768..32767. Tells the ESS control loop to stop deliberately importing/exporting at the grid, directly addressing the battery-selling-to-empty incident. Confirmed via Victron community documentation.
2. **`/Settings/CGwacs/OvervoltageFeedIn = 0`** — DC-coupled PV feed-in enable (`1` = feed excess into grid, `0` = don't). Confirmed correct for this installation (Victron inverter/charger + battery + DC-coupled panels, no AC-coupled inverter) — the AC-coupled equivalent, `/Settings/CGwacs/PreventFeedback`, has inverted logic (`1` = don't feed in) and does not apply here. The existing heatpump flow already reads this same path with matching semantics (`1` = enabled), confirming the enum direction.
3. **PV production restore — placeholder, not yet implemented.** Not the "stop selling" concern but the opposite: if the provider had curtailed PV production (turned panels off) for a negative-price event and the connection drops while in that state, the failsafe should restore normal production rather than leave it curtailed indefinitely. The exact mechanism is hardware-dependent and not yet known — likely candidates are a `Solarcharger Control` node (DC MPPT charger `Mode`: On/Off) or possibly something under `/Settings/CGwacs/`, but this needs to be confirmed by observing which dbus path actually changes when the provider curtails (e.g. via `dbus-spy` over SSH at the moment of a curtailment event, or the live service/path picker in the Node-RED Victron input node's config UI). Shipped as an unwired slot: the Watchdog Evaluate function already includes it in its action list and status logging (reporting "not yet configured"), so adding it later is a single new output node wired to the function's existing action output — no changes to existing nodes or wiring.

### Extensibility

Each action is an independently wired output node fed from the Watchdog Evaluate function's action output. The palette provides generic `victron-output-custom` (or equivalent generic) nodes that can write to any discovered dbus service/path, not just the ones with dedicated typed nodes — so adding a new safety action in the future (once a path is identified) is: drag one output node onto the canvas, wire it to the function's action output, add one line to the function's action-list array (for logging/dry-run text). No existing node or wire needs to change.

## Rollout / testing plan

1. SSH in and back up the live flow: `cp <node-red data dir>/flows.json ~/flows.json.bak-2026-07-09` (confirm exact path on this Cerbo before running).
2. In the Node-RED editor: **Import → new flow tab** (not merge into current tab).
3. Deploy with `DRY_RUN = true`. Watch the "Watchdog Status" debug sidebar through a few normal 30s cycles to confirm successful checks are logged and `failCount` stays at 0.
4. Simulate an outage (e.g. temporarily block `vrm.victronenergy.com` at the router, or disconnect WAN) and confirm: `failCount` climbs over ~5 minutes, the dry-run action message appears once at the threshold, and it does not repeat on subsequent ticks while still down.
5. Reconnect and confirm the "recovered" log line appears and `failCount`/`fired` reset.
6. Only after that rehearsal succeeds, flip `DRY_RUN` to `false` and redeploy.

## Open items

- Exact `flows.json` path on this Cerbo (needed for the backup step) — to confirm during implementation/rollout, not blocking the design.
- Exact node type string for the generic custom output node (`victron-output-custom` is the expected name based on the palette's category-based naming convention, but not yet confirmed against the live palette) — to confirm by inspecting the actual node picker in the Node-RED editor before Action 3 is implemented.
- PV-restore path (Action 3) — deferred until the user identifies it empirically.
