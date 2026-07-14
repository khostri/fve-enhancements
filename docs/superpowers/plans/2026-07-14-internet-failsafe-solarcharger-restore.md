# Internet Failsafe — Solar Charger Restore (Action 3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fill in the previously-unwired "Action 3" slot in `internet_failsafe.json` (see `docs/superpowers/specs/2026-07-09-internet-failsafe-design.md`) so that when the watchdog fires, it also restores any curtailed solar chargers to normal production — dynamically, without hardcoding device-instance numbers.

**Architecture:** The provider confirmed (via a screenshot of their own control system) that they curtail PV production by writing `com.victronenergy.solarcharger.<instance>` service, path `/Mode`, value `4` = off, and restore it with value `1` = normal. The user's own installation's instance numbers are unknown/not fixed in advance, so instead of adding hardcoded `victron-output-settings` nodes (as Actions 1 and 2 use for the fixed `/Settings/CGwacs/...` paths), Action 3 uses a core Node-RED `exec` node to run a small shell command on the Cerbo at fire-time: `dbus -y | grep com.victronenergy.solarcharger` enumerates every currently-connected solar charger service, and the loop writes `/Mode SetValue %1` (i.e. `1`) to each one found. This stays correct if chargers are added, removed, or renumbered. `exec` is a Node-RED **core** node (bundled with every install, not a palette add-on), so it does not violate the existing "no new palette modules" constraint from the original design — but it is a new *node type* for this repo's generated flows, so the structural validator's allow-list must be extended.

**Tech Stack:** Node.js (`node:test` runner, no npm dependencies) — same as the rest of `scripts/`. Target device: Cerbo GX, Node-RED 4.1.1, `@victronenergy/node-red-contrib-victron@1.6.60`, Venus OS's built-in `dbus` CLI tool.

## Global Constraints

- Action 3 must **not** hardcode a solar charger device-instance number anywhere — it must discover connected `com.victronenergy.solarcharger.*` services at fire-time.
- Action 3 must obey the same `DRY_RUN` / fire-once gate as Actions 1 and 2: the `exec` node must only receive a triggering message when `result.triggered && !DRY_RUN` — exactly like `action1`/`action2` in the existing `Watchdog Evaluate` wrapper in `scripts/build-flow.js`.
- The `exec` node itself must not have any config that causes it to run automatically on deploy/restart (it must only run when it receives an input message) — this is inherent to Node-RED's core `exec` node (no timer/repeat option), unlike the past `victron-output-settings` "initial" bug documented in `docs/superpowers/plans/2026-07-09-internet-failsafe.md`, but should be verified structurally by test anyway.
- The new flow must still validate cleanly against `heatpump_relay_control.json` for ID collisions (`node scripts/validate-flow.js internet_failsafe.json heatpump_relay_control.json`).
- Generated flow file remains `internet_failsafe.json` at the repo root — hand-edits are not acceptable, only regenerate via `node scripts/build-flow.js`.
- `DRY_RUN` stays `true` in `scripts/build-flow.js` for this plan (going live is a separate, manual, on-device step per the existing Task 4 in the 2026-07-09 plan) — do not flip it as part of this work.

---

## Task 1: Update the watchdog's action label for the solar charger restore

**Files:**
- Modify: `scripts/watchdog-logic.js:6-10` (the `ACTION_LABELS` array inside `evaluateTick`)
- Test: `scripts/watchdog-logic.test.js`

**Interfaces:**
- Consumes: nothing new — `evaluateTick(state, success, dryRun)` signature is unchanged.
- Produces: `evaluateTick`'s `statusText` now mentions `Solarcharger Mode=1` instead of `Restore PV (not configured)` when triggered. This text is embedded verbatim (via `.toString()`) into the generated flow's `Watchdog Evaluate` function in Task 3, so no other file needs to duplicate this string.

- [ ] **Step 1: Add the failing test**

Append to `scripts/watchdog-logic.test.js`:

```js
test('triggered status text includes the solar charger restore action', () => {
    const result = evaluateTick({ failCount: 9, fired: false }, false, false);
    assert.match(result.statusText, /Solarcharger Mode=1/);
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `node --test scripts/watchdog-logic.test.js`
Expected: FAIL — `result.statusText` still contains `Restore PV (not configured)`, not `Solarcharger Mode=1`.

- [ ] **Step 3: Update the label**

In `scripts/watchdog-logic.js`, change:

```js
    const ACTION_LABELS = [
        'AcPowerSetPoint=0',
        'OvervoltageFeedIn=0',
        'Restore PV (not configured)'
    ];
```

to:

```js
    const ACTION_LABELS = [
        'AcPowerSetPoint=0',
        'OvervoltageFeedIn=0',
        'Solarcharger Mode=1 (all instances)'
    ];
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `node --test scripts/watchdog-logic.test.js`
Expected: all tests pass (including the 7 pre-existing ones), 0 failures.

- [ ] **Step 5: Commit**

```bash
git add scripts/watchdog-logic.js scripts/watchdog-logic.test.js
git commit -m "feat: label the confirmed solar charger restore action in watchdog status text"
```

---

## Task 2: Allow the core `exec` node type in the flow validator

**Files:**
- Modify: `scripts/validate-flow.js:5-10` (`ALLOWED_TYPES`)
- Test: `scripts/validate-flow.test.js`

**Interfaces:**
- Consumes: nothing new.
- Produces: `ALLOWED_TYPES` (exported from `scripts/validate-flow.js`) now includes `'exec'`. Task 3's build script relies on this so its internal `validateFlow` call does not reject the new node.

- [ ] **Step 1: Add the failing test**

Append to `scripts/validate-flow.test.js`:

```js
test('the "exec" node type (core Node-RED, not a palette add-on) is allowed', () => {
    const nodes = minimalValidFlow();
    nodes.push({ id: 'n3', type: 'exec', z: 'tab1', wires: [[], [], []] });
    const errors = validateFlow(nodes);
    assert.ok(!errors.some(e => e.includes('type "exec"')));
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `node --test scripts/validate-flow.test.js`
Expected: FAIL — an error containing `type "exec"` is reported because `exec` is not yet in `ALLOWED_TYPES`.

- [ ] **Step 3: Add `exec` to the allow-list**

In `scripts/validate-flow.js`, change:

```js
const ALLOWED_TYPES = new Set([
    'tab', 'group', 'inject', 'http request', 'catch', 'function', 'debug',
    'change', 'bigtimer', 'rbe',
    'victron-input-system', 'victron-input-settings', 'victron-input-relay',
    'victron-output-relay', 'victron-output-settings', 'global-config'
]);
```

to:

```js
const ALLOWED_TYPES = new Set([
    'tab', 'group', 'inject', 'http request', 'catch', 'function', 'debug', 'exec',
    'change', 'bigtimer', 'rbe',
    'victron-input-system', 'victron-input-settings', 'victron-input-relay',
    'victron-output-relay', 'victron-output-settings', 'global-config'
]);
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `node --test scripts/validate-flow.test.js`
Expected: all tests pass (including the 6 pre-existing ones), 0 failures.

- [ ] **Step 5: Commit**

```bash
git add scripts/validate-flow.js scripts/validate-flow.test.js
git commit -m "feat: allow core exec node type in flow validator for dynamic dbus actions"
```

---

## Task 3: Wire Action 3 — dynamic solar charger restore via `exec` + `dbus -y`

**Files:**
- Modify: `scripts/build-flow.js` (in full — see below)
- Modify (generated, not hand-edited): `internet_failsafe.json`
- Test: `scripts/build-flow.test.js`

**Interfaces:**
- Consumes: `evaluateTick` from `scripts/watchdog-logic.js` (Task 1's updated label flows through automatically via `.toString()`); `validateFlow`/`ALLOWED_TYPES` from `scripts/validate-flow.js` (Task 2).
- Produces: `internet_failsafe.json` now contains 12 nodes/config entries (10 before + 1 `exec` node + 1 `debug` node for its output), with the `Watchdog Evaluate` function's 3rd output wired to the new `exec` node.

- [ ] **Step 1: Add the failing tests**

Append to `scripts/build-flow.test.js`:

```js
test('build-flow.js wires a dynamic dbus restore action for solar chargers', () => {
    execFileSync('node', ['scripts/build-flow.js'], { cwd: REPO_ROOT });
    const flow = JSON.parse(fs.readFileSync(OUTPUT_PATH, 'utf8'));

    const execNodes = flow.filter(n => n.type === 'exec');
    assert.equal(execNodes.length, 1, 'expected exactly one exec node (solar charger restore)');
    const restoreNode = execNodes[0];
    assert.match(
        restoreNode.command,
        /com\.victronenergy\.solarcharger/,
        'command should discover solarcharger services dynamically'
    );
    assert.match(restoreNode.command, /SetValue %1/, 'command should set Mode to 1 (normal)');
    assert.doesNotMatch(
        restoreNode.command,
        /\/solarcharger\/\d+\//,
        'command must not hardcode a specific device instance number'
    );

    const evalNode = flow.find(n => n.type === 'function' && n.name === 'Watchdog Evaluate');
    assert.match(
        evalNode.func,
        /const action3 = doFire \? \{ payload: 1 \} : null;/,
        'action3 must be DRY_RUN-gated the same way as action1/action2'
    );

    const wiresToExec = evalNode.wires[2];
    assert.ok(
        wiresToExec.includes(restoreNode.id),
        'Watchdog Evaluate output 3 must wire to the solar charger restore exec node'
    );
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `node --test scripts/build-flow.test.js`
Expected: FAIL — no `exec` node exists yet in the generated flow, and `action3` is still hardcoded to `null`.

- [ ] **Step 3: Rewrite `scripts/build-flow.js`**

Replace the entire file with:

```js
#!/usr/bin/env node
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { evaluateTick } = require('./watchdog-logic');
const { validateFlow } = require('./validate-flow');

// Flip to false only after the Task 4 dry-run rehearsal succeeds, then rerun this script.
const DRY_RUN = true;

function genId() {
    return crypto.randomBytes(8).toString('hex');
}

const tabId = genId();
const groupCheckId = genId();
const groupActionsId = genId();
const tickId = genId();
const httpId = genId();
const catchId = genId();
const evalId = genId();
const action1Id = genId();
const action2Id = genId();
const action3Id = genId();
const action3DebugId = genId();
const statusDebugId = genId();

const evaluateTickSource = evaluateTick.toString();

const evalFunc = `// --- pure decision logic, byte-identical to scripts/watchdog-logic.js (embedded by scripts/build-flow.js) ---
${evaluateTickSource}

// --- Node-RED wrapper ---
const DRY_RUN = ${DRY_RUN};

// Node-RED's http request node can deliver the SAME failed check twice for one
// underlying request: once via its own output and once via the catch node
// (observed in the field: connection/DNS errors produced two identical-second
// messages per 30s tick, doubling the effective failure rate). Both deliveries
// carry the same _msgid, so ignore an immediate repeat.
const lastMsgId = context.get("lastMsgId");
if (msg._msgid && msg._msgid === lastMsgId) {
    return [null, null, null, null];
}
context.set("lastMsgId", msg._msgid);

const success = typeof msg.statusCode === "number" && msg.statusCode >= 200 && msg.statusCode < 300;
const prevState = context.get("state") || { failCount: 0, fired: false };

const result = evaluateTick(prevState, success, DRY_RUN);
context.set("state", result.state);

node.status({
    fill: result.triggered ? "red" : (success ? "green" : "yellow"),
    shape: "dot",
    text: result.statusText
});

const doFire = result.triggered && !DRY_RUN;
const action1 = doFire ? { payload: 0 } : null;
const action2 = doFire ? { payload: 0 } : null;
const action3 = doFire ? { payload: 1 } : null; // restore solar chargers: Mode=1 on every discovered instance
const statusMsg = { payload: result.statusText };

return [action1, action2, action3, statusMsg];
`;

const flow = [
    {
        id: tabId,
        type: 'tab',
        label: 'Internet Failsafe',
        disabled: false,
        info: 'Watches VRM reachability. After 5 minutes unreachable, zeroes ESS grid setpoint, disables DC feed-in, and restores any curtailed solar chargers, so remote-control loss cannot leave the battery selling indefinitely or PV curtailed indefinitely. See docs/superpowers/specs/2026-07-09-internet-failsafe-design.md.',
        env: []
    },
    {
        id: groupCheckId,
        type: 'group',
        z: tabId,
        name: 'Connectivity Check',
        style: { label: true },
        nodes: [tickId, httpId, catchId],
        x: 54, y: 59, w: 400, h: 160
    },
    {
        id: groupActionsId,
        type: 'group',
        z: tabId,
        name: 'Watchdog + Failsafe Actions',
        style: { label: true },
        nodes: [evalId, action1Id, action2Id, action3Id, action3DebugId, statusDebugId],
        x: 494, y: 59, w: 620, h: 320
    },
    {
        id: tickId,
        type: 'inject',
        z: tabId,
        g: groupCheckId,
        name: 'Tick (30s)',
        props: [],
        repeat: '30',
        crontab: '',
        once: true,
        onceDelay: 1,
        topic: '',
        x: 130, y: 100,
        wires: [[httpId]]
    },
    {
        id: httpId,
        type: 'http request',
        z: tabId,
        g: groupCheckId,
        name: 'Check VRM',
        method: 'HEAD',
        ret: 'txt',
        paytoqs: 'ignore',
        url: 'https://vrm.victronenergy.com',
        tls: '',
        persist: false,
        proxy: '',
        insecureHTTPParser: false,
        authType: '',
        senderr: false,
        reqTimeout: 5000,
        headers: [],
        x: 300, y: 100,
        wires: [[evalId]]
    },
    {
        id: catchId,
        type: 'catch',
        z: tabId,
        g: groupCheckId,
        name: 'VRM Check Error',
        scope: [httpId],
        uncaught: false,
        x: 300, y: 160,
        wires: [[evalId]]
    },
    {
        id: evalId,
        type: 'function',
        z: tabId,
        g: groupActionsId,
        name: 'Watchdog Evaluate',
        func: evalFunc,
        outputs: 4,
        timeout: 0,
        noerr: 0,
        initialize: '',
        finalize: '',
        libs: [],
        x: 560, y: 100,
        wires: [[action1Id], [action2Id], [action3Id], [statusDebugId]]
    },
    {
        id: action1Id,
        type: 'victron-output-settings',
        z: tabId,
        g: groupActionsId,
        service: 'com.victronenergy.settings',
        path: '/Settings/CGwacs/AcPowerSetPoint',
        serviceObj: { service: 'com.victronenergy.settings', name: 'Venus settings' },
        pathObj: {
            path: '/Settings/CGwacs/AcPowerSetPoint',
            type: 'integer',
            name: 'Grid set-point (W)',
            mode: 'both'
        },
        name: 'Set Grid Setpoint = 0',
        onlyChanges: false,
        roundValues: 'no',
        rateLimit: 0,
        outputs: 0,
        conditionalMode: false,
        condition1Operator: '>',
        condition2Enabled: false,
        condition2Service: '',
        condition2Path: '',
        condition2Operator: '>',
        logicOperator: 'AND',
        outputTrue: 'true',
        outputFalse: 'false',
        outputOnChange: false,
        debounce: 2000,
        x: 820, y: 80,
        wires: []
    },
    {
        id: action2Id,
        type: 'victron-output-settings',
        z: tabId,
        g: groupActionsId,
        service: 'com.victronenergy.settings',
        path: '/Settings/CGwacs/OvervoltageFeedIn',
        serviceObj: { service: 'com.victronenergy.settings', name: 'Venus settings' },
        pathObj: {
            path: '/Settings/CGwacs/OvervoltageFeedIn',
            type: 'enum',
            name: 'Feed excess DC-coupled PV into grid',
            enum: {
                '0': "Don't feed excess DC-tied PV into grid",
                '1': 'Feed excess DC-tied PV into the grid'
            },
            mode: 'both'
        },
        name: 'Set DC Feed-In Disabled',
        onlyChanges: false,
        roundValues: 'no',
        rateLimit: 0,
        outputs: 0,
        conditionalMode: false,
        condition1Operator: '>',
        condition2Enabled: false,
        condition2Service: '',
        condition2Path: '',
        condition2Operator: '>',
        logicOperator: 'AND',
        outputTrue: 'true',
        outputFalse: 'false',
        outputOnChange: false,
        debounce: 2000,
        x: 820, y: 130,
        wires: []
    },
    {
        id: action3Id,
        type: 'exec',
        z: tabId,
        g: groupActionsId,
        command: 'for svc in $(dbus -y | grep com.victronenergy.solarcharger | awk \'{print $1}\'); do dbus -y "$svc" /Mode SetValue %1; done',
        addpay: '',
        append: '',
        useSpawn: 'false',
        timer: '10',
        winHide: false,
        oldrc: false,
        name: 'Restore Solar Chargers (dbus, dynamic)',
        x: 820, y: 180,
        wires: [[action3DebugId], [action3DebugId], [action3DebugId]]
    },
    {
        id: action3DebugId,
        type: 'debug',
        z: tabId,
        g: groupActionsId,
        name: 'Solar Restore Exec Output',
        active: true,
        tosidebar: true,
        console: false,
        tostatus: false,
        complete: 'true',
        targetType: 'full',
        statusVal: '',
        statusType: 'auto',
        x: 820, y: 230,
        wires: []
    },
    {
        id: statusDebugId,
        type: 'debug',
        z: tabId,
        g: groupActionsId,
        name: 'Watchdog Status',
        active: true,
        tosidebar: true,
        console: false,
        tostatus: false,
        complete: 'payload',
        targetType: 'msg',
        statusVal: '',
        statusType: 'auto',
        x: 820, y: 280,
        wires: []
    }
];

const heatpumpNodes = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'heatpump_relay_control.json'), 'utf8'));
const heatpumpIds = new Set(heatpumpNodes.map(n => n.id));
const validationErrors = validateFlow(flow, [heatpumpIds]);
if (validationErrors.length) {
    console.error(`FAIL: generated flow failed validation (${validationErrors.length} problem(s)) — not writing internet_failsafe.json:`);
    for (const e of validationErrors) console.error(' - ' + e);
    process.exit(1);
}

const outPath = path.join(__dirname, '..', 'internet_failsafe.json');
fs.writeFileSync(outPath, JSON.stringify(flow, null, 4) + '\n');
console.log('Wrote ' + outPath + ' (' + flow.length + ' nodes/config entries), DRY_RUN=' + DRY_RUN);
```

- [ ] **Step 4: Run the build script**

Run: `node scripts/build-flow.js`
Expected: `Wrote .../internet_failsafe.json (12 nodes/config entries), DRY_RUN=true`

- [ ] **Step 5: Run the new tests to verify they pass**

Run: `node --test scripts/build-flow.test.js`
Expected: both tests (the pre-existing one and the new one) pass, 0 failures.

- [ ] **Step 6: Validate against the already-deployed flow — no ID collisions**

Run: `node scripts/validate-flow.js internet_failsafe.json heatpump_relay_control.json`
Expected: `OK: internet_failsafe.json is structurally valid (12 nodes, 0 id collisions)`

If this fails with a collision error, re-run Step 4 (regenerates fresh random IDs) and repeat Step 6.

- [ ] **Step 7: Run the full test suite**

Run: `node --test`
Expected: all tests across `watchdog-logic.test.js`, `validate-flow.test.js`, and `build-flow.test.js` pass, 0 failures.

- [ ] **Step 8: Commit**

```bash
git add scripts/build-flow.js scripts/build-flow.test.js internet_failsafe.json
git commit -m "feat: wire dynamic solar charger restore (Action 3) via exec + dbus -y"
```

---

## Task 4: Update the design spec to reflect the confirmed Action 3 mechanism

**Files:**
- Modify: `docs/superpowers/specs/2026-07-09-internet-failsafe-design.md`

**Interfaces:** none (documentation only).

- [ ] **Step 1: Update the architecture diagram**

In `docs/superpowers/specs/2026-07-09-internet-failsafe-design.md`, replace the `## Architecture` ASCII diagram:

```
[Tick 30s]──▶[HTTP HEAD vrm.victronenergy.com]──▶[Watchdog Evaluate]──┬──▶[Action 1: AcPowerSetPoint = 0]
                        │                              ▲              ├──▶[Action 2: OvervoltageFeedIn = 0]
                        └──(on network error)─▶[Catch node]           ├──▶[Action 3: restore PV — placeholder, unwired]
                                                                       └──▶[Status/Action debug log]
```

with:

```
[Tick 30s]──▶[HTTP HEAD vrm.victronenergy.com]──▶[Watchdog Evaluate]──┬──▶[Action 1: AcPowerSetPoint = 0]
                        │                              ▲              ├──▶[Action 2: OvervoltageFeedIn = 0]
                        └──(on network error)─▶[Catch node]           ├──▶[Action 3: exec `dbus -y` restore, all solarcharger Mode=1]──▶[debug: exec output]
                                                                       └──▶[Status/Action debug log]
```

and update the node table row for Action 3 from:

```
| *(unwired placeholder)* | TBD — `victron-output-custom` or a typed control node, once identified | Action 3 |
```

to:

```
| Restore Solar Chargers (dbus, dynamic) | `exec` (core) | Action 3 — runs `dbus -y \| grep com.victronenergy.solarcharger \| awk '{print $1}'` then `dbus -y "$svc" /Mode SetValue %1` for each discovered service |
| Solar Restore Exec Output | `debug` (sidebar) | Shows the exec node's stdout/stderr/return-code for each firing, for manual verification |
```

- [ ] **Step 2: Replace the Action 3 writeup**

Replace item 3 under `## Actions`:

```
3. **PV production restore — placeholder, not yet implemented.** ...
```

(the full paragraph through "no changes to existing nodes or wiring.") with:

```
3. **`com.victronenergy.solarcharger.<instance>` service, path `/Mode`, value `1`.** Confirmed directly by the electricity provider: they showed a screenshot of their own control system curtailing PV production by writing `/solarcharger/<instance>/Mode = 4` (off) and restoring it with `/solarcharger/<instance>/Mode = 1` (normal). The instance number is device-specific and not fixed across installations (and the user's own instance numbers were not confirmed at design time), so rather than hardcoding a `victron-output-settings` node per charger, Action 3 uses a core Node-RED `exec` node to run a shell command on the Cerbo at fire-time: `dbus -y | grep com.victronenergy.solarcharger | awk '{print $1}'` enumerates every currently-connected solar charger service (Venus OS's built-in `dbus` CLI; `dbus -y` alone lists all live D-Bus services), and the loop runs `dbus -y "$svc" /Mode SetValue %1` (sets Mode to `1`) against each one found. This adapts automatically if chargers are added, removed, or renumbered — no flow edit needed. `exec` is a Node-RED core node (bundled with every install), so this does not introduce a new palette dependency, only a new *node type* in this repo's own validator allow-list (see `scripts/validate-flow.js`).

   **On verifying the write with the palette's typed-node UI:** the palette's per-device-type nodes (e.g. a "Solarcharger" input node) populate their Device Select dropdown by scanning the live D-Bus only when you open that node's config panel in the editor — there is no way to feed the `exec` node's output into that picker programmatically. To visually confirm a restore worked using the friendly typed-node UI, add a "Solarcharger" input node in the Node-RED editor (its Device Select dropdown will show your actual connected chargers), watch `/Mode`, and wire it to a debug node. This is optional, for manual verification only, and does not affect the dynamic `exec`-based control path — it must be added by hand after import since it depends on live device instance data.
```

- [ ] **Step 3: Update "Open items"**

In `## Open items`, remove the bullet:

```
- PV-restore path (Action 3) — deferred until the user identifies it empirically.
```

(it is now resolved — see the "Actions" section above.)

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/specs/2026-07-09-internet-failsafe-design.md
git commit -m "docs: record confirmed solar charger restore mechanism for Action 3"
```

---

## Self-Review Notes

- **Spec coverage:** dynamic discovery (no hardcoded instance) ✓ Task 3 test `doesNotMatch(/\/solarcharger\/\d+\//)`; `DRY_RUN`/fire-once gate reused unchanged for action3 ✓ Task 3 test on `evalNode.func`; validator allow-list extended before the build script needs it ✓ Task 2 precedes Task 3; no ID collision with `heatpump_relay_control.json` ✓ Task 3 Step 6; docs kept in sync with shipped behavior ✓ Task 4; user's question about surfacing exec output in the palette's typed-node UI answered and documented ✓ Task 4 Step 2 second paragraph.
- **Placeholder scan:** no TBD/TODO left in code; the doc explicitly marks the typed-node verification step as an optional manual add-on (not a placeholder — it's inherently something only the live editor can populate).
- **Type consistency:** `evaluateTick(state, success, dryRun) -> {state, triggered, recovered, statusText}` unchanged from the 2026-07-09 plan; `doFire`/`action1`/`action2`/`action3`/`statusMsg` wrapper shape unchanged in count (still 4 outputs) — only `action3`'s null/payload construction changes from `null` to a `DRY_RUN`-gated `{payload: 1}`.
- **Out of scope (unchanged from original design):** going live (`DRY_RUN = false`), the manual on-device rollout/rehearsal (Task 4 of the 2026-07-09 plan already covers simulating an outage and watching the debug sidebar — it generalizes to the new exec/debug nodes without needing new steps, since it already says "watch the Watchdog Status debug sidebar" and the new `Solar Restore Exec Output` node is simply another sidebar entry to watch during the same rehearsal).
