# Internet Failsafe Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **Task 4 is manual-only** — it deploys to a physical, remotely-controlled Cerbo GX. Do not dispatch it to an autonomous subagent or run its SSH/deploy commands without the user driving each step. Tasks 1–3 are ordinary local Node.js work and can be executed/reviewed normally.

**Goal:** Build a standalone Node-RED flow that detects loss of connectivity to Victron's VRM (the channel the electricity provider uses for remote control) and, after 5 minutes, forces the ESS into a safe state so a dropped connection can never again leave the battery selling until empty.

**Architecture:** The failsafe's decision logic (counter/threshold/dry-run/fire-once state machine) is written as a pure, unit-tested JavaScript function, independent of Node-RED. A build script embeds that exact function (via `.toString()`, so there is no hand-copied duplicate to drift out of sync) into a generated Node-RED flow JSON file, alongside a connectivity-check chain (HTTP request + error catch) and two Victron settings-output nodes. A separate structural validator checks the generated JSON for ID collisions, dangling references, and disallowed node types before it's ever imported into the live Node-RED instance — directly targeting the failure mode that crashed Node-RED on a past import.

**Tech Stack:** Node.js (v25.8.1 confirmed installed), built-in `node:test` test runner, no npm dependencies. Target device: Cerbo GX running Node-RED 4.1.1 with `@victronenergy/node-red-contrib-victron@1.6.60`.

## Global Constraints

- Check target: `https://vrm.victronenergy.com`, method `HEAD`, 5s timeout, every 30s.
- Failsafe threshold: 10 consecutive failed checks (30s × 10 = 5 minutes) before firing.
- On any successful check, the fail counter resets to 0 immediately — no gradual decrement/hysteresis (unlike the existing heatpump flow's oscillation-damping counter; a connection is either up or down).
- Fire-once per outage: once fired, do not re-write the settings again until a success is observed (re-arms for the next outage). Reason: frequent writes to Victron's persistent settings storage cause flash/eMMC wear (documented Victron caution).
- Failsafe actions, written via `com.victronenergy.settings`:
  - `/Settings/CGwacs/AcPowerSetPoint` = `0` (type `integer`, W)
  - `/Settings/CGwacs/OvervoltageFeedIn` = `0` (type `enum`: `0` = don't feed excess DC-tied PV into grid, `1` = feed it in)
  - A third action slot (PV production restore) is defined in the logic and status text but left unwired — no confirmed D-Bus path yet.
- `DRY_RUN` defaults to `true` in the generated flow. It is a build-time constant (in `scripts/build-flow.js`), not a runtime/context toggle, so it survives Node-RED restarts correctly — going live means editing that constant and regenerating, not flipping in-device state.
- The new flow must not share any node/group/tab ID with the already-deployed `heatpump_relay_control.json`, and must use only node types already confirmed installed on the Cerbo (core Node-RED nodes + the `victron-*` types already present in `heatpump_relay_control.json`) — no new palette modules.
- Generated flow file: `internet_failsafe.json` at the repo root (matches the existing convention of `heatpump_relay_control.json`).

---

## Task 1: Watchdog decision logic (pure function, unit-tested)

**Files:**
- Create: `scripts/watchdog-logic.js`
- Test: `scripts/watchdog-logic.test.js`

**Interfaces:**
- Produces: `evaluateTick(state, success, dryRun)` exported from `scripts/watchdog-logic.js`, where:
  - `state: { failCount: number, fired: boolean }`
  - `success: boolean` — whether the connectivity check succeeded this tick
  - `dryRun: boolean`
  - Returns `{ state: { failCount, fired }, triggered: boolean, recovered: boolean, statusText: string }`
  - The function body must be **fully self-contained** (no references to anything outside its own parameter list and internal `const`/`let` declarations) — Task 3 embeds it into a Node-RED function node via `.toString()`, which captures only the function's own source text, not any outer closure.

- [ ] **Step 1: Create the scripts directory and write the failing test**

```bash
mkdir -p scripts
```

Create `scripts/watchdog-logic.test.js`:

```js
'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { evaluateTick } = require('./watchdog-logic');

test('9 consecutive failures do not trigger', () => {
    let state = { failCount: 0, fired: false };
    for (let i = 0; i < 9; i++) {
        const result = evaluateTick(state, false, true);
        assert.equal(result.triggered, false, `iteration ${i + 1} should not trigger`);
        state = result.state;
    }
    assert.equal(state.failCount, 9);
});

test('10th consecutive failure triggers and sets fired', () => {
    let state = { failCount: 9, fired: false };
    const result = evaluateTick(state, false, true);
    assert.equal(result.triggered, true);
    assert.equal(result.state.fired, true);
    assert.equal(result.state.failCount, 10);
});

test('success resets failCount to 0 immediately, no gradual decrement', () => {
    const state = { failCount: 5, fired: false };
    const result = evaluateTick(state, true, true);
    assert.equal(result.state.failCount, 0);
    assert.equal(result.triggered, false);
    assert.equal(result.recovered, false);
});

test('does not re-trigger on continued failure after already fired', () => {
    const state = { failCount: 10, fired: true };
    const result = evaluateTick(state, false, true);
    assert.equal(result.triggered, false, 'should not fire twice for the same outage');
    assert.equal(result.state.fired, true);
    assert.equal(result.state.failCount, 11);
});

test('success after a fire reports recovered and re-arms', () => {
    const state = { failCount: 14, fired: true };
    const result = evaluateTick(state, true, false);
    assert.equal(result.recovered, true);
    assert.equal(result.state.fired, false);
    assert.equal(result.state.failCount, 0);
});

test('dry-run and live status text are distinguishable on trigger', () => {
    const dryText = evaluateTick({ failCount: 9, fired: false }, false, true).statusText;
    const liveText = evaluateTick({ failCount: 9, fired: false }, false, false).statusText;
    assert.match(dryText, /^DRY-RUN:/);
    assert.match(liveText, /^FAILSAFE FIRED:/);
    assert.notEqual(dryText, liveText);
});

test('normal healthy tick reports OK with no trigger or recovery', () => {
    const result = evaluateTick({ failCount: 0, fired: false }, true, true);
    assert.equal(result.triggered, false);
    assert.equal(result.recovered, false);
    assert.equal(result.statusText, 'OK');
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `node --test scripts/watchdog-logic.test.js`
Expected: fails with `Cannot find module './watchdog-logic'`

- [ ] **Step 3: Write the implementation**

Create `scripts/watchdog-logic.js`:

```js
'use strict';

function evaluateTick(state, success, dryRun) {
    const THRESHOLD = 10;
    const ACTION_LABELS = [
        'AcPowerSetPoint=0',
        'OvervoltageFeedIn=0',
        'Restore PV (not configured)'
    ];

    let { failCount, fired } = state;
    let recovered = false;
    let triggered = false;

    if (success) {
        if (fired) recovered = true;
        failCount = 0;
        fired = false;
    } else {
        failCount += 1;
        if (failCount >= THRESHOLD && !fired) {
            triggered = true;
            fired = true;
        }
    }

    let statusText;
    if (triggered) {
        statusText = (dryRun ? 'DRY-RUN: would fire failsafe (' : 'FAILSAFE FIRED: (') + ACTION_LABELS.join(', ') + ')';
    } else if (recovered) {
        statusText = 'Recovered - connectivity restored';
    } else if (!success) {
        statusText = 'Check failed (' + failCount + '/' + THRESHOLD + ')';
    } else {
        statusText = 'OK';
    }

    return { state: { failCount, fired }, triggered, recovered, statusText };
}

module.exports = { evaluateTick };
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `node --test scripts/watchdog-logic.test.js`
Expected: `pass 7`, `fail 0`

- [ ] **Step 5: Commit**

```bash
git add scripts/watchdog-logic.js scripts/watchdog-logic.test.js
git commit -m "test: add unit-tested watchdog decision logic for internet failsafe"
```

---

## Task 2: Flow structural validator (reusable, catches the past crash class of bug)

**Files:**
- Create: `scripts/validate-flow.js`
- Test: `scripts/validate-flow.test.js`

**Interfaces:**
- Produces: `validateFlow(nodes, otherIdSets = [])` exported from `scripts/validate-flow.js`, where `nodes` is a parsed Node-RED flow JSON array and `otherIdSets` is an array of `Set<string>` (node IDs from other already-deployed flow files to check for collisions). Returns `string[]` of human-readable errors (empty array = valid).
- Produces: a CLI entry point, `node scripts/validate-flow.js <flow.json> [compareAgainst.json ...]`, exit code `0` on success and `1` on validation failure — used by Task 3.

- [ ] **Step 1: Write the failing test**

Create `scripts/validate-flow.test.js`:

```js
'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { validateFlow } = require('./validate-flow');

function minimalValidFlow() {
    return [
        { id: 'tab1', type: 'tab', label: 'Test' },
        { id: 'grp1', type: 'group', z: 'tab1', name: 'G', nodes: ['n1'] },
        { id: 'n1', type: 'inject', z: 'tab1', g: 'grp1', wires: [['n2']] },
        { id: 'n2', type: 'debug', z: 'tab1', wires: [] }
    ];
}

test('a well-formed minimal flow produces no errors', () => {
    const errors = validateFlow(minimalValidFlow());
    assert.deepEqual(errors, []);
});

test('duplicate ids within the same file are reported', () => {
    const nodes = minimalValidFlow();
    nodes.push({ id: 'n1', type: 'debug', z: 'tab1', wires: [] });
    const errors = validateFlow(nodes);
    assert.ok(errors.some(e => e.includes('Duplicate id within file: n1')));
});

test('a wire pointing at a nonexistent node id is reported', () => {
    const nodes = minimalValidFlow();
    nodes[2].wires = [['does-not-exist']];
    const errors = validateFlow(nodes);
    assert.ok(errors.some(e => e.includes('wire to missing id does-not-exist')));
});

test('a group referencing a missing member id is reported', () => {
    const nodes = minimalValidFlow();
    nodes[1].nodes.push('ghost-node');
    const errors = validateFlow(nodes);
    assert.ok(errors.some(e => e.includes('references missing node id ghost-node')));
});

test('a disallowed node type is reported', () => {
    const nodes = minimalValidFlow();
    nodes.push({ id: 'n3', type: 'some-unvetted-node', z: 'tab1', wires: [] });
    const errors = validateFlow(nodes);
    assert.ok(errors.some(e => e.includes('type "some-unvetted-node"')));
});

test('an id colliding with an already-deployed flow is reported', () => {
    const nodes = minimalValidFlow();
    const deployedIds = new Set(['n1']);
    const errors = validateFlow(nodes, [deployedIds]);
    assert.ok(errors.some(e => e.includes('Id collides with an already-deployed flow: n1')));
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `node --test scripts/validate-flow.test.js`
Expected: fails with `Cannot find module './validate-flow'`

- [ ] **Step 3: Write the implementation**

Create `scripts/validate-flow.js`:

```js
#!/usr/bin/env node
'use strict';
const fs = require('fs');

const ALLOWED_TYPES = new Set([
    'tab', 'group', 'inject', 'http request', 'catch', 'function', 'debug',
    'victron-input-system', 'victron-input-settings', 'victron-input-relay',
    'victron-output-relay', 'victron-output-settings', 'global-config'
]);

function validateFlow(nodes, otherIdSets = []) {
    const errors = [];
    const ids = new Set();

    for (const n of nodes) {
        if (!n.id) {
            errors.push('Node missing id: ' + JSON.stringify(n).slice(0, 80));
            continue;
        }
        if (ids.has(n.id)) errors.push(`Duplicate id within file: ${n.id}`);
        ids.add(n.id);
    }

    for (const idSet of otherIdSets) {
        for (const id of ids) {
            if (idSet.has(id)) errors.push(`Id collides with an already-deployed flow: ${id}`);
        }
    }

    const tabIds = new Set(nodes.filter(n => n.type === 'tab' || n.type === 'subflow').map(n => n.id));
    const groupIds = new Set(nodes.filter(n => n.type === 'group').map(n => n.id));

    for (const n of nodes) {
        if (!ALLOWED_TYPES.has(n.type)) {
            errors.push(`Node ${n.id} has type "${n.type}" not in the allowed/confirmed-installed list`);
        }
        if (n.type !== 'tab' && n.z !== undefined) {
            if (!tabIds.has(n.z)) errors.push(`Node ${n.id} has z="${n.z}" which is not a known tab id`);
        }
        if (n.g !== undefined && !groupIds.has(n.g)) {
            errors.push(`Node ${n.id} has g="${n.g}" which is not a known group id`);
        }
        if (n.type === 'group' && Array.isArray(n.nodes)) {
            for (const memberId of n.nodes) {
                if (!ids.has(memberId)) errors.push(`Group ${n.id} references missing node id ${memberId}`);
            }
        }
        if (Array.isArray(n.wires)) {
            for (const output of n.wires) {
                if (!Array.isArray(output)) continue;
                for (const targetId of output) {
                    if (!ids.has(targetId)) errors.push(`Node ${n.id} has a wire to missing id ${targetId}`);
                }
            }
        }
        if (n.type === 'catch' && Array.isArray(n.scope)) {
            for (const scopeId of n.scope) {
                if (!ids.has(scopeId)) errors.push(`Catch node ${n.id} scopes to missing id ${scopeId}`);
            }
        }
    }

    return errors;
}

function main() {
    const target = process.argv[2];
    if (!target) {
        console.error('Usage: node validate-flow.js <flow.json> [compareAgainst.json ...]');
        process.exit(2);
    }
    const nodes = JSON.parse(fs.readFileSync(target, 'utf8'));
    const otherIdSets = process.argv.slice(3).map(p =>
        new Set(JSON.parse(fs.readFileSync(p, 'utf8')).map(n => n.id))
    );

    const errors = validateFlow(nodes, otherIdSets);
    if (errors.length) {
        console.error(`FAIL: ${errors.length} problem(s) found in ${target}`);
        for (const e of errors) console.error(' - ' + e);
        process.exit(1);
    }
    console.log(`OK: ${target} is structurally valid (${nodes.length} nodes, 0 id collisions)`);
}

if (require.main === module) {
    main();
}

module.exports = { validateFlow, ALLOWED_TYPES };
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `node --test scripts/validate-flow.test.js`
Expected: `pass 6`, `fail 0`

- [ ] **Step 5: Commit**

```bash
git add scripts/validate-flow.js scripts/validate-flow.test.js
git commit -m "test: add reusable Node-RED flow structural validator"
```

---

## Task 3: Build script — generate `internet_failsafe.json` and validate it

**Files:**
- Create: `scripts/build-flow.js`
- Create (generated, not hand-edited): `internet_failsafe.json`

**Interfaces:**
- Consumes: `evaluateTick` from `scripts/watchdog-logic.js` (Task 1); CLI `node scripts/validate-flow.js <file> [compare...]` (Task 2).
- Produces: `internet_failsafe.json` at the repo root — a complete Node-RED tab ready for **Import → new flow tab**.

- [ ] **Step 1: Write `scripts/build-flow.js`**

```js
#!/usr/bin/env node
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { evaluateTick } = require('./watchdog-logic');

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
const statusDebugId = genId();

const evaluateTickSource = evaluateTick.toString();

const evalFunc = `// --- pure decision logic, byte-identical to scripts/watchdog-logic.js (embedded by scripts/build-flow.js) ---
${evaluateTickSource}

// --- Node-RED wrapper ---
const DRY_RUN = ${DRY_RUN};

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
const action3 = null; // placeholder for future PV-restore action - see spec Action 3
const statusMsg = { payload: result.statusText };

return [action1, action2, action3, statusMsg];
`;

const flow = [
    {
        id: tabId,
        type: 'tab',
        label: 'Internet Failsafe',
        disabled: false,
        info: 'Watches VRM reachability. After 5 minutes unreachable, zeroes ESS grid setpoint and disables DC feed-in so remote-control loss cannot leave the battery selling indefinitely. See docs/superpowers/specs/2026-07-09-internet-failsafe-design.md.',
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
        nodes: [evalId, action1Id, action2Id, statusDebugId],
        x: 494, y: 59, w: 560, h: 220
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
        wires: [[action1Id], [action2Id], [], [statusDebugId]]
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
        initial: 0,
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
        initial: 0,
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
        x: 820, y: 180,
        wires: []
    }
];

const outPath = path.join(__dirname, '..', 'internet_failsafe.json');
fs.writeFileSync(outPath, JSON.stringify(flow, null, 4) + '\n');
console.log('Wrote ' + outPath + ' (' + flow.length + ' nodes/config entries), DRY_RUN=' + DRY_RUN);
```

- [ ] **Step 2: Run the build script**

Run: `node scripts/build-flow.js`
Expected: `Wrote .../internet_failsafe.json (10 nodes/config entries), DRY_RUN=true`

- [ ] **Step 3: Validate the generated flow — self-consistency**

Run: `node scripts/validate-flow.js internet_failsafe.json`
Expected: `OK: internet_failsafe.json is structurally valid (10 nodes, 0 id collisions)`

- [ ] **Step 4: Validate against the already-deployed flow — no ID collisions**

Run: `node scripts/validate-flow.js internet_failsafe.json heatpump_relay_control.json`
Expected: `OK: internet_failsafe.json is structurally valid (10 nodes, 0 id collisions)`

If this fails with a collision error, re-run Step 2 (regenerates fresh random IDs) and repeat Step 4.

- [ ] **Step 5: Run the full test suite once more**

Run: `node --test scripts/`
Expected: all `watchdog-logic.test.js` and `validate-flow.test.js` tests pass, 0 failures.

- [ ] **Step 6: Commit — first working code version**

```bash
git add docs/superpowers/specs/2026-07-09-internet-failsafe-design.md docs/superpowers/plans/2026-07-09-internet-failsafe.md scripts/ internet_failsafe.json
git commit -m "feat: generate internet failsafe Node-RED flow (dry-run mode)"
```

---

## Task 4: Manual rollout on the Cerbo — MANUAL, PERFORM YOURSELF

**Do not automate this task.** It touches a physical device that a third party (your electricity provider) also controls remotely, and undoing a bad deploy requires physical/SSH access. Follow these steps yourself; ask for command explanations as needed rather than having an agent run them unattended.

- [ ] **Step 1: Back up the live flow over SSH**

```bash
ssh root@<cerbo-ip> "cp /data/home/root/.node-red/flows.json /data/home/root/.node-red/flows.json.bak-2026-07-09"
```

(Confirm the actual Node-RED data directory on your Cerbo first — it's typically `/data/home/root/.node-red/` but verify with `ssh root@<cerbo-ip> "find / -maxdepth 4 -name flows.json 2>/dev/null"` if unsure.)

- [ ] **Step 2: Import as a new tab**

In the Node-RED editor: hamburger menu → Import → paste the contents of `internet_failsafe.json` → select **"new flow"** (not "current flow") → Import. Do not deploy yet if you want to review the canvas first; otherwise Deploy.

- [ ] **Step 3: Watch normal operation**

Open the debug sidebar, enable the "Watchdog Status" node's output if not already visible. Confirm you see `OK` roughly every 30 seconds and the node status dot is green.

- [ ] **Step 4: Simulate an outage**

Block `vrm.victronenergy.com` at your router/firewall, or temporarily disconnect the Cerbo's WAN. Confirm in the debug sidebar:
- Status text progresses `Check failed (1/10)` → `Check failed (2/10)` → ... → `Check failed (9/10)`
- At the 10th failure: `DRY-RUN: would fire failsafe (AcPowerSetPoint=0, OvervoltageFeedIn=0, Restore PV (not configured))`
- Further failed checks after that do **not** repeat the DRY-RUN message (fire-once behavior)
- Node status dot turns red at the trigger point

- [ ] **Step 5: Confirm recovery**

Restore connectivity. Confirm the debug sidebar shows `Recovered - connectivity restored` and the status dot returns to green.

- [ ] **Step 6: Go live**

Back in this repo, edit the constant at the top of `scripts/build-flow.js`:

```js
const DRY_RUN = false;
```

Then:

```bash
node scripts/build-flow.js
node scripts/validate-flow.js internet_failsafe.json heatpump_relay_control.json
git add scripts/build-flow.js internet_failsafe.json
git commit -m "feat: arm internet failsafe (go live)"
```

Re-import the updated `internet_failsafe.json` on the Cerbo the same way as Step 2 (new tab will now have different node IDs than the dry-run import — delete the old dry-run tab first via the tab's right-click menu, then import fresh, to avoid leaving a disabled duplicate around). Deploy.

- [ ] **Step 7: Final live smoke check**

Repeat Step 4 (simulate a short outage) once more with live mode armed, and confirm via VRM/VictronConnect that `AcPowerSetPoint` and `OvervoltageFeedIn` actually changed to `0` at the 5-minute mark. Restore connectivity and confirm your provider's remote control resumes controlling the setpoint again on its own.

---

## Self-Review Notes

- **Spec coverage**: connectivity check (Task 3, http request + catch) ✓; 5-min/10-check threshold (Task 1 test) ✓; reset-not-decrement counter (Task 1 test) ✓; fire-once/re-arm (Task 1 tests) ✓; dry-run constant (Task 3 build script + Task 4 Step 6) ✓; both confirmed D-Bus actions (Task 3 nodes) ✓; PV-restore placeholder (Task 3 `action3`/`ACTION_LABELS` entry, unwired) ✓; new-tab isolation from `heatpump_relay_control.json` (Task 3 Step 4, Task 2 collision check) ✓; safe rollout with backup (Task 4 Steps 1–2) ✓.
- **Placeholder scan**: no TBD/TODO in code; the one open unknown (exact `flows.json` path, PV-restore D-Bus path) is called out explicitly as unconfirmed rather than assumed.
- **Type consistency**: `evaluateTick(state, success, dryRun) -> {state, triggered, recovered, statusText}` used identically in Task 1's tests, Task 3's wrapper, and the spec's description.
