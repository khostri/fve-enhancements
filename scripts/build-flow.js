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
