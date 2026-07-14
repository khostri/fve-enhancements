'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');

const REPO_ROOT = path.join(__dirname, '..');
const OUTPUT_PATH = path.join(REPO_ROOT, 'internet_failsafe.json');

test('build-flow.js generates a flow with correct, safe action node content', () => {
    execFileSync('node', ['scripts/build-flow.js'], { cwd: REPO_ROOT });
    const flow = JSON.parse(fs.readFileSync(OUTPUT_PATH, 'utf8'));

    const actionNodes = flow.filter(n => n.type === 'victron-output-settings');
    assert.equal(actionNodes.length, 2, 'expected exactly two victron-output-settings action nodes');

    const acPowerSetpoint = actionNodes.find(n => n.path === '/Settings/CGwacs/AcPowerSetPoint');
    const overvoltageFeedIn = actionNodes.find(n => n.path === '/Settings/CGwacs/OvervoltageFeedIn');
    assert.ok(acPowerSetpoint, 'AcPowerSetPoint action node not found');
    assert.ok(overvoltageFeedIn, 'OvervoltageFeedIn action node not found');

    assert.equal(acPowerSetpoint.pathObj.type, 'integer');
    assert.equal(overvoltageFeedIn.pathObj.type, 'enum');

    for (const node of actionNodes) {
        assert.equal(
            Object.prototype.hasOwnProperty.call(node, 'initial'),
            false,
            `${node.path} must not have an "initial" property — node-red-contrib-victron writes it to dbus at deploy time regardless of DRY_RUN`
        );
    }

    const evalNode = flow.find(n => n.type === 'function' && n.name === 'Watchdog Evaluate');
    assert.ok(evalNode, 'Watchdog Evaluate function node not found');
    assert.match(evalNode.func, /const DRY_RUN = true;/, 'generated flow should default to DRY_RUN = true');
    assert.match(
        evalNode.func,
        /lastMsgId/,
        'generated flow should de-duplicate repeated _msgid deliveries (observed in the field: http request node + catch node both firing for one failed check, doubling the effective failure rate)'
    );
});

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
    assert.match(restoreNode.command, /SetValue 1;/, 'command should set Mode to 1 (normal)');
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
