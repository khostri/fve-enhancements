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

test('the "exec" node type (core Node-RED, not a palette add-on) is allowed', () => {
    const nodes = minimalValidFlow();
    nodes.push({ id: 'n3', type: 'exec', z: 'tab1', wires: [[], [], []] });
    const errors = validateFlow(nodes);
    assert.ok(!errors.some(e => e.includes('type "exec"')));
});
