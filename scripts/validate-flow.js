#!/usr/bin/env node
'use strict';
const fs = require('fs');

const ALLOWED_TYPES = new Set([
    'tab', 'group', 'inject', 'http request', 'catch', 'function', 'debug', 'exec',
    'change', 'bigtimer', 'rbe',
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
