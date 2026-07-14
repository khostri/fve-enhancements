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

test('triggered status text includes the solar charger restore action', () => {
    const result = evaluateTick({ failCount: 9, fired: false }, false, false);
    assert.match(result.statusText, /Solarcharger Mode=1/);
});
