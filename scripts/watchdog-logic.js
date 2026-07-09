'use strict';

// Must remain fully self-contained (only its own params/local declarations) — scripts/build-flow.js embeds this via .toString() into a Node-RED node.
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
