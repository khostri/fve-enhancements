#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Victron CCL->CVL Guard PRO
- Reacts to BMS CCL limits by adjusting DVCC MaxChargeVoltage (CVL).
- Follows BMS drops immediately but applies a gradual step-up for ANY voltage increase.
- Prevents MPPT oscillation (LOOP_DELAY = 4s).
- Implements log rotation to protect storage.
- Failsafe behavior: sets safe CVL if BMS disconnects.
- Restores safest CVL upon script termination.
"""

import time
import logging
from logging.handlers import RotatingFileHandler
import argparse
from dbus.mainloop.glib import DBusGMainLoop
import dbus
import sys
import signal
from datetime import datetime

# --------------------------
# Configuration
# --------------------------
BMS_SERVICE = "com.victronenergy.battery.socketcan_can1" 
PATH_CCL = "/Info/MaxChargeCurrent"
PATH_CVL = "/Info/MaxChargeVoltage"
PATH_I   = "/Dc/0/Current"

DVCC_SERVICE   = "com.victronenergy.settings"
DVCC_CVL_PATH  = "/Settings/SystemSetup/MaxChargeVoltage"

# Regulation Parameters
STEP_DOWN   = 0.05   # V - Step down when I > CCL
STEP_UP     = 0.02   # V - Gradual step up when safe
BUFFER_A    = 5.0    # A - Safety margin below CCL before releasing
FAILSAFE_CVL = 55.11 # V - Target CVL if BMS is lost
LOOP_DELAY  = 4.0    # s - Loop period to allow MPPT reaction
MIN_SAFE_V  = 52.0   # V - Hard bottom limit for CVL

STATUS_FILE = "/data/ccl_guard.status"
LOG_FILE    = "/data/log/ccl_cvl_guard.log"

# --------------------------
# Arguments & Logging
# --------------------------
parser = argparse.ArgumentParser(description="Victron CCL->CVL Guard")
parser.add_argument("--dry", action="store_true", help="DRY-RUN mode (no DBus writes)")
args = parser.parse_args()
DRY_RUN = args.dry

log_handler = RotatingFileHandler(LOG_FILE, maxBytes=1024*1024, backupCount=2)
log_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
log_handler.setFormatter(log_formatter)
log = logging.getLogger("ccl-guard")
log.setLevel(logging.INFO)
log.addHandler(log_handler)

log.info(f"Start guard script (DRY_RUN={DRY_RUN})")

# --------------------------
# DBus Helpers
# --------------------------
def read_val(bus, service, path):
    try:
        obj = bus.get_object(service, path)
        val = obj.GetValue(dbus_interface="com.victronenergy.BusItem")
        return float(val) if val is not None else None
    except: 
        return None

def write_val(bus, service, path, val):
    if DRY_RUN:
        return True
    try:
        obj = bus.get_object(service, path)
        obj.SetValue(float(val), dbus_interface="com.victronenergy.BusItem")
        return True
    except Exception as e:
        log.error(f"Write error on {path}: {e}")
        return False

# --------------------------
# State Variables
# --------------------------
last_bms_cvl = None
current_target_v = None
last_sent_v = None  # Tracks the last intended value to prevent dry-run spam
failsafe_active = False
is_throttling = False

def cleanup_and_reset(bus):
    global last_bms_cvl, current_target_v
    if last_bms_cvl is None:
        log.info("Reset skipped (no previous BMS CVL known).")
        return
    # Select the safer (lower) value for system reset
    safe_val = min(last_bms_cvl, current_target_v) if current_target_v else last_bms_cvl
    write_val(bus, DVCC_SERVICE, DVCC_CVL_PATH, safe_val)
    log.info(f"Reset on exit to: {safe_val} V")

def signal_handler(sig, frame, bus):
    log.info(f"Terminating on signal {sig}...")
    cleanup_and_reset(bus)
    sys.exit(0)

def write_status(**kwargs):
    try:
        with open(STATUS_FILE, "w") as f:
            for k, v in kwargs.items():
                f.write(f"{k}={v}\n")
    except: 
        pass

# --------------------------
# Main Loop
# --------------------------
def main():
    global current_target_v, failsafe_active, is_throttling, last_bms_cvl, last_sent_v

    DBusGMainLoop(set_as_default=True)
    bus = dbus.SystemBus()

    signal.signal(signal.SIGINT,  lambda s, f: signal_handler(s, f, bus))
    signal.signal(signal.SIGTERM, lambda s, f: signal_handler(s, f, bus))

    while True:
        ccl      = read_val(bus, BMS_SERVICE, PATH_CCL)
        cvl_bms  = read_val(bus, BMS_SERVICE, PATH_CVL)
        actual_i = read_val(bus, BMS_SERVICE, PATH_I)
        current_dvcc = read_val(bus, DVCC_SERVICE, DVCC_CVL_PATH)

        if cvl_bms is not None:
            last_bms_cvl = cvl_bms

        # Failsafe detection
        if ccl is None or cvl_bms is None:
            if not failsafe_active:
                write_val(bus, DVCC_SERVICE, DVCC_CVL_PATH, FAILSAFE_CVL)
                log.warning("BMS offline! Failsafe activated.")
                failsafe_active = True
            time.sleep(LOOP_DELAY)
            continue

        if failsafe_active:
            log.info("BMS online. Exiting failsafe.")
            failsafe_active = False
            current_target_v = cvl_bms

        # Initialization
        if current_target_v is None:
            current_target_v = current_dvcc if current_dvcc else cvl_bms

        reason = "NONE"

        # 1. Obey BMS strict drops immediately
        if cvl_bms < current_target_v:
            current_target_v = cvl_bms
            reason = "BMS DROPPED LIMIT"

        # 2. Regulation Logic
        if actual_i is not None and actual_i > ccl:
            # Current exceeded, force voltage down
            current_target_v -= STEP_DOWN
            is_throttling = True
            reason = "THROTTLING (I > CCL)"
            
        elif actual_i is not None and actual_i < (ccl - BUFFER_A):
            # Current is safe. Are we below the target BMS limit?
            if current_target_v < cvl_bms:
                # Step up gradually regardless of whether BMS raised it or we were throttling
                current_target_v += STEP_UP
                reason = "RELEASING (stepping up)"
                
                # Cap to BMS limit
                if current_target_v >= cvl_bms:
                    current_target_v = cvl_bms
                    is_throttling = False
            else:
                is_throttling = False
                if reason == "NONE":
                    reason = "FOLLOWING BMS"
        else:
            # Inside the buffer zone (CCL - BUFFER_A <= I <= CCL)
            if reason == "NONE":
                reason = "HOLDING (in buffer zone)"

        # 3. Apply Hard Limits
        current_target_v = round(min(max(current_target_v, MIN_SAFE_V), cvl_bms), 2)

        # 4. Write Execution
        wrote = False
        if current_dvcc is not None and abs(current_target_v - current_dvcc) > 0.001:
            # Prevent log spam in DRY_RUN mode where current_dvcc never physically updates
            if DRY_RUN and current_target_v == last_sent_v:
                pass 
            else:
                if write_val(bus, DVCC_SERVICE, DVCC_CVL_PATH, current_target_v):
                    mode_str = "[DRY-RUN] " if DRY_RUN else ""
                    log.info(f"{mode_str}Action: {reason} | I:{actual_i}A | CCL:{ccl}A | CVL_NEW:{current_target_v}V (DVCC was:{current_dvcc}V)")
                    last_sent_v = current_target_v
                    wrote = True

        # 5. Status file update
        write_status(
            STATUS="OK" if not failsafe_active else "FAILSAFE",
            LAST_WRITE=datetime.now().strftime("%H:%M:%S"),
            CURRENT_CVL=current_target_v,
            CURRENT_CCL=f"{ccl:.1f}" if ccl else "NA",
            CURRENT_I=f"{actual_i:.1f}" if actual_i is not None else "NA",
            THROTTLING=str(is_throttling).lower()
        )

        time.sleep(LOOP_DELAY)

if __name__ == "__main__":
    try: 
        main()
    except KeyboardInterrupt: 
        sys.exit(0)