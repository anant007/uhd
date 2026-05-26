#!/usr/bin/env python3
"""
Probe all sensors on an X310 SDR and print their values.
Usage: python probe_x310_sensors.py [--args "addr=192.168.10.2"]
"""

import argparse
import uhd

def probe_sensors(usrp):
    tree = usrp.get_tree()

    print("=" * 60)
    print("X310 Sensor Probe")
    print("=" * 60)

    # --- Motherboard sensors ---
    print("\n[Motherboard Sensors]")
    num_mboards = usrp.get_num_mboards()
    for mb in range(num_mboards):
        sensor_names = usrp.get_mboard_sensor_names(mb)
        if not sensor_names:
            print(f"  mboard[{mb}]: no sensors found")
        for name in sensor_names:
            val = usrp.get_mboard_sensor(name, mb)
            print(f"  mboard[{mb}] / {name}: {val.value}  (unit: {val.unit})")

    # --- RX sensors (per channel) ---
    print("\n[RX Sensors]")
    for ch in range(usrp.get_rx_num_channels()):
        sensor_names = usrp.get_rx_sensor_names(ch)
        if not sensor_names:
            print(f"  rx[{ch}]: no sensors found")
        for name in sensor_names:
            val = usrp.get_rx_sensor(name, ch)
            print(f"  rx[{ch}] / {name}: {val.value}  (unit: {val.unit})")

    # --- TX sensors (per channel) ---
    print("\n[TX Sensors]")
    for ch in range(usrp.get_tx_num_channels()):
        sensor_names = usrp.get_tx_sensor_names(ch)
        if not sensor_names:
            print(f"  tx[{ch}]: no sensors found")
        for name in sensor_names:
            val = usrp.get_tx_sensor(name, ch)
            print(f"  tx[{ch}] / {name}: {val.value}  (unit: {val.unit})")

    # --- Property tree deep scan for any remaining sensor paths ---
    print("\n[All sensor paths in property tree]")
    try:
        all_paths = tree.list("/")
        def walk(path):
            try:
                children = tree.list(path)
                for child in children:
                    full = path.rstrip("/") + "/" + child
                    if child == "sensors":
                        # List individual sensors under this node
                        try:
                            s_names = tree.list(full)
                            for s in s_names:
                                s_path = full + "/" + s + "/value"
                                try:
                                    val = tree.access_sensor_value(full + "/" + s).get()
                                    print(f"  {full}/{s}: {val.value}  (unit: {val.unit})")
                                except Exception:
                                    pass
                        except Exception:
                            pass
                    else:
                        walk(full)
            except Exception:
                pass
        walk("/")
    except Exception as e:
        print(f"  (property tree walk skipped: {e})")

    print("\n" + "=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Probe all X310 sensors")
    parser.add_argument("--args", default="", help='Device args, e.g. "addr=192.168.10.2"')
    args = parser.parse_args()

    print(f"Connecting to device: '{args.args}' ...")
    usrp = uhd.usrp.MultiUSRP(args.args)
    print(f"Device: {usrp.get_mboard_name(0)}\n")

    probe_sensors(usrp)


if __name__ == "__main__":
    main()
