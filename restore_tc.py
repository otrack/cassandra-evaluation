#!/usr/bin/env python3
"""Restore tc qdisc and filter rules on a Docker container from a previously saved state.

Before injecting a slowdown, fault_tolerance.sh saves the container's tc rules into
/tmp/tc_qdisc_save.txt and /tmp/tc_filter_save.txt inside the container.  This script
reads those files and re-applies the rules after the slowdown is removed.

Usage: python3 restore_tc.py <container_name>
"""

import sys
import re
import docker


def hex_to_dotted_ip(hex_str):
    """Convert an 8-character hex string like '0a000002' to dotted IP like '10.0.0.2'."""
    val = int(hex_str, 16)
    return f"{(val >> 24) & 0xff}.{(val >> 16) & 0xff}.{(val >> 8) & 0xff}.{val & 0xff}"


def run_tc(container, cmd):
    """Run a tc command inside *container* and raise on failure."""
    result = container.exec_run(cmd)
    if result.exit_code != 0:
        output = result.output.decode().strip() if result.output else ""
        raise RuntimeError(f"tc command failed (exit {result.exit_code}): {cmd!r}\n{output}")


def restore_tc(container_name):
    client = docker.from_env()
    container = client.containers.get(container_name)

    # Read saved state from inside the container.
    qdisc_result = container.exec_run("cat /tmp/tc_qdisc_save.txt")
    if qdisc_result.exit_code != 0:
        print(f"Warning: could not read /tmp/tc_qdisc_save.txt from {container_name}; "
              "skipping tc policy restoration.")
        return

    filter_result = container.exec_run("cat /tmp/tc_filter_save.txt")
    if filter_result.exit_code != 0:
        print(f"Warning: could not read /tmp/tc_filter_save.txt from {container_name}; "
              "skipping tc policy restoration.")
        return

    qdisc_lines = qdisc_result.output.decode().strip().splitlines()
    filter_lines = filter_result.output.decode().strip().splitlines()

    # Restore qdiscs: add root prio qdisc first, then child netem qdiscs.
    for line in qdisc_lines:
        m = re.match(
            r"qdisc prio (\S+): root(?: refcnt \d+)? bands (\d+) priomap (.+)",
            line.strip(),
        )
        if m:
            handle = m.group(1)
            bands = m.group(2)
            priomap = m.group(3).strip()
            cmd = f"tc qdisc add dev eth0 root handle {handle}: prio bands {bands} priomap {priomap}"
            run_tc(container, cmd)

    for line in qdisc_lines:
        m = re.match(
            r"qdisc netem (\S+): parent (\S+).*? delay (\S+)",
            line.strip(),
        )
        if m:
            handle = m.group(1)
            parent = m.group(2)
            delay = m.group(3)
            cmd = f"tc qdisc add dev eth0 parent {parent} handle {handle}: netem delay {delay}"
            run_tc(container, cmd)

    # Restore u32 filters.  The tc filter show output for each entry spans two
    # lines: the filter descriptor (containing "flowid") followed by the match
    # condition ("match HEX/MASK at OFFSET").
    i = 0
    while i < len(filter_lines):
        line = filter_lines[i].strip()
        if "u32" in line and "flowid" in line:
            parent_m = re.search(r"filter parent (\S+):", line)
            proto_m = re.search(r"protocol (\S+)", line)
            pref_m = re.search(r"pref (\d+)", line)
            flowid_m = re.search(r"flowid (\S+)", line)

            if parent_m and proto_m and pref_m and flowid_m and i + 1 < len(filter_lines):
                parent_handle = parent_m.group(1)
                protocol = proto_m.group(1)
                pref = pref_m.group(1)
                flowid = flowid_m.group(1)

                match_line = filter_lines[i + 1].strip()
                mm = re.match(r"match (\S+)/(\S+) at (\d+)", match_line)
                if mm:
                    value_hex = mm.group(1)
                    mask_hex = mm.group(2)
                    offset = mm.group(3)
                    if offset == "16" and mask_hex == "ffffffff":
                        # Destination IP match (offset 16 = dst addr in IPv4 header).
                        ip = hex_to_dotted_ip(value_hex)
                        # tc filter show reports "parent 1:" whereas tc filter add
                        # expects "parent 1:0" (the root class of the prio qdisc).
                        parent_for_add = parent_handle.rstrip(":") + ":0"
                        cmd = (
                            f"tc filter add dev eth0 protocol {protocol} "
                            f"parent {parent_for_add} prio {pref} "
                            f"u32 match ip dst {ip}/32 flowid {flowid}"
                        )
                        run_tc(container, cmd)
                    i += 2
                    continue
        i += 1


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <container_name>")
        sys.exit(1)
    restore_tc(sys.argv[1])
