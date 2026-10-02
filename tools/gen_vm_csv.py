#!/usr/bin/env python3
"""Regenerate vm.csv, the table of VM shapes `machine=` is looked up in.

vm.csv is read in three places, and two of them do more than an exact lookup:

  * cassandra/start_cassandra_data_centers.py and
    infra/simulation/provider.sh resolve `machine=` to a container's
    --cpus/--memory, and from there to the Cassandra heap.
  * utils.sh:compute_test_machine() scans the *whole* table and picks the
    largest-vCPU row that fits the local host, then rewrites `machine=` in
    exp.config.  Rows added here therefore change what `--test` runs size
    themselves to -- see docs/notes.md.

So the table is curated, not exhaustive: only shapes worth provisioning for
this benchmark belong in it.

Sources
  EC2   the AWS Price List bulk CSV, which carries vCPU and Memory per
        instance type and needs no credentials.
  GCE   `gcloud compute machine-types list`, which needs the Compute Engine
        API enabled on the active project.  When gcloud is unavailable or
        denied, the GCE rows already in vm.csv are preserved untouched, so
        running this script can never silently drop them.

Usage
  tools/gen_vm_csv.py                 # refresh EC2 rows, keep GCE rows
  tools/gen_vm_csv.py --check         # report drift, write nothing
"""

import argparse
import csv
import io
import os
import subprocess
import sys
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VM_CSV = os.path.join(ROOT, "vm.csv")

PRICING_URL = (
    "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2"
    "/current/%s/index.csv"
)

# Families worth running Cassandra on: general purpose, compute optimised,
# memory optimised, and the two NVMe-backed ones.  Deliberately excludes
# burstable (t*), GPU, and bare metal -- compute_test_machine() would happily
# select any of them.
EC2_FAMILIES = (
    "m5", "m6i", "m7i",
    "c5", "c6i", "c7i",
    "r5", "r6i",
    "i3", "i4i",
)


def is_ec2(name):
    """EC2 instance types are `family.size`; GCE machine types never contain a dot."""
    return "." in name


def read_existing(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="") as f:
        return [(r["name"], r["vcpus"], r["memory"]) for r in csv.DictReader(f)]


def fmt_mem(gib):
    """Match the existing column: integral values unadorned, else one decimal."""
    return str(int(gib)) if float(gib) == int(gib) else ("%g" % gib)


def fetch_ec2(region, families):
    """Stream the Price List CSV and pull one row per instance type.

    The file is ~290 MB and has five preamble lines before the header, with
    quoted fields containing commas, so it needs a real CSV reader rather than
    a cut/awk pipeline.  Nothing is kept but the shapes themselves.
    """
    url = PRICING_URL % region
    prefixes = tuple(f + "." for f in families)
    shapes = {}
    seen = 0

    with urllib.request.urlopen(url, timeout=120) as resp:
        stream = io.TextIOWrapper(resp, encoding="utf-8", newline="")
        reader = csv.reader(stream)
        header = None
        for row in reader:
            if header is None:
                # Preamble rows are two-column "Key","Value" pairs.
                if len(row) > 10 and "Instance Type" in row:
                    header = {name: i for i, name in enumerate(row)}
                continue
            seen += 1
            try:
                name = row[header["Instance Type"]]
                vcpu = row[header["vCPU"]]
                mem = row[header["Memory"]]
            except IndexError:
                continue
            if not name.startswith(prefixes) or name.endswith(".metal"):
                continue
            if not vcpu or not mem.endswith(" GiB"):
                continue
            shape = (vcpu, fmt_mem(float(mem[: -len(" GiB")])))
            prev = shapes.setdefault(name, shape)
            if prev != shape:
                print(
                    f"warning: {name} reported as {prev} and {shape}; keeping {prev}",
                    file=sys.stderr,
                )
    print(f"scanned {seen} price rows, kept {len(shapes)} instance types",
          file=sys.stderr)
    return shapes


def fetch_gce():
    """Return {name: (vcpus, memory_gib)} from gcloud, or None if unavailable."""
    cmd = [
        "gcloud", "compute", "machine-types", "list",
        "--format=csv[no-heading](name,guestCpus,memoryMb)",
    ]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=180,
                             stdin=subprocess.DEVNULL)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    if out.returncode != 0:
        first = (out.stderr or "").strip().splitlines()
        print("note: gcloud unavailable (%s); preserving GCE rows from vm.csv"
              % (first[0] if first else "no detail"), file=sys.stderr)
        return None

    shapes = {}
    for line in out.stdout.splitlines():
        parts = line.split(",")
        if len(parts) != 3:
            continue
        name, cpus, mem_mb = parts
        shapes.setdefault(name, (cpus, fmt_mem(float(mem_mb) / 1024)))
    return shapes


def ec2_sort_key(name):
    family, _, size = name.partition(".")
    order = ["large", "xlarge"]
    if size in order:
        rank = order.index(size)
    elif size.endswith("xlarge"):
        rank = 1 + int(size[: -len("xlarge")])
    else:
        rank = -1
    return (family, rank, name)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--region", default="us-east-1",
                    help="AWS region whose price list to read (default: us-east-1)")
    ap.add_argument("--check", action="store_true",
                    help="report what would change and exit non-zero; write nothing")
    args = ap.parse_args()

    existing = read_existing(VM_CSV)
    existing_gce = [r for r in existing if not is_ec2(r[0])]

    gce = fetch_gce()
    if gce is None:
        gce_rows = existing_gce
    else:
        # Keep the order already in the file so refreshes stay reviewable, then
        # append anything new.
        known = {r[0] for r in existing_gce}
        gce_rows = [(n, *gce[n]) for n, _, _ in existing_gce if n in gce]
        gce_rows += [(n, *v) for n, v in sorted(gce.items()) if n not in known]

    ec2 = fetch_ec2(args.region, EC2_FAMILIES)
    ec2_rows = [(n, *ec2[n]) for n in sorted(ec2, key=ec2_sort_key)]

    rows = gce_rows + ec2_rows

    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["name", "vcpus", "memory"])
    w.writerows(rows)
    out = buf.getvalue()

    with open(VM_CSV) as f:
        current = f.read()
    if current == out:
        print(f"vm.csv up to date ({len(rows)} shapes)")
        return 0
    if args.check:
        print(f"vm.csv would change: {len(existing)} -> {len(rows)} shapes")
        return 1
    with open(VM_CSV, "w") as f:
        f.write(out)
    print(f"wrote {VM_CSV}: {len(gce_rows)} GCE + {len(ec2_rows)} EC2 = {len(rows)} shapes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
