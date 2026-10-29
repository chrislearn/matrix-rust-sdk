#!/usr/bin/env python3
"""Require actual named, nonzero Callgrind measurements in an offline suite.

Format checked against CodSpeed runner 5.2.1 / codspeed-rust 4.7.0 and real
combined profiles from PR2 run40925034250. summary may be zero; totals ends each
part. Metadata and program-termination parts alone are never measurements.
"""
import argparse
import json
from pathlib import Path
import re
import sys

SUITES = {"crypto_bench", "event_cache", "linked_chunk", "store_bench", "timeline", "room_list"}
METADATA = "Metadata: codspeed-rust 4.7.0"
TRIGGER = "desc: Trigger: Client Request: "
POSITION = re.compile(r"(?:\*|[+-]?\d+|0x[0-9a-fA-F]+)\Z")


def measurements(path, suite):
    """Stream one combined profile; correlate trigger, costs and final totals."""
    expected = f"benchmarks/benches/{suite}.rs::"
    metadata = False
    header = set()
    names = []
    part = None

    def finish():
        if part is None or not part["name"].startswith(expected):
            return
        name = part["name"]
        events = part["events"]
        totals = part["totals"]
        if (len(name) == len(expected) or "Ir" not in events or totals is None
                or events.index("Ir") >= len(totals)
                or totals[events.index("Ir")] <= 0 or not part["cost"] or not part["function"]):
            raise ValueError(f"{path}: incomplete or zero instruction measurement: {name}")
        names.append(name)

    with path.open(encoding="utf-8", errors="replace") as source:
        for line in source:
            line = line.strip()
            if line == "# callgrind format" or line == "version: 1" or line.startswith("creator: callgrind-"):
                header.add("creator" if line.startswith("creator:") else line)
            elif line.startswith("part:"):
                finish()
                if not line[5:].strip().isdigit():
                    raise ValueError(f"{path}: invalid part header")
                part = {"name": "", "events": [], "positions": 0, "totals": None,
                        "cost": False, "function": False}
            elif part is not None:
                if line.startswith(TRIGGER):
                    part["name"] = line[len(TRIGGER):]
                    metadata |= part["name"] == METADATA
                elif line.startswith("events:"):
                    part["events"] = line[7:].split()
                elif line.startswith("positions:"):
                    part["positions"] = len(line[10:].split())
                elif line.startswith("fn="):
                    part["function"] = bool(line[3:])
                elif line.startswith("totals:"):
                    fields = line[7:].split()
                    if not fields or any(not value.isascii() or not value.isdigit() for value in fields):
                        raise ValueError(f"{path}: invalid totals")
                    part["totals"] = [int(value) for value in fields]
                elif part["positions"] and "Ir" in part["events"]:
                    fields = line.split()
                    positions = part["positions"]
                    index = positions + part["events"].index("Ir")
                    if (len(fields) > index and all(POSITION.fullmatch(v) for v in fields[:positions])
                            and fields[index].isascii() and fields[index].isdigit()
                            and int(fields[index]) > 0):
                        part["cost"] = True
    finish()
    if names and header != {"# callgrind format", "version: 1", "creator"}:
        raise ValueError(f"{path}: measurements lack a valid Callgrind header")
    if names and not metadata:
        raise ValueError(f"{path}: measurements lack the pinned Rust integration metadata")
    return names


def validate(root, suite):
    if suite not in SUITES:
        raise ValueError(f"unsupported benchmark suite: {suite}")
    if not root.is_dir():
        raise ValueError(f"profile directory is absent: {root}")
    found = []
    for folder in sorted(root.glob("profile.*.out")):
        if folder.is_dir():
            for path in sorted(folder.glob("*.out")):
                if path.is_file():
                    found.extend(measurements(path, suite))
    if not found:
        raise ValueError(f"{suite}: no named nonzero Callgrind measurements; logs/metadata alone do not qualify")
    return {"suite": suite, "measurements": len(found), "names": found}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile_root", type=Path)
    parser.add_argument("suite", choices=sorted(SUITES))
    args = parser.parse_args()
    try:
        result = validate(args.profile_root, args.suite)
    except (OSError, ValueError) as error:
        print(f"Offline benchmark validation failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
