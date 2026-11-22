#!/usr/bin/env python3
"""Compare active fire membership and mapped values with a supplied JSON reference.

Inputs: arrays of flat fire objects, arrays of SATELIT HazardEvents, or the
Audit/public envelopes {items:[...]} / {data:[...]}. IDs are reported separately
because an independent reference may use another stable identity convention.
"""
import argparse
import json
import math
from pathlib import Path


def read_groups(path):
    if path.stat().st_size > 64 << 20:
        raise ValueError("input exceeds 64 MiB")
    value = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(value, dict):
        value = value.get("items", value.get("data"))
    if not isinstance(value, list):
        raise ValueError("expected a JSON array or Audit/public event envelope")
    groups, owners = {}, set()
    for row in value:
        if not isinstance(row, dict):
            raise ValueError("each event must be an object")
        if row.get("source") and row["source"] != "SATELIT":
            continue
        attributes = row.get("attributes", row)
        if not isinstance(attributes, dict):
            raise ValueError("attributes must be an object")
        if attributes.get("status", "active") != "active":
            if attributes.get("detection_keys"):
                raise ValueError("inactive event still owns detections")
            continue
        keys = attributes.get("detection_keys")
        if not isinstance(keys, list) or len(keys) < 2 or any(not isinstance(key, str) for key in keys):
            raise ValueError("active fire needs at least two string detection_keys")
        if len(set(keys)) != len(keys) or owners.intersection(keys):
            raise ValueError("a detection belongs to multiple fires")
        owners.update(keys)
        key = tuple(sorted(keys))
        groups[key] = {
            "id": row.get("source_ref_id", row.get("fire_id")),
            "detection_count": attributes.get("detection_count", len(keys)),
            "total_frp_mw": attributes.get("total_frp_mw"),
            "severity": row.get("severity"),
            "latitude": row.get("latitude"),
            "longitude": row.get("longitude"),
            "area_name": row.get("area_name"),
            "occurred_at": row.get("occurred_at"),
            "satellites": sorted(attributes["satellites"]) if "satellites" in attributes else None,
            "max_confidence": max_confidence(attributes),
        }
    return groups


def max_confidence(attributes):
    if "max_confidence" in attributes:
        return attributes["max_confidence"]
    legacy = attributes.get("highest_confidence")
    if legacy is None:
        return None
    if not isinstance(legacy, str):
        raise ValueError("highest_confidence must be a string")
    return {"low": "l", "nominal": "n", "high": "h"}.get(legacy, legacy)


def compare(actual, expected):
    missing, extra = sorted(expected.keys() - actual.keys()), sorted(actual.keys() - expected.keys())
    differences, identities = [], []
    for group in sorted(actual.keys() & expected.keys()):
        a, b = actual[group], expected[group]
        if a["id"] is not None and b["id"] is not None and a["id"] != b["id"]:
            identities.append({"detection_keys": list(group), "actual": a["id"], "reference": b["id"]})
        for field, wanted in b.items():
            if field == "id" or wanted is None:
                continue
            got = a[field]
            if field == "occurred_at" and got is not None:
                from datetime import datetime
                equal = datetime.fromisoformat(got.replace("Z", "+00:00")) == datetime.fromisoformat(wanted.replace("Z", "+00:00"))
            elif isinstance(wanted, (int, float)) and isinstance(got, (int, float)):
                equal = math.isclose(got, wanted, rel_tol=0, abs_tol=1e-6)
            else:
                equal = got == wanted
            if not equal:
                differences.append({"detection_keys": list(group), "field": field, "actual": got, "reference": wanted})
    return {"actual_active_fires": len(actual), "reference_active_fires": len(expected), "missing_groups": [list(key) for key in missing], "extra_groups": [list(key) for key in extra], "field_differences": differences, "identity_differences": identities, "membership_and_fields_match": not missing and not extra and not differences}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--actual", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        result = compare(read_groups(args.actual), read_groups(args.reference))
    except (OSError, ValueError, TypeError) as error:
        parser.exit(2, "invalid reference input: " + str(error) + "\n")
    raw = json.dumps(result, indent=2, ensure_ascii=False) + "\n"
    if args.output:
        args.output.write_text(raw, encoding="utf-8")
    print(raw, end="")
    return 0 if result["membership_and_fields_match"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
