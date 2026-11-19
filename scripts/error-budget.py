"""Reports how much of the Field Team alert budget a window used.

Two objectives cover the path. No critical event is lost, with a budget of zero, and 99 percent
of critical events arrive within 5 s, with a budget of 1 percent. The expected events come from the
Audit API and the deliveries from the notifier log, so the check needs no access to the stores.

Usage: kubectl logs deploy/notifier | python scripts/error-budget.py --audit URL --since 2026-11-20T10:00:00Z
The Audit API token is read from AUDIT_TOKEN.
"""

import argparse
import json
import math
import os
import sys
import urllib.parse
import urllib.request

LATENCY_TARGET_MS = 5000
LATE_SHARE = 0.01


def critical_events(audit: str, token: str, since: str) -> set[str]:
    wanted, cursor = set(), ""

    while True:
        query = {"source": "SENSOR_PANTAI", "since": since, "limit": "1000"}

        if cursor:
            query["cursor"] = cursor

        url = f"{audit}/internal/audit/events?{urllib.parse.urlencode(query)}"
        request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})

        with urllib.request.urlopen(request, timeout=10) as response:
            page = json.load(response)

        if page["partial"]:
            sys.exit(f"Audit API answered partial, units {page['unavailable_units']} unreachable. Rerun later.")

        wanted |= {item["hazard_id"] for item in page["items"] if item["attributes"].get("critical") is True}
        cursor = page["next_cursor"]

        if not cursor:
            return wanted


def deliveries(lines: list[str]) -> dict[str, int]:
    """Returns the latency of the first delivery of each hazard."""
    first: dict[str, int] = {}

    for line in lines:
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue

        if record.get("msg") == "field_alert_delivered":
            first.setdefault(record["hazard_id"], record["latency_ms"])

    return first


def report(wanted: set[str], delivered: dict[str, int]) -> list[str]:
    lost = sorted(wanted - delivered.keys())
    latencies = sorted(delivered[hazard] for hazard in wanted & delivered.keys())
    late = sum(1 for value in latencies if value > LATENCY_TARGET_MS)
    allowed = math.floor(len(wanted) * LATE_SHARE)
    p95 = latencies[math.ceil(0.95 * len(latencies)) - 1] if latencies else 0
    used = "none" if late == 0 else ("exceeded" if late > allowed else f"{late / allowed:.0%}")
    loss = "met" if not lost else "exceeded, lost " + ", ".join(lost)

    return [
        f"Critical events {len(wanted)}, delivered {len(latencies)}, lost {len(lost)}",
        f"Loss objective: budget 0, {loss}",
        f"Latency objective: p95 {p95} ms, {late} over {LATENCY_TARGET_MS} ms, {allowed} allowed, budget used {used}",
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--audit", required=True, help="Aggregator origin that serves the Audit API")
    parser.add_argument("--since", required=True, help="window start in ISO 8601 UTC, compared with ingested_at")

    args = parser.parse_args()
    token = os.environ.get("AUDIT_TOKEN")

    if not token:
        sys.exit("AUDIT_TOKEN is required")

    wanted = critical_events(args.audit.rstrip("/"), token, args.since)

    print("\n".join(report(wanted, deliveries(sys.stdin.readlines()))))


if __name__ == "__main__":
    main()
