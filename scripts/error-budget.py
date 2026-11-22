"""Reports how much of the Field Team alert budget a window used.

Two objectives cover the path. No critical event is lost, with a budget of zero, and 99 percent
of critical events arrive within 5 s, with a budget of 1 percent. The expected events come from
Sensor Pantai itself, so an event lost before Atlas stored it still counts as lost. The deliveries
come from the notifier log. Each observation maps to the hazard_id the Aggregator derives for it.

Usage: kubectl logs deploy/notifier | python scripts/error-budget.py --sensor URL --since 2026-11-20T10:00:00Z
The Sensor Pantai token is read from PANTAI_AUTH_TOKEN.
"""

import argparse
import datetime
import hashlib
import json
import math
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

LATENCY_TARGET_MS = 5000
LATE_SHARE = 0.01


def hazard_id(observation_id: str) -> str:
    """Matches CanonicalHazardID in the Aggregator."""
    return hashlib.sha256(f"SENSOR_PANTAI\x00{observation_id}".encode()).hexdigest()


def parse_time(value: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))


def fetch(url: str, token: str) -> dict:
    """Sensor Pantai may answer errors on purpose, so each page is retried before giving up."""
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})

    for attempt in range(5):
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                page = json.load(response)

            if isinstance(page, dict) and isinstance(page.get("items"), list):
                return page
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            pass

        time.sleep(2**attempt)

    sys.exit(f"Sensor Pantai did not return a valid page for {url}. Rerun later.")


def critical_events(sensor: str, token: str, since: str) -> dict[str, str]:
    """Returns the observation_id of each critical event, keyed by hazard_id."""
    start, wanted, cursor = parse_time(since), {}, ""

    while True:
        query = {"limit": "1000"}

        if cursor:
            query["cursor"] = cursor

        page = fetch(f"{sensor}/v1/observations?{urllib.parse.urlencode(query)}", token)

        for item in page["items"]:
            ref, observed = item.get("observation_id"), item.get("observed_at")

            if item.get("critical") is not True or not isinstance(ref, str) or not isinstance(observed, str):
                continue

            try:
                if parse_time(observed) >= start:
                    wanted[hazard_id(ref)] = ref
            except ValueError:
                continue

        cursor = page.get("next_cursor")

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


def report(wanted: dict[str, str], delivered: dict[str, int]) -> list[str]:
    lost = sorted(wanted[hazard] for hazard in wanted.keys() - delivered.keys())
    latencies = sorted(delivered[hazard] for hazard in wanted.keys() & delivered.keys())
    late = sum(1 for value in latencies if value > LATENCY_TARGET_MS)
    allowed = math.floor(len(wanted) * LATE_SHARE)
    p95 = latencies[math.ceil(0.95 * len(latencies)) - 1] if latencies else 0
    used = "none" if late == 0 else ("exceeded" if late > allowed else f"{late / allowed:.0%}")
    loss = "met" if not lost else "exceeded, lost observations " + ", ".join(lost)

    return [
        f"Critical events {len(wanted)}, delivered {len(latencies)}, lost {len(lost)}",
        f"Loss objective: budget 0, {loss}",
        f"Latency objective: p95 {p95} ms, {late} over {LATENCY_TARGET_MS} ms, {allowed} allowed, budget used {used}",
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sensor", required=True, help="Sensor Pantai origin, for example a kubectl port-forward")
    parser.add_argument("--since", required=True, help="window start in ISO 8601 UTC, compared with observed_at")

    args = parser.parse_args()
    token = os.environ.get("PANTAI_AUTH_TOKEN")

    if not token:
        sys.exit("PANTAI_AUTH_TOKEN is required")

    wanted = critical_events(args.sensor.rstrip("/"), token, args.since)

    print("\n".join(report(wanted, deliveries(sys.stdin.readlines()))))


if __name__ == "__main__":
    main()
