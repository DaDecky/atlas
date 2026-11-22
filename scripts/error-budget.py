"""Reports how much of the Field Team alert budget a window used.

Two objectives cover the path. No critical event is lost, with a budget of zero, and 99 percent
of critical events arrive within 5 s, with a budget of 1 percent. The expected events come from
Sensor Pantai itself, so an event lost before Atlas stored it still counts as lost. The deliveries
come from the notifier log. Each observation maps to the hazard_id the Aggregator derives for it.

A critical observation that breaks the base contract, or names a station missing from /v1/stations,
is parked by design (P5 criterion 2). It is reported separately and not counted as lost.

The latency objective uses the notifier's latency_ms, measured from Atlas ingestion. The report also
prints the time from observed_at to delivery, which includes the sensor's own publication delay.

Usage: kubectl logs deploy/notifier | python scripts/error-budget.py --sensor URL --since 2026-11-20T10:00:00Z
The Sensor Pantai token is read from PANTAI_AUTH_TOKEN.
"""

import argparse
import datetime
import hashlib
import json
import math
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

LATENCY_TARGET_MS = 5000
LATE_SHARE = 0.01
RFC3339 = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})")


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


def number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def violation(item, generated: datetime.datetime, stations: set[str]) -> str:
    """Mirrors decodeObservation and the station check in the Aggregator."""
    if not isinstance(item, dict):
        return "not an object"

    for name in ["observation_id", "station_id", "quality"]:
        if not isinstance(item.get(name), str) or not item[name]:
            return f"{name} missing or empty"

    observed = item.get("observed_at")

    if not isinstance(observed, str) or not RFC3339.fullmatch(observed):
        return "observed_at is not RFC 3339"

    if parse_time(observed) > generated:
        return "observed_at is later than generated_at"

    if not number(item.get("sea_level_anomaly_m")) or not number(item.get("wave_height_m")) or item["wave_height_m"] < 0:
        return "sea level or wave height invalid"

    if item["station_id"] not in stations:
        return "unknown station"

    return ""


def station_ids(sensor: str, token: str) -> set[str]:
    page = fetch(f"{sensor}/v1/stations", token)

    return {item["station_id"] for item in page["items"] if isinstance(item, dict) and isinstance(item.get("station_id"), str)}


def critical_events(sensor: str, token: str, since: str) -> tuple[dict[str, tuple[str, datetime.datetime]], int]:
    """Returns each valid critical event keyed by hazard_id, and the count of parked critical ones."""
    start, wanted, parked, cursor = parse_time(since), {}, set(), ""
    stations = station_ids(sensor, token)

    while True:
        query = {"limit": "1000"}

        if cursor:
            query["cursor"] = cursor

        page = fetch(f"{sensor}/v1/observations?{urllib.parse.urlencode(query)}", token)

        try:
            generated = parse_time(page["generated_at"])
        except (KeyError, TypeError, ValueError):
            generated = datetime.datetime.now(datetime.timezone.utc)

        for item in page["items"]:
            if not isinstance(item, dict) or item.get("critical") is not True:
                continue

            reason = violation(item, generated, stations)
            ref = item.get("observation_id")

            if reason:
                parked.add(repr(ref))
                continue

            observed = parse_time(item["observed_at"])

            if observed >= start:
                wanted[hazard_id(ref)] = (ref, observed)

        cursor = page.get("next_cursor")

        if not cursor:
            return wanted, len(parked)


def deliveries(lines: list[str]) -> dict[str, tuple[int, datetime.datetime | None]]:
    """Returns the latency and log time of the first delivery of each hazard."""
    first: dict[str, tuple[int, datetime.datetime | None]] = {}

    for line in lines:
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue

        if record.get("msg") == "field_alert_delivered":
            try:
                logged = parse_time(record["time"])
            except (KeyError, TypeError, ValueError):
                logged = None

            first.setdefault(record["hazard_id"], (record["latency_ms"], logged))

    return first


def percentile95(values: list[int]) -> int:
    return sorted(values)[math.ceil(0.95 * len(values)) - 1] if values else 0


def report(wanted: dict[str, tuple[str, datetime.datetime]], parked: int, delivered: dict) -> list[str]:
    lost = sorted(wanted[hazard][0] for hazard in wanted.keys() - delivered.keys())
    both = wanted.keys() & delivered.keys()
    latencies = sorted(delivered[hazard][0] for hazard in both)
    end_to_end = [
        int((delivered[hazard][1] - wanted[hazard][1]).total_seconds() * 1000) for hazard in both if delivered[hazard][1]
    ]
    late = sum(1 for value in latencies if value > LATENCY_TARGET_MS)
    allowed = math.floor(len(wanted) * LATE_SHARE)
    p95 = percentile95(latencies)
    used = "none" if late == 0 else ("exceeded" if late > allowed else f"{late / allowed:.0%}")
    loss = "met" if not lost else "exceeded, lost observations " + ", ".join(lost)

    return [
        f"Critical events {len(wanted)}, delivered {len(latencies)}, lost {len(lost)}",
        f"Critical observations that break the contract and are parked: {parked}",
        f"Loss objective: budget 0, {loss}",
        f"Latency objective: p95 {p95} ms, {late} over {LATENCY_TARGET_MS} ms, {allowed} allowed, budget used {used}",
        f"Observed-to-delivered p95 {percentile95(end_to_end)} ms over {len(end_to_end)} events",
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sensor", required=True, help="Sensor Pantai origin, for example a kubectl port-forward")
    parser.add_argument("--since", required=True, help="window start in ISO 8601 UTC, compared with observed_at")

    args = parser.parse_args()
    token = os.environ.get("PANTAI_AUTH_TOKEN")

    if not token:
        sys.exit("PANTAI_AUTH_TOKEN is required")

    wanted, parked = critical_events(args.sensor.rstrip("/"), token, args.since)

    print("\n".join(report(wanted, parked, deliveries(sys.stdin.readlines()))))


if __name__ == "__main__":
    main()
