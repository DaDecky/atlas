"""Read all Audit pages with partial/cursor checks; invoke only when Atlas is available."""

import argparse
import datetime
import json
import os
from pathlib import Path
import urllib.error
import urllib.parse
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=["BMKG", "PVMBG", "SATELIT", "SENSOR_PANTAI"], default="SATELIT")
    parser.add_argument("--origin", default=os.environ.get("AUDIT_URL", "http://localhost:8083"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--since", help="optional RFC3339 ingested_at boundary supported by Audit")
    args = parser.parse_args()
    token = os.environ.get("AUDIT_TOKEN")
    if not token:
        parser.error("AUDIT_TOKEN must be supplied through the environment")
    origin = urllib.parse.urlparse(args.origin)
    if origin.scheme not in ("http", "https") or not origin.netloc or origin.username or origin.password or origin.query or origin.fragment:
        parser.error("origin must be an HTTP(S) origin without credentials, query or fragment")
    rows, seen, cursor = [], set(), None
    started = datetime.datetime.now(datetime.timezone.utc).isoformat()
    try:
        while True:
            query = {"source": args.source, "limit": 1000}
            if args.since:
                query["since"] = args.since
            if cursor:
                query["cursor"] = cursor
            request = urllib.request.Request(
                args.origin.rstrip("/") + "/internal/audit/events?" + urllib.parse.urlencode(query),
                headers={"Authorization": "Bearer " + token},
            )
            with urllib.request.urlopen(request, timeout=60) as response:
                page = json.load(response)
            if page.get("partial") or page.get("unavailable_units"):
                raise ValueError("Audit export is partial; restore storage before comparison")
            if not isinstance(page.get("items"), list):
                raise ValueError("Audit response does not contain an items array")
            rows.extend(page["items"])
            cursor = page.get("next_cursor")
            if not cursor:
                break
            if not isinstance(cursor, str) or cursor in seen:
                raise ValueError("Audit cursor is invalid or repeated")
            seen.add(cursor)
        raw = json.dumps(rows, ensure_ascii=False, indent=2) + "\n"
        if len(raw.encode("utf-8")) > 64 << 20:
            raise ValueError("Export exceeds the reference-diff tool's 64 MiB input limit")
        ids = [row.get("hazard_id") for row in rows]
        if any(not identity for identity in ids) or len(ids) != len(set(ids)):
            raise ValueError("Missing or duplicate hazard IDs; use a stable comparison window")
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.output.with_suffix(args.output.suffix + ".tmp")
        temporary.write_text(raw, encoding="utf-8")
        temporary.replace(args.output)
        metadata = {"source": args.source, "events": len(rows), "pages": len(seen) + 1,
                    "started_at": started, "finished_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "atomic_snapshot": False, "window_note": "align with the assistant reference offset/window"}
        args.output.with_suffix(args.output.suffix + ".metadata.json").write_text(
            json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(metadata))
    except (OSError, ValueError, urllib.error.URLError) as error:
        parser.exit(1, str(error) + "\n")


if __name__ == "__main__":
    main()
