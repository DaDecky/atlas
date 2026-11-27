"""Build runtime modules and run their ordinary unit tests, without integration tags."""

import datetime
import json
import os
from pathlib import Path
import subprocess


ASSETS = Path(__file__).resolve().parent
ROOT = ASSETS.parents[1]
PRIVATE = ROOT / ".deploy" / "demo-private"
MODULES = [
    "services/bmkg", "services/pvmbg", "services/bnpb/auth", "services/bnpb/client-api",
    "services/bnpb/aggregator", "services/bnpb/telemetry", "consumers/consumer-a",
    "consumers/consumer-b", "consumers/notifier",
]


def main():
    logs = PRIVATE / "checks" / "go"
    logs.mkdir(parents=True, exist_ok=True)
    result = {"integration_tags": False, "modules": []}
    environment = os.environ.copy()
    environment["GOFLAGS"] = ""
    for relative in MODULES:
        row = {"module": relative}
        module = ROOT / relative
        version = subprocess.run(["go", "version"], cwd=module, capture_output=True, text=True, timeout=180)
        row["toolchain"] = version.stdout.strip()
        for name, command in [("build", ["go", "build", "-mod=readonly", "./..."]),
                              ("unit_tests", ["go", "test", "-mod=readonly", "-count=1", "-timeout=5m", "./..."])]:
            path = logs / (relative.replace("/", "-") + "-" + name + ".log")
            try:
                with path.open("wb") as output:
                    process = subprocess.run(command, cwd=module, env=environment,
                                             stdout=output, stderr=subprocess.STDOUT, timeout=360)
                row[name] = {"exit_code": process.returncode, "log": str(path.relative_to(PRIVATE))}
            except subprocess.TimeoutExpired:
                row[name] = {"exit_code": 124, "log": str(path.relative_to(PRIVATE)), "error": "timed out"}
            print(relative, name, row[name]["exit_code"], flush=True)
        result["modules"].append(row)
        result["passed"] = all(item[check]["exit_code"] == 0 for item in result["modules"] for check in ["build", "unit_tests"])
        result["finished_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        (PRIVATE / "checks" / "go-results.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    raise SystemExit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
