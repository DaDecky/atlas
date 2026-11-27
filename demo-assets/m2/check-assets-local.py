"""Render and check exam assets offline; no kubectl command accesses a server."""

import argparse
import ast
import json
import os
from pathlib import Path
import subprocess
import sys


ASSETS = Path(__file__).resolve().parent
ROOT = ASSETS.parents[1]
PRIVATE = ROOT / ".deploy" / "demo-private"
sys.path.insert(0, str(PRIVATE / "python-deps"))
import yaml


def require(condition, message):
    if not condition:
        raise ValueError(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check-private", action="store_true", help="also check local exam keys and environment; never prints their values")
    args = parser.parse_args()
    checks = PRIVATE / "checks"
    checks.mkdir(parents=True, exist_ok=True)
    result = {"cluster_access": False, "exam_started": False}
    for path in ASSETS.rglob("*.json"):
        if "python-deps" not in path.parts and "evidence" not in path.parts and "checks" not in path.parts:
            json.loads(path.read_text(encoding="utf-8"))
    for path in [ASSETS / "prepare-local.py", ASSETS / "export-audit.py", ASSETS / "check-go-local.py", Path(__file__)]:
        ast.parse(path.read_text(encoding="utf-8"))
    result["python_and_json_parse"] = True
    for label, target in [("base_demo", ROOT / "deploy/k8s/overlays/demo"), ("exam", ASSETS / "exam-overlay")]:
        rendered = subprocess.run(["kubectl", "kustomize", str(target)], cwd=ROOT, capture_output=True, timeout=120)
        require(rendered.returncode == 0, rendered.stderr.decode(errors="replace"))
        (PRIVATE / (label + "-rendered.yaml")).write_bytes(rendered.stdout)
        objects = [obj for obj in yaml.safe_load_all(rendered.stdout) if obj]
        result[label + "_objects"] = len(objects)
        if label == "exam":
            indexed = {(obj["kind"], obj["metadata"]["name"]): obj for obj in objects}
            sensor = indexed[("Deployment", "sensor-pantai")]["spec"]["template"]["spec"]
            container = next(item for item in sensor["containers"] if item["name"] == "sensor")
            values = {item["name"]: item.get("value") for item in container["env"]}
            materials = json.loads((ASSETS / "materials.json").read_text())
            require(container["image"] == materials["sensor_image"], "Sensor digest mismatch")
            require(values["PANTAI_PROFILE"] == "exam" and values["PANTAI_SEED"] == "42", "Sensor exam profile/seed mismatch")
            require(any(item["mountPath"] == "/var/run/pantai" and item["name"] == "key" for item in container["volumeMounts"]), "Missing sensor key mount")
            require(any(item.get("secret", {}).get("secretName") == "pantai-key" for item in sensor["volumes"]), "Missing sensor key Secret reference")
            for name, expected in [("aggregator", 3), ("auth", 2), ("client-api", 3)]:
                deployment = indexed[("Deployment", name)]
                require(deployment["spec"]["replicas"] == expected, "Replica count mismatch: " + name)
                item = next(item for item in deployment["spec"]["template"]["spec"]["containers"] if item["name"] == name)
                require(item["image"].endswith(":milestone-2"), "Image tag mismatch: " + name)
                settings = {setting["name"]: setting.get("value") for setting in item["env"]}
                require(settings.get("OTEL_TRACES_SAMPLER_ARG") == "0.1", "Telemetry sampling missing: " + name)
                if name == "aggregator":
                    require(settings.get("INGESTION_DISABLED") == "0", "Ingestion is not enabled")
            require(("Deployment", "victoriametrics") in indexed and ("Deployment", "jaeger") in indexed, "Telemetry backends missing")
            require(all(not obj.get("data") and not obj.get("stringData") for obj in objects if obj["kind"] == "Secret"), "Unexpected credential data in rendered exam manifest")
            result["exam_configuration"] = "sensor exam + key mount, 3/2/3 replicas, ingestion, telemetry, milestone-2 tags"
    tester = list(yaml.safe_load_all((ASSETS / "tester-manifest.yaml").read_text(encoding="utf-8")))
    agent = next(obj for obj in tester if obj and obj["kind"] == "DaemonSet")
    spec = agent["spec"]["template"]["spec"]
    require(spec["containers"][0]["image"] == materials["tester_image"], "Tester digest mismatch")
    require(any(item.get("secret", {}).get("secretName") == "tester-key" for item in spec["volumes"]), "Tester key Secret mismatch")
    require(any(item["mountPath"] == "/var/run/tester" for item in spec["containers"][0]["volumeMounts"]), "Tester key mount mismatch")
    require(any(item.get("operator") == "Exists" for item in spec["tolerations"]), "Tester does not tolerate control-plane taints")
    result["tester_manifest"] = "digest, Secret, mount, control-plane toleration verified"
    fixture_results = {}
    for label, expected in [("match", 0), ("mismatch", 1)]:
        output = checks / ("p4-synthetic-" + label + ".json")
        process = subprocess.run([sys.executable, str(ROOT / "scripts/satellite-reference-diff.py"),
                                  "--actual", str(ASSETS / ("p4/synthetic-" + label + ".json")),
                                  "--reference", str(ASSETS / "p4/synthetic-reference.json"), "--output", str(output)],
                                 capture_output=True, text=True, timeout=30)
        require(process.returncode == expected, "Unexpected reference diff result: " + label)
        value = json.loads(output.read_text())
        require(value["membership_and_fields_match"] == (label == "match"), "Reference comparison flag mismatch")
        if label == "mismatch":
            require(any(item["field"] == "total_frp_mw" for item in value["field_differences"]), "Synthetic FRP mismatch was not detected")
        fixture_results[label] = {"exit_code": process.returncode, "expected_exit_code": expected}
    result["synthetic_reference_checks"] = fixture_results
    if args.check_private:
        for name in ["pantai.key", "tester.key"]:
            raw = (PRIVATE / name).read_bytes()
            require(raw.startswith(b"01-Atlas.") and raw.endswith(b"\n") and len(raw.splitlines()) == 1, "Exam key format invalid: " + name)
        environment = dict(line.split("=", 1) for line in (ROOT / ".env.m2").read_text(encoding="utf-8-sig").splitlines() if line and not line.startswith("#") and "=" in line)
        require(all(environment.get(name) for name in ["SATELLITE_TOKEN", "AUDIT_TOKEN", "PANTAI_AUTH_TOKEN"]), "Required local demo credential missing")
        for path in [ROOT / ".env.m2", PRIVATE / "pantai.key", PRIVATE / "tester.key", PRIVATE / "exam-rendered.yaml"]:
            process = subprocess.run(["git", "check-ignore", "--quiet", "--", str(path)], cwd=ROOT)
            require(process.returncode == 0, "Private demo file is not ignored by Git: " + path.name)
        result["private_files"] = "required values present, key format valid, files ignored by Git"
    else:
        result["private_files"] = "not checked; use --check-private after configuring local credentials"
    result["passed"] = True
    (checks / "asset-results.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
