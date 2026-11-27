"""Shared local configuration and subprocess helpers for cluster/demo commands."""

import datetime
import ipaddress
import json
import os
from pathlib import Path
import re
import runpy
import shlex
import subprocess
import sys

ASSETS = Path(__file__).resolve().parent
ROOT = ASSETS.parents[1]
PRIVATE = ROOT / ".deploy" / "demo-private"
KUBECONFIG = PRIVATE / "cluster" / "kubeconfig.yaml"
EXPECTED_NODES = {"atlas-node-a": "10.0.0.40", "atlas-node-b": "10.0.0.41", "atlas-node-c": "10.0.0.42"}


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def load_config(path=None):
    candidate = Path(path) if path else PRIVATE / "cluster.json"
    if not candidate.exists():
        if path:
            raise ValueError("Config file does not exist: " + str(path))
        candidate = ASSETS / "cluster.example.json"
    config = json.loads(candidate.read_text(encoding="utf-8-sig"))
    validate_config(config)
    return config


def validate_config(config):
    nodes = config.get("nodes", [])
    if len(nodes) != 3 or {node.get("name") for node in nodes} != set(EXPECTED_NODES):
        raise ValueError("Atlas needs exactly atlas-node-a/b/c, with unique names")
    if len({node.get("ssh") for node in nodes}) != 3:
        raise ValueError("Each VM needs a distinct SSH alias")
    for node in nodes:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node.get("ssh", "")):
            raise ValueError("Use SSH aliases from ~/.ssh/config, without shell characters or user@host")
        if str(ipaddress.ip_address(node["ip"])) != EXPECTED_NODES[node["name"]]:
            raise ValueError("Node IPs must match the current Atlas NetworkPolicies: 10.0.0.40/41/42")
        if node.get("role") != ("server" if node["name"] == "atlas-node-a" else "agent"):
            raise ValueError("atlas-node-a is the server; b/c are agents")
    for field, expected in [("pod_cidr", "10.42.0.0/16"), ("service_cidr", "10.43.0.0/16"), ("node_network", "10.0.0.0/24")]:
        if str(ipaddress.ip_network(config.get(field, ""))) != expected:
            raise ValueError("NetworkPolicies expect " + field + "=" + expected)
    if not re.fullmatch(r"v1\.\d+\.\d+\+k3s\d+", config.get("k3s_version", "")):
        raise ValueError("Invalid pinned K3s version")
    if config.get("namespace") != "sistem-informasi-kebencanaan-bnpb":
        raise ValueError("Namespace must match the vendor/tester manifests")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", config.get("context", "")):
        raise ValueError("Invalid kubeconfig context name")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", config.get("image_tag", "")):
        raise ValueError("Invalid image tag")


def read_environment(path=None):
    path = Path(path) if path else ROOT / ".env.m2"
    if not path.exists():
        raise ValueError("Create .env.m2 with: python demo-assets/m2/demo.py init-env")
    values = {}
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key in values:
            raise ValueError("Duplicate environment key: " + key)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key] = value
    return values


def write_private(path, content):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = content.encode("utf-8") if isinstance(content, str) else content
    path.write_bytes(raw)
    helpers = runpy.run_path(str(ROOT / "scripts" / "deploy-k8s.py"), run_name="atlas_private_permissions")
    helpers["protect"](path)


class Runner:
    def __init__(self, dry_run=False):
        self.dry_run = dry_run

    def run(self, arguments, *, data=None, private=False, env=None, timeout=1200, stream=False):
        arguments = [str(value) for value in arguments]
        print(("PLAN " if self.dry_run else "RUN ") + shlex.join(arguments), file=sys.stderr)
        if self.dry_run:
            return b""
        if private and stream:
            raise ValueError("Private command output must be captured")
        completed = subprocess.run(arguments, cwd=ROOT, input=data, capture_output=not stream, env=env, timeout=timeout)
        if completed.returncode:
            detail = "credential-bearing output suppressed" if private else (completed.stderr or b"").decode(errors="replace")[-2000:]
            raise RuntimeError(f"Command failed ({completed.returncode}): {detail}")
        return completed.stdout or b""


def kube(config, *arguments):
    return ["kubectl", "--kubeconfig", str(KUBECONFIG), "--context", config["context"], *arguments]


def yaml_module():
    sys.path.insert(0, str(PRIVATE / "python-deps"))
    try:
        import yaml
    except ImportError as error:
        raise RuntimeError("Install demo-assets/m2/requirements.txt or activate /opt/atlas/demo-venv") from error
    return yaml


def save_json(path, value):
    write_private(path, json.dumps(value, indent=2, ensure_ascii=False) + "\n")
