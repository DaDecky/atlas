#!/usr/bin/env python3
"""Build/load local images, apply Kustomize, and bootstrap the measured data map.

No secrets appear in generated manifests, command arguments, or progress output.
Existing env values, PVCs, and placement maps are preserved across deploys.
"""
import argparse
import base64
import concurrent.futures
import csv
import datetime
import io
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import urllib.parse

ROOT = Path(__file__).resolve().parents[1]
NS = "sistem-informasi-kebencanaan-bnpb"
SERVICES = {"bmkg": "services/bmkg", "pvmbg": "services/pvmbg", "auth": "services/bnpb/auth", "client-api": "services/bnpb/client-api", "aggregator": "services/bnpb/aggregator", "consumer-a": "consumers/consumer-a", "consumer-b": "consumers/consumer-b"}


def run(args, *, payload=None, private=False, cwd=ROOT):
    result = subprocess.run(args, cwd=cwd, input=payload, capture_output=True)
    if result.returncode:
        if private:
            raise RuntimeError("Secret operation failed; credential-bearing output suppressed")
        raise RuntimeError(result.stderr.decode(errors="replace")[-4000:])
    return result.stdout


def protect(path):
    if os.name == "nt":
        who = run(["whoami", "/user", "/fo", "csv", "/nh"]).decode()
        sid = list(csv.reader(io.StringIO(who)))[0][1]
        run(["icacls", str(path), "/inheritance:r", "/grant:r", "*" + sid + ":(F)", "*S-1-5-18:(F)", "*S-1-5-32-544:(F)"], private=True)
    else:
        path.chmod(0o600)


def environment(path):
    if not path.exists():
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        from cryptography.hazmat.primitives import serialization
        key = Ed25519PrivateKey.generate()
        private = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
        public = key.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        lines = []
        for line in (ROOT / ".env.example").read_text().splitlines():
            if line and not line.startswith("#") and "=" in line:
                name, value = line.split("=", 1)
                if not value:
                    value = base64.b64encode(private if name == "AUTH_ED25519_PRIVATE_KEY_B64" else public).decode() if name in ["AUTH_ED25519_PRIVATE_KEY_B64", "AUTH_ED25519_PUBLIC_KEY_B64"] else secrets.token_hex(24)
                    if name == "PVMBG_TOKEN":
                        value = "pvmbg.atlas." + value
                line = name + "=" + value
            lines.append(line)
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        protect(path)
        print("Created ignored deployment environment with fresh local credentials", flush=True)
    values = {}
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key] = value
    additions = []
    for name in ["AUDIT_TOKEN", "PANTAI_AUTH_TOKEN"]:
        if not values.get(name):
            values[name] = secrets.token_hex(24)
            additions.append(name + "=" + values[name])
    if additions:
        with path.open("a", encoding="utf-8") as handle:
            handle.write("\n# Local M2 service credentials\n" + "\n".join(additions) + "\n")
    protect(path)
    return values


def main():
    global NS
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--overlay", choices=["dev", "demo"], default="dev")
    parser.add_argument("--kubeconfig")
    parser.add_argument("--context")
    parser.add_argument("--namespace", default=NS, help="isolated namespace for a dev deployment")
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env.m2")
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--load-vms", nargs="+", help="existing SSH aliases for image import")
    parser.add_argument("--kind-cluster", help="import images into an existing kind cluster")
    parser.add_argument("--tag", default="m2-" + datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S"))
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", args.namespace):
        parser.error("namespace must be a Kubernetes DNS label")
    NS = args.namespace
    if not args.tag or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_." for c in args.tag):
        raise ValueError("invalid image tag")
    kube = ["kubectl"]
    if args.kubeconfig:
        kube += ["--kubeconfig", args.kubeconfig]
    if args.context:
        kube += ["--context", args.context]
    run(kube + ["cluster-info"])
    cache = ROOT / ".deploy"
    cache.mkdir(exist_ok=True)
    run(["python", "scripts/build-k8s-manifests.py"])
    values = environment(args.env_file.resolve())
    images = ["atlas/" + name + ":" + args.tag for name in SERVICES]
    if args.build:
        def build(item):
            name, folder = item
            print("Building", name, flush=True)
            result = subprocess.run(["docker", "build", "-t", "atlas/" + name + ":" + args.tag, folder], cwd=ROOT, capture_output=True)
            (cache / ("build-" + name + ".log")).write_bytes(result.stdout + result.stderr)
            if result.returncode:
                raise RuntimeError("image build failed: " + name + "; see ignored build log")
            print("Built", name, flush=True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(build, SERVICES.items()))
    for image in images:
        run(["docker", "image", "inspect", "--format", "{{.Id}}", image])
    if args.load_vms:
        archive = cache / ("images-" + args.tag + ".tar")
        print("Exporting local images", flush=True)
        run(["docker", "image", "save", "-o", str(archive)] + images)
        remote = "/tmp/atlas-images-" + args.tag + ".tar"
        for alias in args.load_vms:
            if not alias or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_." for c in alias):
                raise ValueError("invalid SSH alias")
            print("Loading images on", alias, flush=True)
            run(["scp", "-q", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", str(archive), alias + ":" + remote])
            run(["ssh", "-o", "BatchMode=yes", alias, "sudo k3s ctr -n k8s.io images import " + remote])
            run(["ssh", "-o", "BatchMode=yes", alias, "rm -- " + remote])
    if args.kind_cluster:
        run(["kind", "load", "docker-image", "--name", args.kind_cluster] + images)

    def apply(obj, private=False):
        run(kube + ["apply", "--server-side", "--field-manager=atlas-deploy", "-f", "-"], payload=json.dumps(obj).encode(), private=private)

    apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NS}})

    def secret(name, data):
        # Use data so server-side apply can track/prune keys on later deploys.
        apply({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": name, "namespace": NS}, "type": "Opaque", "data": {key: base64.b64encode(value.encode()).decode() for key, value in data.items()}}, private=True)
        # Earlier stringData deployments did not track removals. Migrate only
        # these known non-secret settings; credential keys remain untouched.
        old_settings = {"auth-credentials": ["ACCESS_TOKEN_TTL"], "client-api-credentials": ["AUTH_BASE_URL", "AGGREGATOR_BASE_URL"], "aggregator-credentials": ["POLL_INTERVAL", "BMKG_BASE_URL", "PVMBG_BASE_URL"]}.get(name, [])
        if old_settings:
            run(kube + ["-n", NS, "patch", "secret", name, "--type=merge", "--patch", json.dumps({"data": {key: None for key in old_settings}})], private=True)

    def config(name, data):
        apply({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": name, "namespace": NS}, "data": data})

    def db(user, password, host, database):
        return "postgres://" + user + ":" + urllib.parse.quote(password, safe="") + "@" + host + ":5432/" + database + "?sslmode=disable"

    def selected(*keys):
        return {key: values[key] for key in keys}

    broker = "amqp://atlas:" + urllib.parse.quote(values["RABBITMQ_PASSWORD"], safe="") + "@rabbitmq:5672/"
    secret("canonical-store-credentials", {"admin-password": values["POSTGRES_ADMIN_PASSWORD"], "aggregator-password": values["CANONICAL_DB_PASSWORD"]})
    for owner in ["auth", "bmkg", "pvmbg"]:
        secret(owner + "-db-credentials", {"POSTGRES_USER": owner, "POSTGRES_DB": owner, "POSTGRES_PASSWORD": values[owner.upper() + "_DB_PASSWORD"]})
    secret("rabbitmq-credentials", {"RABBITMQ_DEFAULT_USER": "atlas", "RABBITMQ_DEFAULT_PASS": values["RABBITMQ_PASSWORD"]})
    secret("bmkg-credentials", {"DATABASE_URL": db("bmkg", values["BMKG_DB_PASSWORD"], "bmkg-db", "bmkg"), "BMKG_API_KEY": values["BMKG_API_KEY"]})
    secret("pvmbg-credentials", {"DATABASE_URL": db("pvmbg", values["PVMBG_DB_PASSWORD"], "pvmbg-db", "pvmbg"), "PVMBG_TOKEN": values["PVMBG_TOKEN"]})
    auth = selected("AUTH_VALIDATE_TOKEN", "AUTH_ED25519_PRIVATE_KEY_B64", "MEDIA_CLIENT_ID", "MEDIA_CLIENT_SECRET", "FIELD_TEAM_CLIENT_ID", "FIELD_TEAM_CLIENT_SECRET", "INTERNAL_OPS_CLIENT_ID", "INTERNAL_OPS_CLIENT_SECRET")
    config("auth-config", selected("ACCESS_TOKEN_TTL"))
    auth["AUTH_DATABASE_URL"] = db("auth", values["AUTH_DB_PASSWORD"], "auth-db", "auth")
    secret("auth-credentials", auth)
    aggregator = selected("BMKG_API_KEY", "PVMBG_TOKEN", "AGGREGATOR_INTERNAL_TOKEN", "AUDIT_TOKEN")
    aggregator.update(DATABASE_URL=db("aggregator", values["CANONICAL_DB_PASSWORD"], "canonical-store-0.canonical-store", "canonical"), RABBITMQ_URL=broker)
    config("aggregator-config", {"POLL_INTERVAL": values["POLL_INTERVAL"], "BMKG_BASE_URL": "http://bmkg:8081", "PVMBG_BASE_URL": "http://pvmbg:8082"})
    if values.get("SATELLITE_TOKEN"):
        aggregator["SATELLITE_TOKEN"] = values["SATELLITE_TOKEN"]
    secret("aggregator-credentials", aggregator)
    client = selected("AUTH_VALIDATE_TOKEN", "AUTH_ED25519_PUBLIC_KEY_B64", "AGGREGATOR_INTERNAL_TOKEN")
    client.update(RABBITMQ_URL=broker)
    config("client-api-config", {"AUTH_BASE_URL": "http://auth:8084", "AGGREGATOR_BASE_URL": "http://aggregator:8083"})
    secret("client-api-credentials", client)
    for name in ["consumer-a", "consumer-b"]:
        secret(name + "-credentials", {"AMQP_URL": broker})
    secret("pantai-token", {"token": values["PANTAI_AUTH_TOKEN"]})
    overlay = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization", "namespace": NS, "resources": ["../deploy/k8s/overlays/" + args.overlay], "images": [{"name": "atlas/" + name, "newTag": args.tag} for name in SERVICES]}
    (cache / "kustomization.yaml").write_text(json.dumps(overlay, indent=2) + "\n", encoding="utf-8")
    print("Applying", args.overlay, "overlay", flush=True)
    print(run(kube + ["apply", "-k", str(cache)]).decode(), flush=True)
    run(kube + ["-n", NS, "rollout", "status", "statefulset/canonical-store", "--timeout=300s"])
    if args.overlay == "demo":
        pods = json.loads(run(kube + ["-n", NS, "get", "pods", "-l", "app=canonical-store", "-o", "json"]))["items"]
        holders = sorted([(pod["metadata"]["name"], pod["spec"]["nodeName"]) for pod in pods])
        if len(holders) != 3 or len({node for _, node in holders}) != 3:
            raise RuntimeError("demo needs one canonical store on each of three nodes")
        existing = subprocess.run(kube + ["-n", NS, "get", "configmap", "canonical-placement", "-o", "json"], capture_output=True)
        if existing.returncode:
            placement = json.loads(run(["go", "run", "./cmd/data-tool", "-mode", "placement", "-nodes", ",".join(node for _, node in holders)], cwd=ROOT / SERVICES["aggregator"]))
            placement["metadata"]["namespace"] = NS
            apply(placement)
        else:
            table = json.loads(json.loads(existing.stdout)["data"]["placement.json"])
            if set(table["nodes"]) != {node for _, node in holders}:
                raise RuntimeError("existing placement differs from measured holders; use the data recovery workflow")
        config = {"nodes": [{"name": node, "url": db("aggregator", values["CANONICAL_DB_PASSWORD"], pod + ".canonical-store", "canonical")} for pod, node in holders], "placement": {"Origin": "https://kubernetes.default.svc", "Namespace": NS, "Name": "canonical-placement", "TokenFile": "/var/run/secrets/kubernetes.io/serviceaccount/token", "CAFile": "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"}}
        secret("canonical-data-config", {"config.json": json.dumps(config)})
    for name in ["auth-db", "bmkg-db", "pvmbg-db", "rabbitmq"]:
        run(kube + ["-n", NS, "rollout", "status", "statefulset/" + name, "--timeout=300s"])
    for name in SERVICES:
        run(kube + ["-n", NS, "rollout", "status", "deployment/" + name, "--timeout=300s"])
    run(kube + ["-n", NS, "rollout", "status", "deployment/sensor-pantai", "--timeout=300s"])
    (cache / "last-deploy.json").write_text(json.dumps({"tag": args.tag, "overlay": args.overlay, "namespace": NS}, indent=2) + "\n", encoding="utf-8")
    print("Deployment ready:", args.overlay, "tag", args.tag, flush=True)
    if args.overlay == "demo":
        print("Three replicated data stores and read replicas active; source ingestion awaits source-leadership integration.", flush=True)


if __name__ == "__main__":
    main()
