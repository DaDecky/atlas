"""Run one explicit Atlas demo phase. Setup/deploy never starts the satellite exam."""

import argparse
import base64
import datetime
import getpass
import hashlib
import json
import os
from pathlib import Path
import re
import runpy
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

from ops_common import ASSETS, KUBECONFIG, PRIVATE, ROOT, Runner, kube, load_config, read_environment, save_json, utc_now, write_private

SATELLITE = "https://satellite.naufarrel.dev"


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Demo:
    def __init__(self, config, runner, run_id):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,100}", run_id):
            raise ValueError("Invalid run ID; use a short name without directory separators")
        self.config, self.runner, self.run_id = config, runner, run_id
        self.evidence = PRIVATE / "evidence" / run_id
        self.values = read_environment() if (ROOT / ".env.m2").exists() else {}

    def kubectl(self, *arguments, **kwargs):
        return self.runner.run(kube(self.config, *arguments), **kwargs)

    def required(self, name):
        value = self.values.get(name)
        if not value and not self.runner.dry_run:
            raise ValueError("Missing local environment value: " + name)
        return value or "dry-run-value"

    def request(self, origin, path, *, token=None, payload=None, method=None):
        method = method or ("POST" if payload is not None else "GET")
        if self.runner.dry_run:
            print("PLAN HTTP", method, origin + path, "(credentials omitted)", file=sys.stderr)
            return {"dry_run": True}
        headers = {"Content-Type": "application/json", "X-Correlation-ID": "atlas-" + self.run_id}
        if token:
            headers["Authorization"] = "Bearer " + token
        request = urllib.request.Request(origin.rstrip("/") + path, method=method, headers=headers,
                                         data=json.dumps(payload).encode() if payload is not None else None)
        started = time.monotonic()
        try:
            with urllib.request.build_opener(NoRedirect).open(request, timeout=60) as response:
                result = json.load(response)
                print(f"HTTP {response.status} time_total={time.monotonic() - started:.3f}s", file=sys.stderr)
                return result
        except urllib.error.HTTPError as error:
            raise RuntimeError(f"HTTP {error.code} at {path}; response body omitted") from None

    def login(self, prefix):
        result = self.request(self.config["auth_url"], "/auth/login", payload={
            "client_id": self.required(prefix + "_CLIENT_ID"), "client_secret": self.required(prefix + "_CLIENT_SECRET")})
        return "dry-run-client-token" if self.runner.dry_run else result["access_token"]

    def emit(self, name, value):
        if not self.runner.dry_run:
            save_json(self.evidence / name, value)
        print(json.dumps(value, indent=2, ensure_ascii=False))

    def apply(self, value, private=False):
        return self.kubectl("apply", "-f", "-", data=json.dumps(value).encode(), private=private)

    def configure_exam(self):
        ns = self.config["namespace"]
        self.apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "tester-agent"}})
        for filename, name, namespace in [("pantai.key", "pantai-key", ns), ("tester.key", "tester-key", "tester-agent")]:
            if self.runner.dry_run:
                raw = b"dry-run-placeholder"
            else:
                raw = (PRIVATE / filename).read_bytes()
                if not raw.startswith(b"01-Atlas.") or len(raw.splitlines()) != 1:
                    raise ValueError("Invalid exam key format: " + filename)
            self.apply({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": name, "namespace": namespace},
                        "type": "Opaque", "data": {"key": base64.b64encode(raw).decode()}}, private=True)
        self.kubectl("-n", ns, "patch", "deployment", "sensor-pantai", "--type=strategic", "--patch-file", ASSETS / "sensor-exam.patch.json")
        self.kubectl("apply", "-f", ASSETS / "tester-manifest.yaml")
        # Refresh envFrom values even when image tags have not changed.
        self.kubectl("-n", ns, "rollout", "restart", "deployment/aggregator")
        self.kubectl("-n", ns, "rollout", "status", "deployment/aggregator", "--timeout=5m")
        self.kubectl("-n", ns, "rollout", "status", "deployment/sensor-pantai", "--timeout=5m")
        self.kubectl("-n", "tester-agent", "rollout", "status", "daemonset/tester-agent", "--timeout=5m")

    def deploy(self):
        for key in ["SATELLITE_TOKEN", "AUDIT_TOKEN", "PANTAI_AUTH_TOKEN"]:
            self.required(key)
        if not self.runner.dry_run:
            for filename in ["pantai.key", "tester.key"]:
                if not (PRIVATE / filename).is_file():
                    raise ValueError("Prepare local exam key first: " + filename)
                raw = (PRIVATE / filename).read_bytes()
                if not raw.startswith(b"01-Atlas.") or len(raw.splitlines()) != 1:
                    raise ValueError("Invalid exam key format: " + filename)
        from cluster import Cluster
        Cluster(self.config, self.runner).verify()
        command = [sys.executable, str(ROOT / "scripts/deploy-k8s.py"), "--build", "--overlay", "demo",
                   "--enable-ingestion", "--enable-telemetry", "--kubeconfig", str(KUBECONFIG), "--context", self.config["context"],
                   "--tag", self.config["image_tag"], "--load-vms", *[node["ssh"] for node in self.config["nodes"]]]
        environment = os.environ.copy()
        environment["PATH"] = str(Path(sys.executable).parent) + os.pathsep + environment.get("PATH", "")
        self.runner.run(command, env=environment, timeout=3600, stream=True)
        self.configure_exam()

    def start_satellite(self):
        token = self.required("SATELLITE_TOKEN")
        state_file = PRIVATE / "satellite-start-state.json"
        fingerprint = hashlib.sha256(token.encode()).hexdigest()
        if not self.runner.dry_run and state_file.exists():
            state = json.loads(state_file.read_text())
            if state.get("token_fingerprint") == fingerprint:
                raise ValueError("This token already has a local start receipt; no second start was sent")
        result = self.request(SATELLITE, "/v1/exam/start", token=token, method="POST")
        if not self.runner.dry_run:
            save_json(state_file, {"token_fingerprint": fingerprint, "run_id": self.run_id, "started_at": utc_now()})
        self.emit("satellite-exam-start.json", result)

    def exam(self, problem, since=None):
        if problem == "p5":
            if not since:
                page = self.request(self.config["sensor_url"], "/v1/observations?limit=1", token=self.required("PANTAI_AUTH_TOKEN"))
                since = "dry-run-sensor-time" if self.runner.dry_run else page.get("generated_at")
            if not since:
                raise ValueError("Cannot determine sensor observation time; provide --since RFC3339 explicitly")
            if not self.runner.dry_run:
                if datetime.datetime.fromisoformat(since.replace("Z", "+00:00")).tzinfo is None:
                    raise ValueError("P5 --since must include its timezone")
                save_json(self.evidence / "p5-window.json", {"since": since, "started_at_wall_clock": utc_now(), "clock": "sensor generated_at or explicit --since"})
        result = self.kubectl("exec", "-n", "tester-agent", "daemonset/tester-agent", "--", "/app", "run", "exam", problem)
        if not self.runner.dry_run:
            write_private(self.evidence / (problem + "-exam.txt"), result)
            print(result.decode(errors="replace"))

    def ports(self):
        processes = []
        forwards = [("client-api", "8080:8080"), ("auth", "8084:8084"), ("aggregator", "8083:8083"),
                    ("sensor-pantai", "18080:8080"), ("victoriametrics", "8428:8428"), ("jaeger", "16686:16686")]
        try:
            for service, mapping in forwards:
                command = kube(self.config, "-n", self.config["namespace"], "port-forward", "svc/" + service, mapping, "--address=127.0.0.1")
                if self.runner.dry_run:
                    self.runner.run(command)
                else:
                    processes.append(subprocess.Popen(command, cwd=ROOT))
            if not self.runner.dry_run:
                print("Port forwards running on localhost; Ctrl+C stops all six.")
                while all(process.poll() is None for process in processes):
                    time.sleep(1)
                raise RuntimeError("A port forward exited; inspect its output and restart ports")
        finally:
            for process in processes:
                process.terminate()
            for process in processes:
                process.wait(timeout=10)

    def capture(self):
        ns = self.config["namespace"]
        commands = {
            "notifier": kube(self.config, "-n", ns, "logs", "-f", "deploy/notifier", "--since=1h"),
            "tester": kube(self.config, "-n", "tester-agent", "logs", "-f", "-l", "app=tester-agent", "--prefix=true", "--max-log-requests=10", "--since=5m"),
            "aggregator": kube(self.config, "-n", ns, "logs", "-f", "-l", "app=aggregator", "--prefix=true", "--max-log-requests=10", "--since=5m"),
        }
        if self.runner.dry_run:
            for command in commands.values():
                self.runner.run(command)
            return
        self.evidence.mkdir(parents=True, exist_ok=True)
        save_json(self.evidence / "capture-window.json", {"started_at": utc_now()})
        stop, lock, processes = threading.Event(), threading.Lock(), []

        def follow(label, command):
            while not stop.is_set():
                with lock:
                    if stop.is_set():
                        return
                    process = subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    processes.append(process)
                with (self.evidence / (label + ".log")).open("ab") as output:
                    for line in process.stdout:
                        output.write(line)
                        output.flush()
                        if label == "tester":
                            print(line.decode(errors="replace").rstrip(), flush=True)
                process.wait()
                stop.wait(1)

        threads = [threading.Thread(target=follow, args=item, daemon=True) for item in commands.items()]
        for thread in threads:
            thread.start()
        try:
            print("Capturing notifier/tester/aggregator; Tester Agent logs remain visible. Ctrl+C stops capture.")
            while True:
                time.sleep(1)
        finally:
            stop.set()
            with lock:
                for process in processes:
                    if process.poll() is None:
                        process.terminate()
            for thread in threads:
                thread.join(timeout=10)
            save_json(self.evidence / "capture-ended.json", {"finished_at": utc_now()})

    def runner_services(self):
        for name, selector, target, node_port in [("client-api-k6", "client-api", 8080, 32562), ("auth-k6", "auth", 8084, 31561)]:
            self.apply({"apiVersion": "v1", "kind": "Service", "metadata": {"name": name, "namespace": self.config["namespace"], "labels": {"purpose": "atlas-demo-load"}},
                        "spec": {"type": "NodePort", "selector": {"app": selector}, "ports": [{"port": target, "targetPort": target, "nodePort": node_port}]}})
        print("Runner endpoints:", self.config["runner_client_url"], self.config["runner_auth_url"])

    def load(self, args):
        environment = os.environ.copy()
        for name in ["MEDIA_CLIENT_ID", "MEDIA_CLIENT_SECRET", "FIELD_TEAM_CLIENT_ID", "FIELD_TEAM_CLIENT_SECRET"]:
            environment[name] = self.required(name)
        environment.update(BASE_URL=self.config["runner_client_url"], AUTH_URL=self.config["runner_auth_url"],
                           SCENARIO=args.scenario, DURATION=args.duration, THINK_TIME="0",
                           SUMMARY_JSON=str(self.evidence / ("load-" + args.scenario + ".json")))
        if args.vus is not None:
            environment["VUS"] = str(args.vus)
        if args.rate is not None:
            environment["RATE"] = str(args.rate)
        if not self.runner.dry_run:
            self.evidence.mkdir(parents=True, exist_ok=True)
        self.runner.run(["k6", "run", str(ROOT / "loadtest/m2.js")], env=environment, stream=True, timeout=3600)

    def field_report(self, repeat):
        saved = self.evidence / "field-report-payload.json"
        if repeat:
            if self.runner.dry_run:
                payload = {"report_id": "dry-run-repeat"}
            else:
                payload = json.loads(saved.read_text())
        else:
            payload = json.loads((ASSETS / "p3/field-report.json").read_text())
            payload["observed_at"] = utc_now()
            payload["report_id"] = "atlas-" + self.run_id + "-" + datetime.datetime.now(datetime.timezone.utc).strftime("%H%M%S%f")
            if not self.runner.dry_run:
                save_json(saved, payload)
        self.emit("field-report-response.json", self.request(self.config["client_url"], "/field-reports", token=self.login("FIELD_TEAM"), payload=payload))

    def prepare_rollout(self, tag):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", tag):
            raise ValueError("Invalid replacement image tag")
        image = "atlas/aggregator:" + tag
        archive = PRIVATE / ("aggregator-" + tag + ".tar")
        if not self.runner.dry_run:
            archive.parent.mkdir(parents=True, exist_ok=True)
        self.runner.run(["docker", "build", "-t", image, "-f", str(ROOT / "services/bnpb/aggregator/Dockerfile"), str(ROOT / "services/bnpb")], stream=True, timeout=3600)
        self.runner.run(["docker", "image", "save", "-o", str(archive), image])
        for node in self.config["nodes"]:
            remote = "/tmp/atlas-aggregator-" + tag + ".tar"
            self.runner.run(["scp", "-q", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", str(archive), node["ssh"] + ":" + remote])
            self.runner.run(["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", node["ssh"], "sudo k3s ctr -n k8s.io images import " + remote])
            self.runner.run(["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", node["ssh"], "rm -f -- " + remote])


def parser():
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--config", type=Path)
    value.add_argument("--run-id", default="demo")
    value.add_argument("--dry-run", action="store_true")
    commands = value.add_subparsers(dest="command", required=True)
    command = commands.add_parser("init-env")
    command.add_argument("--interactive", action="store_true", help="read token and exam keys with hidden prompts")
    command = commands.add_parser("status")
    command.add_argument("--live", action="store_true")
    for name in ["deploy", "configure-exam", "ports", "capture", "runner-services", "p3-list", "p3-replay", "export-satellite", "error-budget", "scale-sequence"]:
        commands.add_parser(name)
    command = commands.add_parser("start-satellite")
    command.add_argument("--recording-started", action="store_true", required=True)
    command = commands.add_parser("exam")
    command.add_argument("problem", choices=["p1", "p2", "p3", "p5"])
    command.add_argument("--since")
    command = commands.add_parser("audit")
    command.add_argument("--source", choices=["BMKG", "PVMBG", "SATELIT", "SENSOR_PANTAI"], default="BMKG")
    command = commands.add_parser("p3-policy")
    command.add_argument("mode", choices=["before", "after"])
    command = commands.add_parser("field-report")
    command.add_argument("--repeat", action="store_true", help="resend the exact payload saved for this run ID")
    command = commands.add_parser("load")
    command.add_argument("scenario", choices=["sustained", "throughput", "rollout", "overload", "latency"])
    command.add_argument("--duration", default="150s")
    command.add_argument("--vus", type=int)
    command.add_argument("--rate", type=int)
    command = commands.add_parser("scale")
    command.add_argument("replicas", type=int, choices=range(1, 6))
    command = commands.add_parser("rollout")
    command.add_argument("tag")
    command = commands.add_parser("prepare-rollout")
    command.add_argument("tag")
    command = commands.add_parser("submit-video")
    command.add_argument("url")
    return value


def main():
    args = parser().parse_args()
    config = load_config(args.config)
    demo = Demo(config, Runner(args.dry_run), args.run_id)
    ns = config["namespace"]
    if args.command == "init-env":
        if args.dry_run:
            print("PLAN create/preserve .env.m2 and optionally read exam keys interactively")
            return
        helpers = runpy.run_path(str(ROOT / "scripts/deploy-k8s.py"), run_name="atlas_demo_environment")
        helpers["environment"](ROOT / ".env.m2")
        if args.interactive:
            token = getpass.getpass("Satellite demo token (Enter preserves existing): ")
            if token:
                text = (ROOT / ".env.m2").read_text(encoding="utf-8-sig")
                lines = [line for line in text.splitlines() if not line.startswith("SATELLITE_TOKEN=")]
                write_private(ROOT / ".env.m2", "\n".join([*lines, "SATELLITE_TOKEN=" + token, ""]))
            for filename in ["pantai.key", "tester.key"]:
                key = getpass.getpass(filename + " from assistant email (Enter preserves existing): ")
                if key:
                    if not re.fullmatch(r"01-Atlas\.[A-Za-z0-9_-]+", key):
                        raise ValueError("Invalid exam key format")
                    write_private(PRIVATE / filename, key + "\n")
        print("Local environment ready. Status reports missing exam material without printing credentials.")
    elif args.command == "status":
        status = {"context": config["context"], "kubeconfig_present": KUBECONFIG.exists(),
                  "environment_present": (ROOT / ".env.m2").exists(), "exam_keys_present": all((PRIVATE / name).exists() for name in ["pantai.key", "tester.key"]),
                  "satellite_token_present": bool(demo.values.get("SATELLITE_TOKEN")),
                  "official_p1_dataset_present": (PRIVATE / "seismic_events.jsonl.gz").exists(),
                  "satellite_start_receipt_present": (PRIVATE / "satellite-start-state.json").exists(), "live_state": "not checked"}
        if args.live:
            status["pods"] = json.loads(demo.kubectl("-n", ns, "get", "pods", "-o", "json")) if not args.dry_run else {"dry_run": True}
            status["live_state"] = "queried" if not args.dry_run else "dry-run"
        print(json.dumps(status, indent=2))
    elif args.command == "deploy": demo.deploy()
    elif args.command == "configure-exam": demo.configure_exam()
    elif args.command == "ports": demo.ports()
    elif args.command == "capture": demo.capture()
    elif args.command == "runner-services": demo.runner_services()
    elif args.command == "start-satellite": demo.start_satellite()
    elif args.command == "exam": demo.exam(args.problem, args.since)
    elif args.command == "audit":
        demo.emit("audit-" + args.source + ".json", demo.request(config["audit_url"], "/internal/audit/checksum?source=" + args.source, token=demo.required("AUDIT_TOKEN")))
    elif args.command == "p3-list":
        demo.emit("p3-unprocessed.json", demo.request(config["client_url"], "/internal-ops/unprocessed?source=PVMBG&limit=10&include_raw=false", token=demo.login("INTERNAL_OPS")))
    elif args.command == "p3-replay":
        demo.emit("p3-replay.json", demo.request(config["client_url"], "/internal-ops/replay", token=demo.login("INTERNAL_OPS"), payload=json.loads((ASSETS / "p3/replay.json").read_text())))
    elif args.command == "p3-policy":
        policy = json.loads((ASSETS / ("p3/policy-" + args.mode + ".json")).read_text())
        demo.apply({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "source-schema-policy", "namespace": ns}, "data": {"policy.json": json.dumps(policy)}})
        print("Policy applied. Wait for the projected file to update before replay; this command does not replay automatically.")
    elif args.command == "field-report": demo.field_report(args.repeat)
    elif args.command == "prepare-rollout": demo.prepare_rollout(args.tag)
    elif args.command == "export-satellite":
        environment = os.environ.copy()
        environment.update(AUDIT_TOKEN=demo.required("AUDIT_TOKEN"), AUDIT_URL=config["audit_url"])
        demo.runner.run([sys.executable, str(ASSETS / "export-audit.py"), "--source", "SATELIT", "--output", str(demo.evidence / "p4-satellite.json")], env=environment, stream=True)
    elif args.command == "error-budget":
        if args.dry_run:
            print("PLAN read p5-window.json/notifier.log and run scripts/error-budget.py")
            return
        state = json.loads((demo.evidence / "p5-window.json").read_text())
        environment = os.environ.copy()
        environment["PANTAI_AUTH_TOKEN"] = demo.required("PANTAI_AUTH_TOKEN")
        result = demo.runner.run([sys.executable, str(ROOT / "scripts/error-budget.py"), "--sensor", config["sensor_url"], "--since", state["since"]],
                                 data=(demo.evidence / "notifier.log").read_bytes(), env=environment)
        write_private(demo.evidence / "p5-error-budget.txt", result)
        print(result.decode(errors="replace"))
    elif args.command == "load":
        if not re.fullmatch(r"[1-9]\d*(s|m|h)", args.duration) or args.vus is not None and not 1 <= args.vus <= 10000 or args.rate is not None and args.rate < 1:
            raise ValueError("Use a positive duration, 1..10000 VUs, and a positive rate")
        demo.load(args)
    elif args.command == "scale":
        demo.kubectl("-n", ns, "scale", "deployment/aggregator", "--replicas=" + str(args.replicas))
        demo.kubectl("-n", ns, "rollout", "status", "deployment/aggregator", "--timeout=5m")
    elif args.command in ("scale-sequence", "rollout"):
        environment = os.environ.copy()
        environment["KUBECONFIG"] = str(KUBECONFIG)
        command = ["bash", str(ROOT / "loadtest/scale.sh"), "scale"]
        if args.command == "rollout":
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", args.tag):
                raise ValueError("Invalid replacement image tag")
            command = ["bash", str(ROOT / "loadtest/scale.sh"), "rollout", args.tag]
        demo.runner.run(command, env=environment, stream=True)
    elif args.command == "submit-video":
        from urllib.parse import urlparse
        url = urlparse(args.url)
        if url.scheme != "https" or url.hostname not in ("www.youtube.com", "youtube.com") or url.username or url.password:
            raise ValueError("Use the final HTTPS YouTube URL without a link shortener")
        demo.emit("video-submission.json", demo.request(SATELLITE, "/v1/video", token=demo.required("SATELLITE_TOKEN"), payload={"url": args.url}))
        demo.emit("video-submission-check.json", demo.request(SATELLITE, "/v1/video", token=demo.required("SATELLITE_TOKEN")))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopped foreground demo command.", file=sys.stderr)
    except (ValueError, RuntimeError, OSError, KeyError, subprocess.TimeoutExpired) as error:
        print("ERROR:", str(error), file=sys.stderr)
        raise SystemExit(1)
