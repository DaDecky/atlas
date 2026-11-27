"""Command planning and isolation checks; no VM, cluster or exam service is accessed."""

import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ASSETS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ASSETS))
import cluster
import demo
import ops_common


class RecordingRunner:
    def __init__(self, dry_run=True):
        self.dry_run, self.calls = dry_run, []

    def run(self, arguments, **kwargs):
        self.calls.append(([str(item) for item in arguments], kwargs))
        if "node-token" in str(arguments):
            return b"synthetic-join-token\n"
        return b""


class OperationTests(unittest.TestCase):
    def setUp(self):
        self.config = ops_common.load_config(ASSETS / "cluster.example.json")

    def make_demo(self, runner=None):
        with patch.object(demo, "read_environment", return_value={}):
            instance = demo.Demo(self.config, runner or RecordingRunner(), "unit-run")
        instance.values = {"SATELLITE_TOKEN": "synthetic-test-token"}
        return instance

    def test_topology_validation_rejects_bad_names_ips_roles_aliases(self):
        mutations = [
            lambda c: c["nodes"][1].update(name="atlas-node-a"),
            lambda c: c["nodes"][1].update(ip="192.168.1.10"),
            lambda c: c["nodes"][1].update(role="server"),
            lambda c: c["nodes"][1].update(ssh="host; dangerous"),
            lambda c: c.update(k3s_version="latest; echo bad"),
        ]
        for mutate in mutations:
            value = copy.deepcopy(self.config)
            mutate(value)
            with self.assertRaises(ValueError):
                ops_common.validate_config(value)

    def test_run_id_cannot_escape_private_folder(self):
        with self.assertRaises(ValueError):
            demo.Demo(self.config, RecordingRunner(), "../../outside")

    def test_kubectl_uses_isolated_kubeconfig_and_context(self):
        arguments = ops_common.kube(self.config, "get", "nodes")
        self.assertIn(str(ops_common.KUBECONFIG), arguments)
        self.assertIn("atlas-demo", arguments)

    def test_join_token_is_only_transferred_over_stdin(self):
        runner = RecordingRunner(dry_run=False)
        with tempfile.TemporaryDirectory() as folder, patch.object(cluster, "PRIVATE", Path(folder)):
            def write(path, content):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
            instance = cluster.Cluster(self.config, runner)
            with patch.object(cluster, "write_private", side_effect=write), patch.object(instance, "fetch_kubeconfig"), patch.object(instance, "verify", return_value={"ok": True}):
                instance.bootstrap()
        transfers = [(args, kwargs) for args, kwargs in runner.calls if kwargs.get("data") == b"synthetic-join-token\n"]
        self.assertEqual(len(transfers), 2)
        for arguments, kwargs in runner.calls:
            self.assertNotIn("synthetic-join-token", " ".join(arguments))
        self.assertTrue(all(kwargs.get("private") for _, kwargs in transfers))

    def test_deploy_does_not_start_exam_or_call_satellite(self):
        instance = self.make_demo()
        with patch.object(instance, "request") as http, patch.object(instance, "configure_exam") as configure:
            instance.deploy()
        http.assert_not_called()
        configure.assert_called_once()
        self.assertTrue(any("deploy-k8s.py" in str(args) for args, _ in instance.runner.calls))

    def test_start_requires_recording_flag_in_cli(self):
        with self.assertRaises(SystemExit):
            demo.parser().parse_args(["start-satellite"])

    def test_satellite_start_is_post_without_body_and_locally_once(self):
        runner = RecordingRunner(dry_run=False)
        with tempfile.TemporaryDirectory() as folder, patch.object(demo, "PRIVATE", Path(folder)):
            instance = self.make_demo(runner)
            def save(path, value):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(value))
            with patch.object(demo, "save_json", side_effect=save), patch.object(instance, "emit"), patch.object(instance, "request", return_value={"ok": True}) as http:
                instance.start_satellite()
                self.assertEqual(http.call_args.kwargs["method"], "POST")
                self.assertNotIn("payload", http.call_args.kwargs)
                with self.assertRaises(ValueError):
                    instance.start_satellite()
                self.assertEqual(http.call_count, 1)

    def test_secret_payload_is_stdin_and_output_is_private(self):
        instance = self.make_demo()
        instance.configure_exam()
        secrets = [(args, kwargs) for args, kwargs in instance.runner.calls if kwargs.get("private")]
        self.assertEqual(len(secrets), 2)
        for arguments, kwargs in secrets:
            value = json.loads(kwargs["data"])
            self.assertEqual(value["kind"], "Secret")
            self.assertTrue(arguments[-2:] == ["-f", "-"])

    def test_p3_listing_obeys_internal_ops_page_limit(self):
        self.assertIn("limit=10", (ASSETS / "p3/requests.http").read_text())
        self.assertEqual(json.loads((ASSETS / "p3/replay.json").read_text())["limit"], 100)

    def test_os_installers_only_plan_in_dry_run(self):
        bash = Path(r"C:\Program Files\Git\bin\bash.exe") if sys.platform == "win32" else Path("/bin/bash")
        for role in ["node", "operator", "runner"]:
            run = subprocess.run([str(bash), str(ASSETS / "bootstrap/setup-os.sh"), "--role", role, "--dry-run"], capture_output=True, text=True)
            self.assertEqual(run.returncode, 0, run.stderr)
            self.assertIn("No command was installed", run.stdout)
        run = subprocess.run([str(bash), str(ASSETS / "bootstrap/bootstrap-node.sh"), "--role", "server", "--name", "atlas-node-a", "--ip", "10.0.0.40", "--server-ip", "10.0.0.40", "--version", "v1.37.2+k3s1", "--dry-run"], capture_output=True, text=True)
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertIn("Plan:", run.stdout)


if __name__ == "__main__":
    unittest.main()
