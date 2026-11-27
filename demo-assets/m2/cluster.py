"""Bootstrap the three Atlas VMs over SSH, or inspect the planned commands offline."""

import argparse
import json
from pathlib import Path
import shlex
import sys
import uuid

from ops_common import ASSETS, EXPECTED_NODES, KUBECONFIG, PRIVATE, Runner, kube, load_config, save_json, utc_now, write_private, yaml_module


class Cluster:
    def __init__(self, config, runner):
        self.config, self.runner = config, runner

    def ssh(self, node, arguments, **kwargs):
        return self.runner.run(["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", node["ssh"], shlex.join(arguments)], **kwargs)

    def bootstrap(self):
        nodes = sorted(self.config["nodes"], key=lambda item: item["role"] != "server")
        server = nodes[0]
        for node in nodes:
            self.ssh(node, ["sudo", "-n", "true"])
        for node in nodes:
            remote = "/tmp/atlas-bootstrap-" + uuid.uuid4().hex
            self.ssh(node, ["mkdir", "-m", "700", remote])
            self.runner.run(["scp", "-q", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                             str(ASSETS / "bootstrap/setup-os.sh"), str(ASSETS / "bootstrap/bootstrap-node.sh"), node["ssh"] + ":" + remote + "/"])
            arguments = ["sudo", "-n", "bash", remote + "/bootstrap-node.sh", "--role", node["role"],
                         "--name", node["name"], "--ip", node["ip"], "--server-ip", server["ip"],
                         "--version", self.config["k3s_version"], "--node-network", self.config["node_network"],
                         "--pod-cidr", self.config["pod_cidr"], "--service-cidr", self.config["service_cidr"]]
            if node["role"] == "agent":
                self.ssh(node, ["sudo", "-n", "install", "-d", "-m", "0700", "/etc/rancher/k3s"])
                token = b"" if self.runner.dry_run else (PRIVATE / "cluster/join.token").read_bytes()
                self.ssh(node, ["sudo", "-n", "sh", "-c", "umask 077; cat > /etc/rancher/k3s/atlas-join.token"], data=token, private=True)
                arguments.extend(["--token-file", "/etc/rancher/k3s/atlas-join.token"])
            self.ssh(node, arguments, timeout=1800)
            if node["role"] == "server":
                token = self.ssh(node, ["sudo", "-n", "cat", "/var/lib/rancher/k3s/server/node-token"], private=True)
                if not self.runner.dry_run:
                    if not token.strip():
                        raise ValueError("K3s server returned an empty join token")
                    write_private(PRIVATE / "cluster/join.token", token)
        self.fetch_kubeconfig()
        return self.verify()

    def fetch_kubeconfig(self):
        server = next(node for node in self.config["nodes"] if node["role"] == "server")
        raw = self.ssh(server, ["sudo", "-n", "cat", "/etc/rancher/k3s/k3s.yaml"], private=True)
        if self.runner.dry_run:
            return
        yaml = yaml_module()
        value = yaml.safe_load(raw)
        if len(value.get("clusters", [])) != 1 or len(value.get("users", [])) != 1 or len(value.get("contexts", [])) != 1:
            raise ValueError("Expected a single-cluster server kubeconfig")
        name = self.config["context"]
        value["clusters"][0]["name"] = name
        value["clusters"][0]["cluster"]["server"] = "https://" + server["ip"] + ":6443"
        value["users"][0]["name"] = name
        value["contexts"][0].update(name=name, context={"cluster": name, "user": name})
        value["current-context"] = name
        write_private(KUBECONFIG, yaml.safe_dump(value))

    def verify(self):
        self.runner.run(kube(self.config, "wait", "--for=condition=Ready", *["node/" + name for name in EXPECTED_NODES], "--timeout=300s"))
        raw = self.runner.run(kube(self.config, "get", "nodes", "-o", "json"))
        if self.runner.dry_run:
            return {"dry_run": True, "cluster_contacted": False}
        nodes = json.loads(raw)["items"]
        if {node["metadata"]["name"] for node in nodes} != set(EXPECTED_NODES):
            raise ValueError("Cluster nodes do not match the three-node Atlas placement")
        boots = set()
        for node in nodes:
            name = node["metadata"]["name"]
            if not any(address["type"] == "InternalIP" and address["address"] == EXPECTED_NODES[name] for address in node["status"]["addresses"]):
                raise ValueError("Unexpected InternalIP for " + name)
            if node["status"]["nodeInfo"]["kubeletVersion"] != self.config["k3s_version"]:
                raise ValueError("K3s version mismatch for " + name)
            boots.add(node["status"]["nodeInfo"]["bootID"])
        if len(boots) != 3 or "" in boots:
            raise ValueError("The three nodes must have distinct kernel boot IDs")
        result = {"verified_at": utc_now(), "nodes_ready": 3, "distinct_kernels": 3, "k3s_version": self.config["k3s_version"]}
        save_json(PRIVATE / "cluster/verified.json", result)
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("command", choices=["init-config", "plan", "bootstrap", "kubeconfig", "verify"])
    args = parser.parse_args()
    if args.command == "init-config":
        destination = args.config or PRIVATE / "cluster.json"
        if destination.exists():
            print("Config exists; preserved:", destination)
        elif args.dry_run:
            print("PLAN create local config:", destination)
        else:
            write_private(destination, (ASSETS / "cluster.example.json").read_bytes())
            print("Created local config:", destination)
        return
    config = load_config(args.config)
    if args.command == "plan":
        print(json.dumps({"nodes": config["nodes"], "k3s_version": config["k3s_version"], "kubeconfig": str(KUBECONFIG),
                          "prerequisites": ["Ubuntu 24.04 amd64 VMs with the configured LAN IPs", "SSH host keys verified and aliases configured", "noninteractive sudo", "internet access to official package/image repositories"]}, indent=2))
        return
    cluster = Cluster(config, Runner(args.dry_run))
    if args.command == "bootstrap":
        print(json.dumps(cluster.bootstrap(), indent=2))
    elif args.command == "kubeconfig":
        cluster.fetch_kubeconfig()
    else:
        print(json.dumps(cluster.verify(), indent=2))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, RuntimeError, OSError) as error:
        print("ERROR:", str(error), file=sys.stderr)
        raise SystemExit(1)
