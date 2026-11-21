#!/usr/bin/env python3
"""Generate the inspectable Kustomize bases using only Python's JSON library.

JSON is a YAML subset accepted by kubectl and Kustomize. Vendor manifests and
the data owner's deploy/store base are referenced, never rewritten here.
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "deploy/k8s"
NAMESPACE = "sistem-informasi-kebencanaan-bnpb"


def write(path, value):
    path = ROOT / path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def resource(kind, name, **body):
    version = "apps/v1" if kind in ["Deployment", "StatefulSet"] else "networking.k8s.io/v1" if kind == "NetworkPolicy" else "v1"
    return dict(apiVersion=version, kind=kind, metadata={"name": name}, **body)


def memory(request, limit):
    return {"requests": {"memory": request}, "limits": {"memory": limit}}


def service(name, port, headless=False):
    spec = {"selector": {"app": name}, "ports": [{"name": "tcp", "port": port, "targetPort": port}]}
    if headless:
        spec["clusterIP"] = "None"
    return resource("Service", name, spec=spec)


def workload(name, role, port, sizes, *, stateful=False, health="/health", owner=None, volume=False, image=None, env=None):
    labels = {"app": name, "role": role}
    if owner:
        labels["owner"] = owner
    container = {"name": name, "image": image or "atlas/" + name + ":m2-local", "imagePullPolicy": "IfNotPresent" if image else "Never", "ports": [{"containerPort": port, "name": "tcp"}], "resources": memory(*sizes)}
    if role == "store":
        probe = {"exec": {"command": ["sh", "-c", "pg_isready -U \"$POSTGRES_USER\" -d \"$POSTGRES_DB\""]}}
    elif name == "rabbitmq":
        probe = {"tcpSocket": {"port": port}}
    else:
        probe = {"httpGet": {"path": health, "port": port}}
    container["readinessProbe"] = dict(probe, periodSeconds=3, timeoutSeconds=2, failureThreshold=2)
    container["livenessProbe"] = {"tcpSocket": {"port": port}, "periodSeconds": 10, "timeoutSeconds": 2, "initialDelaySeconds": 30}
    container["startupProbe"] = dict(probe, periodSeconds=2, timeoutSeconds=2, failureThreshold=60)
    if env:
        container["env"] = [{"name": key, "value": value} for key, value in env.items()]
    container["envFrom"] = [{"secretRef": {"name": name + "-credentials"}}]
    if name in ["auth", "client-api", "aggregator"]:
        container["envFrom"].append({"configMapRef": {"name": name + "-config"}})
    pod = {"containers": [container]}
    dependencies = {"auth": [("auth-db", 5432)], "bmkg": [("bmkg-db", 5432)], "pvmbg": [("pvmbg-db", 5432)], "aggregator": [("canonical-store-0.canonical-store", 5432), ("rabbitmq", 5672)]}
    if name in dependencies:
        wait = " && ".join("until nc -z -w 2 " + host + " " + str(target) + "; do sleep 1; done" for host, target in dependencies[name])
        pod["initContainers"] = [{"name": "wait-dependencies", "image": container["image"], "imagePullPolicy": container["imagePullPolicy"], "command": ["sh", "-c", wait], "resources": memory("32Mi", "64Mi"), "securityContext": {"runAsNonRoot": True, "runAsUser": 10001}}]
    if role not in ["store", "broker"]:
        pod["securityContext"] = {"runAsNonRoot": True, "runAsUser": 10001, "fsGroup": 10001}
        pod["automountServiceAccountToken"] = name == "aggregator"
    if volume:
        container["volumeMounts"] = [{"name": "data", "mountPath": volume}]
    if name == "aggregator":
        pod["serviceAccountName"] = "aggregator-data"
        pod["terminationGracePeriodSeconds"] = 30
        # Count this revision during a rollout, so outgoing pods do not push the new replicas onto one worker.
        aggregator_selector = {"matchLabels": {"app": "aggregator"}}
        revision_keys = ["pod-template-hash"]
        pod["topologySpreadConstraints"] = [{"maxSkew": 1, "topologyKey": "kubernetes.io/hostname", "whenUnsatisfiable": "ScheduleAnyway", "labelSelector": aggregator_selector, "matchLabelKeys": revision_keys}]
        pod["affinity"] = {"podAntiAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": [{
            "weight": 100,
            "podAffinityTerm": {"topologyKey": "kubernetes.io/hostname", "labelSelector": aggregator_selector, "matchLabelKeys": revision_keys},
        }]}}
        pod["volumes"] = [{"name": "schema-policy", "configMap": {"name": "source-schema-policy"}}]
        container["volumeMounts"] = [{"name": "schema-policy", "mountPath": "/etc/atlas/schema", "readOnly": True}]
        container["lifecycle"] = {"preStop": {"exec": {"command": ["sh", "-c", "sleep 5"]}}}
    spec = {"replicas": 1, "selector": {"matchLabels": {"app": name}}, "template": {"metadata": {"labels": labels}, "spec": pod}}
    if stateful:
        spec["serviceName"] = name
        spec["volumeClaimTemplates"] = [{"metadata": {"name": "data"}, "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "2Gi"}}}}]
    else:
        spec["strategy"] = {"type": "RollingUpdate", "rollingUpdate": {"maxUnavailable": 0, "maxSurge": 1}} if not volume else {"type": "Recreate"}
        if volume:
            pod["volumes"] = [{"name": "data", "persistentVolumeClaim": {"claimName": name + "-data"}}]
    return resource("StatefulSet" if stateful else "Deployment", name, spec=spec)


def main():
    objects = []
    for name, owner in [("auth-db", "auth"), ("bmkg-db", "bmkg"), ("pvmbg-db", "pvmbg")]:
        objects += [service(name, 5432, True), workload(name, "store", 5432, ("512Mi", "1Gi"), stateful=True, owner=owner, volume="/var/lib/postgresql", image="postgres:18.3-alpine@sha256:54451ecb8ab38c24c3ec123f2fd501303a3a1856a5c66e98cecf2460d5e1e9d7")]
    objects += [service("rabbitmq", 5672, True), workload("rabbitmq", "broker", 5672, ("512Mi", "1Gi"), stateful=True, volume="/var/lib/rabbitmq", image="rabbitmq:4.2.5-alpine@sha256:d4a3806070111cc69130a6542aa4049b612789852ef11fc3862abc94631b2572")]
    for name, port in [("bmkg", 8081), ("pvmbg", 8082)]:
        objects += [service(name, port), workload(name, "mock", port, ("64Mi", "128Mi"))]
    objects += [service("auth", 8084), workload("auth", "api", 8084, ("128Mi", "256Mi"))]
    objects += [service("client-api", 8080), workload("client-api", "api", 8080, ("128Mi", "256Mi"))]
    objects += [service("aggregator", 8083), workload("aggregator", "aggregator", 8083, ("256Mi", "512Mi"), env={"SOURCE_SCHEMA_POLICY_FILE": "/etc/atlas/schema/policy.json", "GOMEMLIMIT": "384MiB"})]
    for name, port in [("consumer-a", 8091), ("consumer-b", 8092), ("notifier", 8093)]:
        objects += [workload(name, "consumer", port, ("128Mi", "256Mi"), volume="/data"), resource("PersistentVolumeClaim", name + "-data", spec={"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "1Gi"}}})]
    objects += [service("sensor-pantai", 8080)]
    write("base/workloads.yaml", {"apiVersion": "v1", "kind": "List", "items": objects})
    write("base/namespace.yaml", resource("Namespace", NAMESPACE, **{}))
    policies = [resource("NetworkPolicy", "store-default-deny", spec={"podSelector": {"matchLabels": {"role": "store"}}, "policyTypes": ["Ingress"], "ingress": []})]
    for name, owner in [("canonical-store", "aggregator"), ("auth-db", "auth"), ("bmkg-db", "bmkg"), ("pvmbg-db", "pvmbg")]:
        sources = [{"podSelector": {"matchLabels": {"app": owner}}}, {"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "tester-agent"}}}]
        policies.append(resource("NetworkPolicy", name + "-owner", spec={"podSelector": {"matchLabels": {"app": name}}, "policyTypes": ["Ingress"], "ingress": [{"from": sources, "ports": [{"protocol": "TCP", "port": 5432}]}]}))
    write("base/network-policies.yaml", {"apiVersion": "v1", "kind": "List", "items": policies})
    if not (ROOT / "base/policy.json").exists():
        write("base/policy.json", {})
    kustomization = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization", "namespace": NAMESPACE, "resources": ["namespace.yaml", "workloads.yaml", "network-policies.yaml", "../../store", "../vendor/sensor-pantai"], "configMapGenerator": [{"name": "source-schema-policy", "files": ["policy.json"]}], "generatorOptions": {"disableNameSuffixHash": True}, "patches": [{"target": {"kind": "Deployment", "name": "sensor-pantai"}, "patch": json.dumps([{"op": "add", "path": "/spec/template/spec/containers/0/resources", "value": memory("128Mi", "256Mi")}])}, {"target": {"kind": "StatefulSet", "name": "canonical-store"}, "patch": json.dumps([{"op": "add", "path": "/spec/template/metadata/labels/owner", "value": "aggregator"}])}]}
    kustomization["patches"][0]["patch"] = json.dumps([
        {"op": "add", "path": "/spec/template/spec/containers/0/resources", "value": memory("128Mi", "256Mi")},
        {"op": "add", "path": "/spec/template/spec/containers/0/livenessProbe", "value": {"tcpSocket": {"port": 8080}, "initialDelaySeconds": 30, "periodSeconds": 10, "timeoutSeconds": 2}},
    ])
    write("base/kustomization.yaml", kustomization)
    write("vendor/sensor-pantai/kustomization.yaml", {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization", "resources": ["manifest.yaml"]})
    write("overlays/dev/kustomization.yaml", {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization", "resources": ["../../base"], "replicas": [{"name": "canonical-store", "count": 1}], "patches": [{"target": {"kind": "StatefulSet", "name": "canonical-store"}, "patch": json.dumps([{"op": "remove", "path": "/spec/volumeClaimTemplates/0/spec/storageClassName"}])}]})
    pin = {"target": {"kind": "Deployment", "name": "auth|client-api|bmkg|pvmbg|consumer-a|consumer-b|notifier"}, "patch": json.dumps([{"op": "add", "path": "/spec/template/spec/nodeSelector", "value": {"kubernetes.io/hostname": "atlas-node-a"}}])}
    host_addresses = ["10.0.0." + str(host) for host in [40, 41, 42]] + ["10.42." + str(subnet) + "." + str(host) for subnet in [0, 2, 4] for host in [0, 1]]
    host_patch = {"target": {"kind": "NetworkPolicy", "name": "canonical-store-owner|auth-db-owner|bmkg-db-owner|pvmbg-db-owner"}, "patch": json.dumps([{"op": "add", "path": "/spec/ingress/0/from/-", "value": {"ipBlock": {"cidr": address + "/32"}}} for address in host_addresses])}
    # The existing Proxmox Tester Agent uses host networking in dev as well.
    dev_path = ROOT / "overlays/dev/kustomization.yaml"
    dev = json.loads(dev_path.read_text())
    dev["patches"].append(host_patch)
    write("overlays/dev/kustomization.yaml", dev)
    demo = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization", "resources": ["../../base"], "replicas": [{"name": "aggregator", "count": 3}], "patches": [pin, host_patch, {"target": {"kind": "StatefulSet", "name": "auth-db|bmkg-db|pvmbg-db|rabbitmq"}, "patch": pin["patch"]}, {"target": {"kind": "Deployment", "name": "aggregator"}, "patch": json.dumps([{"op": "add", "path": "/spec/template/spec/containers/0/env/-", "value": {"name": "INGESTION_DISABLED", "value": "1"}}, {"op": "add", "path": "/spec/template/spec/containers/0/env/-", "value": {"name": "CLUSTER_DATA_CONFIG", "value": "/etc/atlas/data/config.json"}}, {"op": "add", "path": "/spec/template/spec/volumes/-", "value": {"name": "data-config", "secret": {"secretName": "canonical-data-config"}}}, {"op": "add", "path": "/spec/template/spec/containers/0/volumeMounts/-", "value": {"name": "data-config", "mountPath": "/etc/atlas/data", "readOnly": True}}])}]}
    demo["replicas"] += [{"name": name, "count": 2} for name in ["auth", "client-api"]]
    write("overlays/demo/kustomization.yaml", demo)
    print("Generated base and dev/demo overlays; vendor manifests preserved")


if __name__ == "__main__":
    main()
