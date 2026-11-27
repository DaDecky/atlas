"""Prepare Atlas demo assets locally; this script never contacts the cluster."""

import json
from pathlib import Path
import urllib.request


ASSETS = Path(__file__).resolve().parent
ROOT = ASSETS.parents[1]
NS = "sistem-informasi-kebencanaan-bnpb"
SENSOR_IMAGE = "sustuna/sensor-pantai@sha256:51fbd90d3891ad6e9f5768ad330ffb65205644cb29e50da65c7538f2e0b4397d"
TESTER_IMAGE = "sustuna/tester-agent@sha256:b3143da159cf23a8744ede12b847da6208353754234d53a5a7b46d071cf277ec"
MANIFEST_URL = "https://raw.githubusercontent.com/susTuna/if4031-tester-agent/ebb55261711123a9a211739ffbf140a275caa6da/manifest.yaml"


def write_json(relative, value):
    path = ASSETS / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_text(relative, value):
    path = ASSETS / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def main():
    sensor_patch = {
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": "sensor-pantai"},
        "spec": {"template": {"spec": {
            "containers": [{"name": "sensor", "image": SENSOR_IMAGE,
                            "env": [{"name": "PANTAI_PROFILE", "value": "exam"},
                                    {"name": "PANTAI_SEED", "value": "42"}],
                            "volumeMounts": [{"name": "key", "mountPath": "/var/run/pantai", "readOnly": True}]}],
            "volumes": [{"name": "key", "secret": {"secretName": "pantai-key"}}],
        }}},
    }
    write_json("sensor-exam.patch.json", {"spec": sensor_patch["spec"]})
    patches = [{"target": {"kind": "Deployment", "name": "sensor-pantai"},
                "patch": json.dumps(sensor_patch)}]
    patches.append({"target": {"kind": "Deployment", "name": "aggregator"}, "patch": json.dumps({
        "apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "aggregator"},
        "spec": {"template": {"spec": {"containers": [{"name": "aggregator", "env": [
            {"name": "INGESTION_DISABLED", "value": "0"}]}]}}},
    })})
    telemetry_env = [
        {"name": "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "value": "http://jaeger:4318/v1/traces"},
        {"name": "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "value": "http://victoriametrics:8428/opentelemetry/v1/metrics"},
        {"name": "OTEL_TRACES_SAMPLER", "value": "parentbased_traceidratio"},
        {"name": "OTEL_TRACES_SAMPLER_ARG", "value": "0.1"},
    ]
    for name in ["aggregator", "auth", "client-api"]:
        patches.append({"target": {"kind": "Deployment", "name": name}, "patch": json.dumps({
            "apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": name},
            "spec": {"template": {"spec": {"containers": [{"name": name, "env": telemetry_env}]}}},
        })})
    services = ["bmkg", "pvmbg", "auth", "client-api", "aggregator", "consumer-a", "consumer-b", "notifier"]
    write_json("exam-overlay/kustomization.yaml", {
        "apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization", "namespace": NS,
        "resources": ["../../../deploy/k8s/overlays/demo", "../../../deploy/k8s/telemetry"],
        "images": [{"name": "atlas/" + name, "newTag": "milestone-2"} for name in services],
        "patches": patches,
    })
    with urllib.request.urlopen(MANIFEST_URL, timeout=30) as response:
        manifest = response.read().decode("utf-8")
    if TESTER_IMAGE not in manifest or "secretName: tester-key" not in manifest:
        raise ValueError("Official Tester Agent manifest does not match the supplied image/key reference")
    write_text("tester-manifest.yaml", manifest)
    for name, namespace in [("pantai-key", NS), ("tester-key", "tester-agent")]:
        write_json("templates/" + name + ".template.yaml", {
            "apiVersion": "v1", "kind": "Secret", "metadata": {"name": name, "namespace": namespace},
            "type": "Opaque", "stringData": {"key": "<gunakan berkas key dari email; jangan apply template ini>"},
        })
    write_json("p3/policy-before.json", {})
    write_json("p3/policy-after.json", {"accept_ash_rename": True, "accept_string_count": True})
    write_json("p3/replay.json", {"source": "PVMBG", "reason_code": "schema_violation", "limit": 100})
    for version in [2, 3, 4, 5]:
        write_json(f"p3/schema-{version}.json", {"version": version})
    write_json("p3/field-report.json", {
        "report_id": "atlas-demo-field-20261127-001", "observed_at": "2026-11-27T00:00:00Z",
        "location": {"latitude": -6.9, "longitude": 107.6, "area_name": "Bandung"},
        "category": "SEISMIC", "message": "Contoh laporan demo Atlas; perbarui waktu observasi sebelum pengiriman.",
    })
    write_text("p3/requests.http", """# Request manual: jalankan hanya ketika cluster dan token sudah tersedia.
# Isi OPS_TOKEN/FIELD_TOKEN di environment REST client, bukan dalam file ini.
@client = http://localhost:8080
@audit = http://localhost:8083

### Daftar parking: limit Internal Ops maksimal 10
GET {{client}}/internal-ops/unprocessed?source=PVMBG&limit=10
Authorization: Bearer {{OPS_TOKEN}}
X-Correlation-ID: atlas-demo-p3-list

### Ambil body satu pesan (raw base64; periksa raw_chunked/ReadParkingPart jika perlu)
GET {{client}}/internal-ops/unprocessed?source=PVMBG&include_raw=true&limit=1
Authorization: Bearer {{OPS_TOKEN}}

### Ringkasan Audit
GET {{audit}}/internal/audit/unprocessed?source=PVMBG&limit=20
Authorization: Bearer {{AUDIT_TOKEN}}

### Replay hanya setelah kebijakan diperbaiki dan gangguan ujian selesai
POST {{client}}/internal-ops/replay
Authorization: Bearer {{OPS_TOKEN}}
Content-Type: application/json
X-Correlation-ID: atlas-demo-p3-replay

{"source":"PVMBG","reason_code":"schema_violation","limit":100}

### Laporan lapangan: gunakan payload field-report.json dan perbarui observed_at
POST {{client}}/field-reports
Authorization: Bearer {{FIELD_TOKEN}}
Content-Type: application/json
X-Correlation-ID: atlas-demo-p3-field

< ./field-report.json
""")
    write_text("p4/requests.http", """# Pembacaan manual; start ujian hanya saat recorder sudah berjalan.
@audit = http://localhost:8083
@satellite = https://satellite.naufarrel.dev

### SATELIT pada Atlas: lanjutkan next_cursor untuk ekspor lengkap
GET {{audit}}/internal/audit/events?source=SATELIT&limit=1000
Authorization: Bearer {{AUDIT_TOKEN}}

### Checksum SATELIT pada Atlas
GET {{audit}}/internal/audit/checksum?source=SATELIT
Authorization: Bearer {{AUDIT_TOKEN}}

### Stream: jangan jalankan sekarang untuk memulai sesi
GET {{satellite}}/v1/stream
Authorization: Bearer {{SATELLITE_TOKEN}}

### Mulai ujian SEKALI, tepat saat rekaman; jangan gunakan Send All
POST {{satellite}}/v1/exam/start
Authorization: Bearer {{SATELLITE_TOKEN}}
""")
    fixture = [{
        "fire_id": "synthetic-fire-001", "detection_keys": ["synthetic-a", "synthetic-b"],
        "detection_count": 2, "total_frp_mw": 20, "severity": "WASPADA",
        "latitude": -6.0, "longitude": 106.0, "area_name": "Synthetic fixture",
        "occurred_at": "2026-11-27T00:00:00Z", "satellites": ["NOAA-20", "Suomi-NPP"],
        "max_confidence": "h",
    }]
    write_json("p4/synthetic-reference.json", fixture)
    write_json("p4/synthetic-match.json", fixture)
    write_json("p4/synthetic-mismatch.json", [{**fixture[0], "total_frp_mw": 21}])
    write_json("materials.json", {
        "sensor_image": SENSOR_IMAGE, "tester_image": TESTER_IMAGE, "tester_manifest": MANIFEST_URL,
        "deadline_wib": "2026-11-29T23:59:00+07:00", "p1_expected_records": 25001,
        "p1_expected_checksum": "8600358903602dad850c6a5bd96b670336ad0d4ffd9a69932dea25c58b344f0f",
        "reference_fixtures": "synthetic only; not assistant exam data",
        "pending_external_materials": ["seismic_events.jsonl.gz", "satellite reference for the recorded window, if supplied"],
        "cluster_changes": False, "exam_session_started": False,
    })
    print("Prepared local demo assets; no cluster command or satellite session was executed.")


if __name__ == "__main__":
    main()
