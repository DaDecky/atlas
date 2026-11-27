# Panduan Demo Milestone 2 - Atlas

Panduan operator dan narator untuk persiapan, rekaman, pengumpulan bukti, dan pengiriman video. Acuan terbaru adalah email Asisten IF4031 tanggal 25 November 2026 dan [tutorial demo resmi](https://docs.google.com/document/d/1qfJWX5VjpUeQiEESJ4y-6Zrh-cgavDPGeD0dcVSWPVg/edit?tab=t.0). Detail aplikasi mengikuti [README](README.md).

**Tenggat video: Minggu, 29 November 2026, pukul 23.59 WIB.** Tutorial terbaru mewajibkan YouTube public atau unlisted, tanpa pemendek URL. Judul: `Demo IF4031 - <No Kelompok> - Atlas - 2026`, disertai deskripsi singkat.

Dokumen ini berisi prosedur dan target pengujian, bukan pernyataan bahwa pengujian sudah lulus. Isi hasil dengan angka dari run yang direkam.

**Script siap dijalankan setelah VM hidup:** [QUICKSTART setup dan demo](demo-assets/m2/QUICKSTART.md) menjelaskan installer dependency Ubuntu, bootstrap tiga VM dari operator, deploy konfigurasi ujian, serta command demo P1-P5. Periksa status lokal dengan `python demo-assets/m2/demo.py status`; perintah ini tidak mengakses cluster.

## 1. Pembagian tugas dan alur video

Pembagian demo mengikuti kontribusi di laporan: **Razi Rachman Widyadhana (13523004)** memegang keandalan dan operasi/P2/P5, **Muhammad Dicky Isra (13523075)** memegang data/P1, dan **Muhammad Adha Ridwan (13523098)** memegang cluster, ingestion, telemetry/P3/P4. Semua anggota tampil dan menjelaskan bagiannya.

**Adha menjadi satu-satunya operator yang mengubah cluster**, termasuk deploy, Secret, Tester Agent, scaling, rollout, dan operasi VM. Razi menjalankan k6 di runner dan Dicky melakukan pemeriksaan data serta merekam layar. Saat Razi atau Dicky menjelaskan skenario, mereka memberi aba-aba kepada Adha untuk menjalankan tindakan cluster; perintah mutasi pada bagian masing-masing tetap dijalankan Adha.

| Bagian, sesuai urutan wajib | Isi penjelasan | Penanggung jawab |
| --- | --- | --- |
| 1. Arsitektur dan keputusan desain | Diagram, alur antar-service, perubahan M1 ke M2, pilihan teknologi, alternatif, dan trade-off. | **Razi** membuka dan menjelaskan arsitektur umum; **Dicky** menjelaskan penyimpanan; **Adha** menjelaskan deployment dan ingestion. |
| 2. Menjalankan aplikasi | Build, konfigurasi environment, satu perintah deploy, dan pemeriksaan kesiapan service. | **Adha** menjalankan dan menjelaskan deploy; **Dicky** merekam terminal dan status service. |
| 3. Demo P1 | Sharding, replikasi, pembacaan, node hilang, dan pemulihan. | **Dicky** memimpin narasi, database, dan Audit; **Adha** menjalankan gangguan/recovery; **Razi** memantau request dan latensi. |
| 3. Demo P2 | Lease, writer epoch, failover, perubahan jam, scaling, dan rollout. | **Razi** memimpin narasi dan k6; **Adha** menjalankan skenario, scaling, rollout, dan freeze/resume; **Dicky** membantu bukti data dan rekaman. |
| 3. Demo P3 | Parking/DLQ, kebijakan skema, replay, dan laporan lapangan. | **Adha** memimpin narasi ingestion, skema, dan replay; **Dicky** menjelaskan tabel parking serta bukti recovered; **Razi** membantu request client dan pemeriksaan scope. |
| 3. Demo P4 | Feed satelit live, checkpoint, pengelompokan kebakaran, dan referensi. | **Adha** memimpin narasi, offset/lag, dan pemulihan feed; **Dicky** membantu ekspor dan reference diff; **Razi** membantu pembacaan dashboard. |
| 3. Demo P5 | Sensor Pantai, isolasi gangguan, overload, notifier, dan error budget. | **Razi** memimpin narasi, k6, notifier capture, dan error budget; **Adha** menjalankan skenario exam; **Dicky** menunjukkan penyimpanan/outbox dan merekam hasil. |
| Penutup dan pengiriman | Ringkas hasil aktual, batasan, identitas video, URL YouTube, dan bukti `received_at`. | **Razi** menutup, mengunggah, dan mengirim URL; **Dicky** memeriksa video final; **Adha** memverifikasi kesiapan akses dan bukti pengiriman. |

Rekam pengujian secara langsung. Jelaskan komunikasi, perubahan state/data, dan penyebab hasilnya selama skenario berjalan. Siapkan terminal dan ukuran huruf yang terbaca, suara yang jelas, serta diagram dan kode yang akan ditunjukkan. Ikuti batas durasi pada spesifikasi, yang ditulis maksimal 30-45 menit; latih rundown sebelum menjalankan sesi satelit ujian.

### 1.1 Tugas Razi sebelum dan saat rekaman

- Siapkan diagram dan narasi pembuka, perubahan M1 ke M2, keputusan teknologi, serta penutup berdasarkan hasil aktual.
- Siapkan runner k6, kredensial client, akses langsung ke Client API/Auth, dan output untuk sustained, throughput 1/3 instance, rollout, dan overload per kelas client.
- Siapkan skenario P2 bersama Adha: tentukan pemegang lease yang akan diamati dan worker untuk freeze, lalu jelaskan lease, epoch, dan hasil failover/fencing.
- Siapkan P5: jalur alert, indikator circuit breaker, dashboard latensi BMKG, notifier capture, dan perhitungan error budget. Mulai notifier capture sebelum jendela P5 dan simpan PID serta waktu awalnya.
- Selama P1, bantu Dicky mengukur request/latensi. Selama P3, bantu request client, scope, dan laporan Field Team.
- Setelah rekaman, unggah ke YouTube dengan judul/deskripsi yang diwajibkan, kirim URL, dan simpan respons pengiriman serta hasil GET verifikasi.

### 1.2 Tugas Dicky sebelum dan saat rekaman

- Ambil lampiran dataset resmi, verifikasi 25.001 event dan checksum, lalu siapkan konversi `SeismicEvent` ke `HazardEvent` sesuai pemetaan repo. Koordinasikan eksekusi bulk-load dengan Adha.
- Siapkan baseline dataset, sebaran shard/replica, contoh `hazard_id`, query database, pagination, dan pembanding checksum/himpunan ID yang tidak tercampur event live.
- Pimpin P1: jelaskan distribusi, dua salinan, partial read, kehilangan node, refill, dan bukti tidak ada data hilang.
- Bantu P3 dengan tabel pesan gagal/body/hash/status recovered, serta bantu P5 dengan penyimpanan/outbox. Semua query pemeriksaan bersifat baca.
- Bantu Adha menyiapkan ekspor satelit lengkap dan referensi P4 pada window/offset yang sesuai.
- Menjadi penanggung jawab screen recorder, memastikan layar narator/operator terlihat, teks terbaca, suara setiap anggota masuk, dan log Tester Agent terlihat selama gangguan. Simpan rekaman serta tangkapan bukti per problem dan periksa video final sebelum Razi mengirim.

### 1.3 Tugas Adha sebelum dan saat rekaman

- Siapkan tiga VM, k3s, kubeconfig, SSH/Proxmox, NetworkPolicies, image, telemetry, dan endpoint yang dapat dijangkau runner.
- Siapkan `.env.m2`, Secret ujian, profil exam Sensor Pantai, Tester Agent, port forward, readiness, dan log seluruh node.
- Jalankan dan jelaskan build/deploy dari awal. Koordinasikan pemuatan dataset Dicky dan import image pengganti yang diperlukan Razi.
- Setelah Dicky memastikan recorder sudah berjalan, mulai sesi satelit **sekali** dan catat responsnya. Tampilkan bahwa ingestion mengikuti feed ujian sejak awal rekaman.
- Jalankan `exam p1`, `exam p2`, `exam p3`, dan `exam p5` satu per satu sesuai aba-aba narator. Tunggu hasil dan pemulihan sebelum berpindah problem. Kendalikan seluruh scaling, rollout, serta freeze/resume VM.
- Pimpin P3 dan P4: tunjukkan alur ingestion, kebijakan skema, replay, checkpoint, lag, koreksi kebakaran, resume, dan reference diff bersama Dicky.
- Saat game day P5 aktif, pantau tanpa intervensi sampai window selesai. Setelah video diunggah, bantu memastikan URL bisa diakses dan bukti pengiriman tersimpan.

### 1.4 Aba-aba saat pergantian bagian

1. **Dicky:** pastikan recorder, mikrofon, dan tampilan log siap; mulai rekaman.
2. **Adha:** mulai sesi satelit sekali setelah recorder berjalan; kemudian **Razi** membuka penjelasan arsitektur.
3. **Adha:** pimpin bagian deploy; setelah service siap, serahkan narasi ke **Dicky** untuk P1.
4. **Dicky -> Razi -> Adha -> Razi:** urutan pemimpin narasi P1, P2, P3/P4, lalu P5. Adha tetap menjadi operator cluster sepanjang sesi.
5. **Razi:** setelah tiap problem, sebutkan hasil ukur dan batasannya. **Dicky** memastikan bukti tersimpan sebelum memberi aba-aba lanjut.
6. Setelah semua pengujian selesai, **Razi** menutup; **Dicky** menghentikan recorder dan memeriksa video; **Razi** mengunggah/mengirim, lalu **Adha** ikut memeriksa respons penerimaan.

### 1.5 Paket persiapan lokal Adha

Aset persiapan yang dapat dibagikan tersedia di [`demo-assets/m2/`](demo-assets/m2/README.md). Token, kunci, backup environment, dependency Python lokal, dan hasil render/check tetap berada di `.env.m2` serta `.deploy/demo-private/` yang diabaikan Git. Pada mesin persiapan ini, token satelit dan kedua berkas kunci sudah diisi; anggota lain mengisi kredensial dari email pada mesin masing-masing. Ikuti langkah pada README paket untuk memasang dependency dan memeriksa aset tanpa cluster.

| Berkas | Kegunaan saat infra tersedia |
| --- | --- |
| [sensor-exam.patch.json](demo-assets/m2/sensor-exam.patch.json) | Patch profil exam, seed 42, digest sensor, dan mount `pantai-key`. Terapkan sesudah deploy aplikasi. |
| [tester-manifest.yaml](demo-assets/m2/tester-manifest.yaml) | Manifest resmi dengan digest email, target namespace, mount `tester-key`, dan toleration control-plane yang telah diperiksa. |
| [exam-overlay/kustomization.yaml](demo-assets/m2/exam-overlay/kustomization.yaml) | Konfigurasi untuk review lokal: profil sensor exam, ingestion aktif, telemetry, serta tag image `milestone-2`. |
| `.deploy/demo-private/exam-rendered.yaml` | Hasil render 48 objek untuk pemeriksaan konfigurasi. Deploy aplikasi tetap memakai `make deploy-demo`, karena bootstrap Secret, placement, dan konfigurasi data dilakukan oleh script deploy. |
| [templates/](demo-assets/m2/templates/) | Template struktur Secret tanpa nilai kunci. Buat Secret aktual memakai `--from-file=key=...` pada bagian 3.2; jangan apply nilai placeholder template. |
| [p3/policy-before.json](demo-assets/m2/p3/policy-before.json) dan [p3/policy-after.json](demo-assets/m2/p3/policy-after.json) | Kebijakan awal ketat dan contoh perbaikan untuk rename ash/string count. Aktifkan hanya perbaikan yang sesuai gangguan yang diamati. |
| [p3/replay.json](demo-assets/m2/p3/replay.json), [p3/field-report.json](demo-assets/m2/p3/field-report.json), dan [p3/requests.http](demo-assets/m2/p3/requests.http) | Payload replay/laporan serta request pemeriksaan parking dan body. Perbarui waktu observasi pada contoh laporan sebelum pengiriman. |
| [p4/requests.http](demo-assets/m2/p4/requests.http) | Request inspeksi Audit/stream dan start sesi yang dipisahkan. Jangan memakai Send All karena berisi start ujian. |
| [export-audit.py](demo-assets/m2/export-audit.py) | Ekspor Audit lengkap dengan pagination, pemeriksaan partial/cursor, dan metadata waktu. Token dibaca dari environment. |
| [p1/conversion-contract.json](demo-assets/m2/p1/conversion-contract.json) | Handoff untuk Dicky: format sumber/kanonik, identitas BMKG, checksum resmi, dan batas pemuatan dengan writer epoch yang berlaku. |
| `.deploy/demo-private/checks/asset-results.json` dan `go-results.json` | Hasil validasi lokal dan lokasi log build/unit test. |

Validasi lokal: overlay dasar berhasil dirender menjadi 39 objek; overlay ujian menjadi 48 objek, dengan sensor exam, ingestion, telemetry, dan replika Aggregator/Auth/Client API 3/2/3. Digest/mount Tester Agent cocok. Build dan unit test sembilan modul runtime lulus menggunakan Go 1.27.4 pada modul. Skrip reference diff diuji dengan fixture sintetis yang cocok dan yang sengaja berbeda; fixture tersebut bukan data ujian atau hasil feed live.

Belum dilakukan: akses cluster, pemasangan Secret, deploy, start sesi satelit, pengujian gangguan, dan pengukuran performa live. Lampiran dataset P1 dan referensi satelit asli belum tersedia dalam paket ini. Karena belum ada dataset asli, pemuatan dan konversi 25.001 record belum dijalankan.

Untuk memeriksa kembali paket tanpa mengakses cluster, gunakan:

```bash
python demo-assets/m2/check-assets-local.py
```

Jika kode berubah dan pemeriksaan build/unit perlu diulang, jalankan `python demo-assets/m2/check-go-local.py`. Tambahkan `--check-private` pada pemeriksaan aset untuk memvalidasi keberadaan kunci dan environment lokal setelah diisi. Script pemeriksaan Go memakai unit test biasa tanpa tag integrasi; pengujian terhadap PostgreSQL/RabbitMQ/cluster tetap merupakan tahap tersendiri.

Setelah infra tersedia, ikuti bagian 3 untuk deploy aplikasi dan pembuatan Secret dari berkas kunci yang sudah siap, terapkan patch sensor, lalu verifikasi Tester Agent dan endpoint. Untuk ekspor P4, muat `AUDIT_TOKEN` ke environment dan gunakan helper yang telah disiapkan:

```bash
python demo-assets/m2/export-audit.py --source SATELIT \
  --output "$EVIDENCE/p4-satellite.json"
```

### 1.6 Catatan narasi Adha

**Arsitektur dan deployment:** buka diagram deployment, [overlay demo](deploy/k8s/overlays/demo/kustomization.yaml), dan [script deploy](scripts/deploy-k8s.py). Jelaskan: "Atlas memisahkan service aplikasi, broker, dan penyimpanan. Tiga VM mempunyai kernel terpisah; jika tetap pada topologi Proxmox yang dilaporkan, ketiganya berbagi satu host fisik. Aggregator dan Canonical Store tersebar, sedangkan beberapa service pendukung berada di node-a. Script deploy membangun dan mengimpor image, memasang konfigurasi, menyiapkan placement, lalu menunggu readiness." Tampilkan topologi yang benar-benar dipakai saat rekaman dan sebutkan batas CPU/RAM bersama jika semua VM masih di satu host.

**Menjalankan aplikasi:** tunjukkan perintah deploy dan hasil readiness, tanpa membuka isi environment/Secret. Jelaskan beda token HTTP sensor dengan kunci ujian, profil exam/seed, dan fungsi Tester Agent di setiap node. Tampilkan status service, dashboard metrik, trace, dan cara request client mencapai Aggregator. Sebutkan bahwa Razi memakai runner terpisah untuk pengukuran beban.

**P3, sebelum gangguan:** buka [kebijakan ingestion](services/bnpb/aggregator/internal/aggregator/ingestion_policy.go) dan [Ops API](services/bnpb/aggregator/internal/aggregator/ops.go). Jelaskan: "Field opsional dapat diterima tanpa menebak perubahan makna. Rename, perubahan tipe, atau hilangnya field wajib ditahan sampai ada kebijakan eksplisit. Data gagal disimpan bersama alasan, hash, dan correlation ID; Dicky akan menunjukkan penyimpanannya." Jalankan ujian lalu tunjukkan alasan aktual yang muncul. API Internal Ops membatasi listing maksimal 10 item, sedangkan Audit mempunyai batas pagination sendiri.

**P3, perbaikan dan replay:** jelaskan perubahan policy yang diperlukan, tunggu file ConfigMap tersinkron, lalu kirim replay. Jawaban 202 dengan status `queued` berarti perintah replay sudah diterima, bukan seluruh pesan sudah recovered. Tunggu hasil dan minta Dicky menunjukkan status recovered serta event yang benar. Tunjukkan bahwa pengiriman ulang laporan lapangan tidak menggandakan `report_id` dan jelaskan peran Aggregator sebagai konsumen `field.reports`.

**P4, aliran dan checkpoint:** buka [adapter satelit](services/bnpb/aggregator/cmd/aggregator/satellite.go), [mesin pengelompokan](services/bnpb/aggregator/internal/satellite/engine.go), dan [integrasi satelit](services/bnpb/aggregator/internal/aggregator/satellite.go). Jelaskan: "Aliran ujian dimulai sekali sejak awal rekaman pada 60x. Offset menjadi checkpoint; watermark berasal dari waktu event. Deteksi dari beberapa satelit dapat membentuk satu kebakaran. Update/retract mengubah state yang sama, dan resume harus melanjutkan tanpa kehilangan atau penggandaan." Tunjukkan contoh nyata beserta offset/lag; jangan menyatakan jalur yang belum terpicu sudah terbukti.

**P4, pembuktian hasil:** jelaskan window/offset yang dipakai, ekspor semua halaman Audit, dan perlihatkan diff bersama Dicky. Sebutkan jumlah kebakaran aktif, selisih anggota/field, dan waktu mengejar backlog berdasarkan hasil aktual. Jika referensi asli belum diberikan, nyatakan pembandingan referensi belum dilakukan; hasil fixture sintetis hanya memeriksa alat pembanding.

**Aba-aba operasi:** sebelum gangguan, sebutkan skenario dan target yang akan dipicu; sesudahnya tunjukkan perubahan metrik/log dan tunggu bukti tersimpan. Pada P1/P2/P5, Razi atau Dicky memimpin penjelasan sementara Adha menjalankan perintah cluster. Selama game day tanpa intervensi, amati dan catat sampai jendela berakhir.

## 2. Checklist sebelum rekaman

- [ ] Checkout yang digunakan sesuai tag `milestone-2`; catat hash commit dan tag image yang terpasang.
- [ ] Tiga node Atlas (`atlas-node-a`, `atlas-node-b`, `atlas-node-c`) Ready dan memiliki kernel terpisah. Tutorial menetapkan minimal dua node, tetapi implementasi Canonical Store Atlas membutuhkan tiga.
- [ ] Akses SSH dan Proxmox tersedia untuk menyalakan kembali atau membekukan VM.
- [ ] Docker, Go 1.27.4, Python 3 dengan `cryptography`, Bash, Make, kubectl, curl, jq, dan k6 tersedia pada mesin yang sesuai.
- [ ] Alias SSH ketiga node dapat menjalankan `sudo k3s ctr` untuk impor image.
- [ ] `.env.m2` sudah disiapkan dan `SATELLITE_TOKEN` sudah diisi dari email.
- [ ] `AUDIT_TOKEN`, token akses HTTP Sensor Pantai, dan kredensial tiga kelas client tersedia.
- [ ] Dataset `seismic_events.jsonl.gz` dari lampiran sudah diunduh, diverifikasi, dikonversi, dan dimuat.
- [ ] Sensor Pantai memakai profil `exam`, seed `42`, image sesuai digest email, dan kunci terpasang.
- [ ] Tester Agent memakai digest email dan Secret `tester-key`; satu pod Ready di setiap node, termasuk control-plane.
- [ ] Port forward, dashboard, database client, dan pengambilan log sudah disiapkan.
- [ ] Mesin k6 dan jalur akses langsung ke Client API/Auth siap. Port forward tidak dipakai untuk pengukuran kapasitas.
- [ ] Image pengganti untuk rolling update sudah tersedia pada semua node.
- [ ] Referensi satelit, jika diberikan asisten, tersedia beserta window/offset pembandingnya.
- [ ] Pembagian narator dan recorder sudah dicoba.

### 2.1 Token dan kunci mempunyai fungsi berbeda

| Materi | Lokasi atau penggunaan |
| --- | --- |
| Token demo Feed Satelit | `SATELLITE_TOKEN` di `.env.m2`, diteruskan deploy ke Secret `aggregator-credentials`. Dipakai untuk start sesi, stream, dan pengiriman video sesuai email. |
| `AUDIT_TOKEN` | Bearer token Audit API Atlas. Email ini tidak secara eksplisit memberikan token Audit yang berbeda; gunakan konfigurasi yang disepakati dengan asisten. |
| `PANTAI_AUTH_TOKEN` | Token HTTP bersama antara Sensor Pantai dan adapter Atlas, termasuk pembacaan histori untuk error budget. |
| Kunci ujian Sensor Pantai | Berkas `key` dalam Secret `pantai-key`, dipasang sebagai `/var/run/pantai/key`. Ini berbeda dari `PANTAI_AUTH_TOKEN`. |
| Kunci ujian Tester Agent | Berkas `key` dalam Secret `tester-key`, namespace `tester-agent`, dipasang oleh manifest resmi ke `/var/run/tester/key`. |
| Kredensial Media, Field Team, Internal Ops | Login ke Auth untuk mendapatkan access token sesuai scope. Access token perlu diperbarui ketika kedaluwarsa. |

Nilai rahasia diambil dari email dan konfigurasi lokal. Jangan memasukkannya ke dokumen, commit, tangkapan layar, atau output terminal rekaman. Gunakan folder `.deploy/` yang sudah diabaikan Git untuk berkas kunci dan hasil mentah.

## 3. Persiapan terminal dan deployment

**Penanggung jawab: Adha.** Razi menyiapkan runner dan kredensial client; Dicky menyiapkan recorder, data, dan bukti baseline. Seluruh perubahan cluster pada bagian ini dijalankan Adha.

Semua blok `bash` dijalankan dari root repo `atlas` melalui Bash/WSL. Blok ini tidak memakai sintaks PowerShell. Pastikan kubectl, Docker, dan alias SSH dapat diakses dari shell tersebut. Ganti placeholder sebelum menjalankan perintah. Untuk terminal baru, muat kembali variabel yang diperlukan.

```bash
set +x
set -o pipefail
export NS=sistem-informasi-kebencanaan-bnpb
export KUBE_CONTEXT='<context-cluster>'
export PRIVATE_DIR='.deploy/demo-private'
export RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
export EVIDENCE="$PRIVATE_DIR/evidence/$RUN_ID"
mkdir -p "$EVIDENCE"
kubectl config use-context "$KUBE_CONTEXT"
git rev-parse HEAD | tee "$EVIDENCE/commit.txt"
git rev-parse 'milestone-2^{commit}' | tee "$EVIDENCE/tag-commit.txt"
kubectl get nodes -o wide | tee "$EVIDENCE/nodes-before.txt"
```

### 3.1 Environment dan satu perintah deploy

Pada persiapan pertama, deploy dapat membuat `.env.m2` dengan kredensial acak. Siapkan berkas tersebut sebelum rekaman. Tambahkan atau perbarui baris berikut melalui editor lokal, menggunakan token dari email:

```dotenv
SATELLITE_TOKEN=<token-demo-dari-email>
```

Jangan mengganti `PANTAI_AUTH_TOKEN` dengan kunci ujian sensor. Jangan menampilkan isi `.env.m2` saat merekam.

```bash
set -a
source .env.m2
set +a

make deploy-demo DEPLOY_ARGS="--context $KUBE_CONTEXT --load-vms <ssh-node-a> <ssh-node-b> <ssh-node-c> --tag milestone-2"
```

Perintah ini membangun dan mengimpor image, men-deploy overlay demo, mengaktifkan ingestion dan telemetry, lalu menunggu workload. Ganti ketiga alias SSH di atas. Jika environment Secret diubah setelah pod berjalan, restart workload terkait agar nilai baru dibaca, lalu tunggu readiness. Contoh setelah menambahkan token satelit:

```bash
kubectl -n "$NS" rollout restart deployment/aggregator
kubectl -n "$NS" rollout status deployment/aggregator --timeout=5m
kubectl -n "$NS" get pods -o wide | tee "$EVIDENCE/pods-before.txt"
kubectl -n "$NS" get deployment -o wide | tee "$EVIDENCE/deployments.txt"
```

### 3.2 Simpan kunci ujian tanpa menampilkannya

Jalankan di luar rekaman. Input `read -s` tidak ditampilkan pada terminal.

```bash
umask 077
read -rsp 'Kunci Sensor Pantai dari email: ' PANTAI_EXAM_KEY
printf '\n'
printf '%s\n' "$PANTAI_EXAM_KEY" > "$PRIVATE_DIR/pantai.key"
unset PANTAI_EXAM_KEY
read -rsp 'Kunci Tester Agent dari email: ' TESTER_EXAM_KEY
printf '\n'
printf '%s\n' "$TESTER_EXAM_KEY" > "$PRIVATE_DIR/tester.key"
unset TESTER_EXAM_KEY

kubectl -n "$NS" create secret generic pantai-key \
  --from-file=key="$PRIVATE_DIR/pantai.key" --dry-run=client -o yaml \
  | kubectl -n "$NS" apply -f -
kubectl create namespace tester-agent --dry-run=client -o yaml | kubectl apply -f -
kubectl -n tester-agent create secret generic tester-key \
  --from-file=key="$PRIVATE_DIR/tester.key" --dry-run=client -o yaml \
  | kubectl -n tester-agent apply -f -
```

### 3.3 Aktifkan profil ujian Sensor Pantai

Manifest vendor repo masih memakai `practice` dan mount kunci ujian masih dikomentari. Setelah deploy selesai, terapkan patch operasional berikut. Patch ini menggabungkan environment dan volume yang diperlukan tanpa mengganti token HTTP atau volume data sensor.

```bash
cat > "$PRIVATE_DIR/sensor-exam.patch.json" <<'JSON'
{
  "spec": {
    "template": {
      "spec": {
        "containers": [{
          "name": "sensor",
          "image": "sustuna/sensor-pantai@sha256:51fbd90d3891ad6e9f5768ad330ffb65205644cb29e50da65c7538f2e0b4397d",
          "env": [
            {"name": "PANTAI_PROFILE", "value": "exam"},
            {"name": "PANTAI_SEED", "value": "42"}
          ],
          "volumeMounts": [{"name": "key", "mountPath": "/var/run/pantai", "readOnly": true}]
        }],
        "volumes": [{"name": "key", "secret": {"secretName": "pantai-key"}}]
      }
    }
  }
}
JSON
kubectl -n "$NS" patch deployment sensor-pantai --type=strategic \
  --patch-file "$PRIVATE_DIR/sensor-exam.patch.json"
kubectl -n "$NS" rollout status deployment/sensor-pantai --timeout=5m
```

Periksa kembali profil, seed, dan mount setelah deploy ulang; manifest dasar dapat mengembalikan profil ke `practice`. Sensor memakai `emptyDir`, sehingga pergantian pod juga perlu diperhitungkan ketika menentukan awal pengukuran. Profil sensor dapat mulai menghasilkan data sebelum P5; mulailah pencatatan notifier sebelum jendela yang akan dihitung.

### 3.4 Pasang dan verifikasi Tester Agent

Gunakan [manifest resmi yang dirujuk README](https://raw.githubusercontent.com/susTuna/if4031-tester-agent/ebb55261711123a9a211739ffbf140a275caa6da/manifest.yaml). Digest di manifest ini cocok dengan email. Pertahankan RBAC, label, mount, dan konfigurasi targetnya.

```bash
curl -fsSL \
  https://raw.githubusercontent.com/susTuna/if4031-tester-agent/ebb55261711123a9a211739ffbf140a275caa6da/manifest.yaml \
  -o "$PRIVATE_DIR/tester-manifest.yaml"
grep -F 'sustuna/tester-agent@sha256:b3143da159cf23a8744ede12b847da6208353754234d53a5a7b46d071cf277ec' \
  "$PRIVATE_DIR/tester-manifest.yaml"
kubectl apply -f "$PRIVATE_DIR/tester-manifest.yaml"
kubectl -n tester-agent rollout status daemonset/tester-agent --timeout=5m
kubectl -n tester-agent get pods -o wide | tee "$EVIDENCE/tester-pods.txt"
kubectl exec -n tester-agent daemonset/tester-agent -- /app list \
  | tee "$EVIDENCE/tester-scenarios.txt"
```

Pastikan jumlah pod Ready sama dengan jumlah node yang ditargetkan dan ada satu pod pada control-plane. Skenario latihan yang didokumentasikan: `kill-store`, `wipe-node`, `clock-skew`, `partition`, `kill-leader`, dan `schema`.

### 3.5 Port forward dan pemeriksaan awal

Jalankan pada terminal tersendiri. Setelah pod target mati atau berganti, port forward mungkin perlu dijalankan ulang; catat gangguan tunnel secara terpisah dari kegagalan service.

```bash
kubectl -n "$NS" port-forward svc/client-api 8080:8080 &
kubectl -n "$NS" port-forward svc/auth 8084:8084 &
kubectl -n "$NS" port-forward svc/aggregator 8083:8083 &
kubectl -n "$NS" port-forward svc/sensor-pantai 18080:8080 &
kubectl -n "$NS" port-forward svc/victoriametrics 8428:8428 &
kubectl -n "$NS" port-forward svc/jaeger 16686:16686 &
```

```bash
export CLIENT_URL=http://localhost:8080
export AUTH_URL=http://localhost:8084
export AUDIT_URL=http://localhost:8083
export SENSOR_URL=http://localhost:18080
curl -fsS "$CLIENT_URL/health"
curl -fsS "$SENSOR_URL/healthz"
```

Sensor sehat menjawab `{"status":"ok"}`. Buka analitik di `http://localhost:8083/internal/analytics`, metrik di `http://localhost:8428/vmui`, dan trace di `http://localhost:16686`. Request data Audit tetap membutuhkan `AUDIT_TOKEN`.

Helper login berikut mengambil kredensial dari environment, tanpa mencetak token ke layar. Panggil lagi sebelum request jika token sudah kedaluwarsa; k6 menangani refresh melalui `loadtest/session.js`.

```bash
login() {
  python - "$1" <<'PY' | curl -fsS "$AUTH_URL/auth/login" \
    -H 'Content-Type: application/json' --data-binary @- | jq -er '.access_token'
import json, os, sys
prefix = sys.argv[1]
print(json.dumps({"client_id": os.environ[prefix + "_CLIENT_ID"],
                  "client_secret": os.environ[prefix + "_CLIENT_SECRET"]}))
PY
}
MEDIA_TOKEN="$(login MEDIA)"
curl -fsS -H "Authorization: Bearer $MEDIA_TOKEN" \
  -H "X-Correlation-ID: demo-$RUN_ID-health" \
  -w '\nhttp_code=%{http_code} time_total=%{time_total}s\n' \
  "$CLIENT_URL/hazards?limit=20"
```

## 4. Dataset resmi P1: verifikasi dan pemuatan

**Penanggung jawab: Dicky.** Dicky menyiapkan lampiran, konversi, checksum, dan validasi hasil. Adha mengoordinasikan ingestion serta mengeksekusi bulk-load ke cluster.

Simpan lampiran email sebagai `.deploy/demo-private/seismic_events.jsonl.gz`. Berkas ini belum disertakan dalam pesan email yang ditempel ke percakapan; operator perlu mengambil lampiran aslinya.

```bash
export DATASET="$PRIVATE_DIR/seismic_events.jsonl.gz"
python - <<'PY' | tee "$EVIDENCE/dataset-verification.txt"
import gzip, hashlib, json, os
with gzip.open(os.environ["DATASET"], "rt", encoding="utf-8") as stream:
    ids = [json.loads(line)["event_id"] for line in stream if line.strip()]
if not all(isinstance(value, str) and value for value in ids):
    raise SystemExit("event_id harus berupa string yang tidak kosong")
actual = hashlib.sha256(b"\n".join(sorted(value.encode("utf-8") for value in ids))).hexdigest()
expected = "8600358903602dad850c6a5bd96b670336ad0d4ffd9a69932dea25c58b344f0f"
print(json.dumps({"records": len(ids), "unique_ids": len(set(ids)), "sha256": actual}, indent=2))
if len(ids) != 25001 or len(set(ids)) != 25001 or actual != expected:
    raise SystemExit("Dataset tidak cocok dengan jumlah/checksum pada email")
PY
```

Hash dihitung dari `event_id` yang diurutkan berdasarkan byte UTF-8, dipisahkan satu newline, tanpa newline tambahan di akhir; ini mengikuti cara checksum Atlas di `audit.go`. Jangan memakai checksum berkas gzip sebagai penggantinya.

**Checkpoint pemuatan yang harus diselesaikan sebelum rekaman:** lampiran berisi `SeismicEvent`, sedangkan `data-tool -mode load` menerima JSONL `HazardEvent` kanonik. Repo belum menyediakan perintah konversi lampiran ini secara langsung. Gunakan pemetaan yang sama dengan `MapSeismic` di [mapping.go](services/bnpb/aggregator/internal/aggregator/mapping.go), pertahankan `event_id` sebagai `source_ref_id`, dan gunakan `CanonicalHazardID` untuk identitasnya. Siapkan serta periksa hasil konversi sebagai `$PRIVATE_DIR/seismic-hazards.jsonl`; jangan memasukkan JSONL sumber langsung ke loader.

Image Aggregator sudah menyertakan `data-tool`. Setelah konversi benar, dalam fase persiapan bulk-load yang terkoordinasi dengan pemegang ingestion BMKG, gunakan konfigurasi cluster dan writer epoch yang sedang berlaku:

```bash
AGG_POD="$(kubectl -n "$NS" get pod -l app=aggregator -o jsonpath='{.items[0].metadata.name}')"
BMKG_EPOCH="$(kubectl -n "$NS" get configmap canonical-placement -o json \
  | jq -er '.data["placement.json"] | fromjson | .writer_epochs.BMKG')"
kubectl -n "$NS" exec -i "$AGG_POD" -- data-tool \
  -mode load -config /etc/atlas/data/config.json -epoch "$BMKG_EPOCH" -input - \
  < "$PRIVATE_DIR/seismic-hazards.jsonl" | tee "$EVIDENCE/dataset-load.json"
kubectl -n "$NS" exec "$AGG_POD" -- data-tool \
  -mode metrics -config /etc/atlas/data/config.json | tee "$EVIDENCE/data-node-metrics.json"
```

Jangan mengubah epoch untuk memaksa loader lolos. Jika epoch berubah atau loader ditolak, koordinasikan pemuatan kembali dengan operator ingestion. Bandingkan himpunan ID dataset terhadap hasil Audit sesudah pemuatan. Jika BMKG juga mempunyai seed bawaan atau event live, checksum seluruh `source=BMKG` berbeda dari checksum dataset saja; laporkan jumlah tambahan tersebut dan hitung pembanding khusus 25.001 ID dataset. Checksum sebelum/sesudah gangguan juga harus memakai himpunan data atau cutoff yang sama agar event baru tidak terlihat sebagai kehilangan data.

## 5. Mulai pencatatan dan sesi rekaman

**Pembagian:** Dicky menyalakan recorder dan menampilkan log; Razi memulai notifier capture dan menyimpan PID/waktu pengukuran; Adha memulai sesi satelit setelah recorder berjalan.

Sebelum gangguan dan sebelum jendela pengukuran P5, mulai pengambilan log notifier. Helper ini menyambung kembali setelah pergantian pod.

```bash
scripts/notifier-log.sh "$EVIDENCE/notifier.log" &
NOTIFIER_CAPTURE_PID=$!
```

Pada terminal yang terlihat di video, tampilkan seluruh log Tester Agent. Jika lebih dari lima pod ditargetkan, naikkan `--max-log-requests` sesuai jumlah pod.

```bash
kubectl logs -n tester-agent -l app=tester-agent --all-containers=true \
  --prefix=true --max-log-requests=10 --since=5m -f \
  | tee "$EVIDENCE/tester-live.log"
```

**Tepat saat mulai merekam, jalankan start satelit sekali.** Feed kemudian berjalan dari offset 0 dengan percepatan 60x selama bagian video berikutnya. Jangan menjalankan start ketika hanya latihan atau memeriksa kesiapan.

```bash
curl --fail-with-body -sS -X POST \
  -H "Authorization: Bearer $SATELLITE_TOKEN" \
  https://satellite.naufarrel.dev/v1/exam/start \
  | tee "$EVIDENCE/satellite-exam-start.json"
```

Panggilan kedua tidak membuat sesi baru. Pastikan Aggregator terhubung ke feed ujian menggunakan token tersebut; tampilkan kemajuan offset dan lag sejak awal. Untuk memperlihatkan SSE secara langsung, buka terminal tambahan dan hentikan dengan Ctrl+C setelah bukti cukup:

```bash
curl -N -H "Authorization: Bearer $SATELLITE_TOKEN" \
  https://satellite.naufarrel.dev/v1/stream
```

Jalankan setiap skenario ujian secara terpisah, tunggu selesai dan kumpulkan bukti sebelum berpindah problem. Nama `exam p1`, `p2`, `p3`, `p5` mengikuti email; perintah `practice` pada tutorial dipakai saat latihan.

## 6. P1 - Penyimpanan terdistribusi dan pemulihan node

**Narator utama: Dicky.** Adha menjalankan `exam p1` dan recovery node. Razi menampilkan pembacaan client beserta latensinya. Dicky mengumpulkan bukti database, Audit, dan rekaman.

**Narasi:** event dibagi ke shard dan disimpan pada dua node berbeda. Baca menggunakan salinan yang masih tersedia; hasil yang tidak lengkap harus diberi penanda. Refill memulihkan salinan ketika node kembali.

1. Tampilkan tiga node, sebaran shard, jumlah record per node, dan lokasi salinan beberapa event dataset.
2. Tampilkan pagination lintas shard dan dashboard analitik, lalu bandingkan total dengan Audit pada cutoff yang sama.
3. Ambil baseline, jalankan ujian, dan terus lakukan pembacaan selama gangguan.

```bash
curl -fsS -H "Authorization: Bearer $AUDIT_TOKEN" \
  "$AUDIT_URL/internal/audit/checksum?source=BMKG" \
  | tee "$EVIDENCE/p1-before.json"
HAZARD_ID='<hazard_id-dari-Audit-events>'
curl -fsS -G -H "Authorization: Bearer $AUDIT_TOKEN" \
  --data-urlencode "hazard_id=$HAZARD_ID" \
  "$AUDIT_URL/internal/audit/location" | tee "$EVIDENCE/p1-location.json"

kubectl exec -n tester-agent daemonset/tester-agent -- /app run exam p1
```

4. Tunjukkan HTTP status, `time_total`, `partial`, dan unit yang tidak tersedia. Untuk pengamatan panjang gunakan k6 yang memperbarui token, melalui jalur layanan langsung.
5. Tampilkan pemulihan node, progres refill, dan kembalinya dua salinan pada node berbeda. Verifikasi checksum/himpunan ID serta baca ulang dataset.

```bash
kubectl get nodes -o wide | tee "$EVIDENCE/p1-nodes-after.txt"
curl -fsS -H "Authorization: Bearer $AUDIT_TOKEN" \
  "$AUDIT_URL/internal/audit/checksum?source=BMKG" \
  | tee "$EVIDENCE/p1-after.json"
```

Target: tidak ada event dataset hilang; salinan kembali lengkap; pembacaan lengkap atau ditandai sebagian. Ukur waktu gangguan dan pemulihan. Untuk kriteria kehilangan node kedua, lakukan hanya setelah refill pertama sudah lengkap dan kumpulkan bukti terpisah.

**Catatan pemulihan `wipe-node`:** README mencatat run `go` yang masih aktif di ConfigMap `tester-run` dapat mematikan VM lagi saat dinyalakan. Tunggu run ujian benar-benar selesai dan periksa ConfigMap sebelum menyalakan VM. Pada latihan, README menyarankan mengganti run dengan `practice schema`; jangan mengganti run ujian yang masih berjalan dengan skenario latihan.

## 7. P2 - Leadership, fencing, scaling, dan rollout

**Narator utama: Razi.** Razi menjalankan k6 dan menganalisis hasil. Adha menjalankan `exam p2`, scaling, rollout, serta freeze/resume atas aba-aba Razi. Dicky membantu bukti data dan perekaman.

**Narasi:** satu pemegang lease melakukan polling setiap sumber. Writer epoch menolak penulis basi meskipun jam bergeser atau instance lama hidup kembali. Penambahan instance memperbesar kapasitas baca tanpa menggandakan ingestion.

```bash
kubectl -n "$NS" get leases | tee "$EVIDENCE/p2-leases-before.txt"
kubectl exec -n tester-agent daemonset/tester-agent -- /app run exam p2
```

Tampilkan pemegang lease lama/baru, waktu sampai penulisan pertama oleh pengganti, source poll, epoch, dan bukti ketiadaan duplikasi. Untuk freeze manual yang diperlukan kriteria, bekukan VM worker pemegang lease melalui Proxmox lebih lama dari TTL lease, kemudian resume. Catat tindakan dan waktu. Bedakan bukti "tidak terjadi penulis ganda" dari bukti penolakan tulis dengan epoch basi; angka nol percobaan tidak membuktikan jalur penolakan sudah terpicu.

### 7.1 Siapkan runner beban

Jalankan k6 pada mesin dengan kapasitas memadai dan akses langsung ke layanan. Catat CPU/RAM runner, rute jaringan, replika, image, bentuk query, dan TTL cache. Menjalankan 10.000 VU pada control-plane Atlas yang terbatas dapat menghabiskan resource bersama. Port forward cocok untuk inspeksi, tetapi dapat membatasi pengukuran throughput.

Pada runner, sediakan `loadtest/m2.js` dan `loadtest/session.js`, muat kredensial melalui environment lokal, lalu set alamat Client API/Auth yang dapat dijangkau. ClusterIP hanya bisa digunakan jika runner memiliki rute ke jaringan service.

```bash
export BASE_URL='http://<alamat-client-api-yang-dijangkau-runner>:8080'
export AUTH_URL='http://<alamat-auth-yang-dijangkau-runner>:8084'
```

Jika asisten memberikan skrip beban khusus, jalankan skrip tersebut sesuai instruksi dan simpan hasilnya; perintah berikut memakai skrip bawaan Atlas. Ubah alamat kembali ke port forward ketika kembali menggunakan helper login lokal.

### 7.2 Beban 10.000 VU dan scaling 3 -> 5 -> 2

Atur 3 instance dan tunggu Ready. Pada runner, mulai beban dengan fase stabil 150 detik setelah ramp-up 30 detik:

```bash
kubectl -n "$NS" scale deployment/aggregator --replicas=3
kubectl -n "$NS" rollout status deployment/aggregator --timeout=5m
# Jalankan pada runner, dengan EVIDENCE menunjuk folder output lokalnya.
k6 run -e SCENARIO=sustained -e VUS=10000 -e DURATION=150s \
  -e SUMMARY_JSON="$EVIDENCE/p2-sustained.json" loadtest/m2.js \
  | tee "$EVIDENCE/p2-sustained.log"
```

Saat beban mulai, pada terminal operator cluster jalankan `loadtest/scale.sh scale`. Default-nya mengubah 3 menjadi 5 setelah 40 detik, kemudian menjadi 2 setelah 40 detik berikutnya. Simpan output dan amati pod, latensi, error, serta memori telemetry.

Target: 10.000 VU bertahan minimal 120 detik dan unexpected error di bawah 1%. Catat 429, partial, kegagalan Auth, dan kegagalan request secara terpisah.

### 7.3 Throughput 1 instance dibanding 3

Untuk masing-masing jumlah replika `1` lalu `3`: scale pada operator, tunggu rollout selesai, kemudian jalankan perintah yang sama pada runner. Ganti `REPLICAS` sesuai run.

```bash
# Operator: set REPLICAS=1, kemudian ulangi seluruh run dengan REPLICAS=3.
REPLICAS=1
kubectl -n "$NS" scale deployment/aggregator --replicas="$REPLICAS"
kubectl -n "$NS" rollout status deployment/aggregator --timeout=5m
# Runner: set nilai REPLICAS yang sama dengan operator.
k6 run -e SCENARIO=throughput -e VUS=500 -e DURATION=150s -e THINK_TIME=0 \
  -e SUMMARY_JSON="$EVIDENCE/p2-throughput-$REPLICAS.json" loadtest/m2.js \
  | tee "$EVIDENCE/p2-throughput-$REPLICAS.log"
```

Hitung rasio dari **complete 200/s** pada summary, bukan total `http_reqs` yang juga menghitung login dan 429. Target 3/1 minimal 2,0. Gunakan mesin, query, `VUS`, `DURATION`, `THINK_TIME`, dan kebijakan cache yang sama. Cache halaman pertama default 1 detik; nyatakan pengaruhnya. Jika target gagal, tampilkan hasil dan jelaskan bottleneck yang terlihat pada metrik.

### 7.4 Rolling update tanpa request gagal

Siapkan tag pengganti dari kode yang dinilai dan impor image ke semua node sebelum run. Contoh build/import ada di README bagian M2 P2. Pada runner:

```bash
k6 run -e SCENARIO=rollout -e RATE=200 -e DURATION=300s \
  -e SUMMARY_JSON="$EVIDENCE/p2-rollout.json" loadtest/m2.js \
  | tee "$EVIDENCE/p2-rollout.log"
```

Saat beban berjalan, operator menjalankan `loadtest/scale.sh rollout <tag-image-pengganti>`. Target: tidak ada request gagal, 429, atau dropped iteration. Setelah selesai, kembalikan Aggregator ke 3 replika dan tunggu Ready sebelum problem berikutnya.

## 8. P3 - Skema sumber, parking/DLQ, replay, dan laporan lapangan

**Narator utama: Adha.** Adha menjalankan `exam p3`, perubahan kebijakan, dan replay; Dicky menunjukkan tabel parking serta hasil recovered; Razi membantu request client, scope, dan laporan Field Team.

**Narasi:** perubahan opsional dapat diterima, tetapi perubahan yang mengubah makna diparkir dengan body, hash, alasan, retry policy, dan correlation ID. Internal Ops memutuskan perbaikan dan replay. Pada Atlas, istilah DLQ pada tugas ditunjukkan melalui penyimpanan pesan `unprocessed_messages` dan API Internal Ops.

```bash
kubectl exec -n tester-agent daemonset/tester-agent -- /app run exam p3
OPS_TOKEN="$(login INTERNAL_OPS)"
curl -fsS -H "Authorization: Bearer $OPS_TOKEN" \
  "$CLIENT_URL/internal-ops/unprocessed?source=PVMBG&limit=10" \
  | tee "$EVIDENCE/p3-unprocessed.json"
curl -fsS -H "Authorization: Bearer $AUDIT_TOKEN" \
  "$AUDIT_URL/internal/audit/unprocessed?source=PVMBG&limit=20" \
  | tee "$EVIDENCE/p3-audit-unprocessed.json"
```

Tampilkan `reason_code`, panjang dan SHA-256 body, `raw_cut`, status, attempts, serta correlation ID melalui Internal Ops/database. Body besar dapat disimpan dalam chunk; listing Audit hanya menampilkan ringkasan. Tunjukkan bahwa sumber lain tetap berjalan dan data gagal tidak dibuang.

Setelah tahap gangguan ujian selesai, demonstrasikan perbaikan kebijakan untuk perubahan yang benar-benar terjadi. Contoh berikut menerima penggantian nama ash dan string count; jangan mengaktifkannya sebelum bukti kegagalan terkumpul, dan pertahankan kebijakan lain yang masih diperlukan:

```bash
cat > "$PRIVATE_DIR/policy.json" <<'JSON'
{"accept_ash_rename":true,"accept_string_count":true}
JSON
kubectl -n "$NS" create configmap source-schema-policy \
  --from-file=policy.json="$PRIVATE_DIR/policy.json" --dry-run=client -o yaml \
  | kubectl -n "$NS" apply -f -
```

Tunggu file ConfigMap pada pod tersinkron; biasanya sekitar satu menit. Field wajib yang hilang memerlukan pemetaan `volcano_by_report` yang valid, bukan sekadar dua flag di atas. Login ulang, kemudian replay dengan limit eksplisit:

```bash
OPS_TOKEN="$(login INTERNAL_OPS)"
curl --fail-with-body -sS -X POST \
  -H "Authorization: Bearer $OPS_TOKEN" -H 'Content-Type: application/json' \
  -d '{"source":"PVMBG","reason_code":"schema_violation","limit":100}' \
  "$CLIENT_URL/internal-ops/replay" | tee "$EVIDENCE/p3-replay.json"
```

Tunjukkan pesan menjadi recovered dan event yang benar tersedia. Jika backlog melebihi satu batch, lanjutkan replay terukur dan periksa pagination. Jangan menimpa event yang lebih baru dengan replay lama. Uji akses Internal Ops dan penolakan client tanpa scope yang sesuai.

Untuk laporan lapangan, kirim payload valid dengan Field Team, ukur latensi respons 202, lalu verifikasi `report_id` pada inbox. Gunakan ID yang sama untuk pengiriman ulang dan pastikan tidak menambah laporan unik. Tampilkan antrean `field.reports`; Aggregator adalah konsumennya, bukan consumer-a/b. Periksa dampak Client API mati setelah 202, Aggregator mati ketika memproses, dan restart broker pada fase uji yang sesuai. Gunakan database client untuk menunjukkan isi inbox secara langsung, bukan hanya menghitung 202.

## 9. P4 - Feed satelit live dan pemulihan

**Narator utama: Adha.** Adha menunjukkan ingestion, checkpoint, lag, dan pemulihan; Dicky membantu ekspor, referensi, dan pemeriksaan data; Razi membantu dashboard dan analisis latensi.

**Narasi:** offset menjadi checkpoint, beberapa deteksi dapat membentuk satu kebakaran, late event diproses menurut watermark, update/retract memperbarui event yang sama, dan retry/resume tidak menggandakan hasil.

1. Tampilkan stream ujian yang sudah dimulai pada awal rekaman, source `SATELIT` di Audit, jumlah kebakaran, offset, dan lag.
2. Amati kecepatan 60x, waktu mengejar backlog, serta memori. Target catch-up sesuai spesifikasi; jangan menyebut tidak ada kehilangan hanya karena pod tetap Running.
3. Tunjukkan contoh penggabungan deteksi, late event, update/retract, lalu pemulihan setelah koneksi putus atau instance pemroses restart pada tahap uji yang sesuai.
4. Bandingkan hasil lengkap dengan referensi asisten pada window/offset yang setara. Jangan memakai satu halaman Audit sebagai seluruh dataset.

Ekspor seluruh halaman Audit SATELIT dengan script pembacaan berikut. Script menghentikan ekspor jika storage memberikan hasil partial atau cursor berulang:

```bash
python - <<'PY'
import json, os, urllib.parse, urllib.request
rows, seen, cursor = [], set(), None
while True:
    query = {"source": "SATELIT", "limit": 1000}
    if cursor:
        query["cursor"] = cursor
    url = os.environ["AUDIT_URL"] + "/internal/audit/events?" + urllib.parse.urlencode(query)
    request = urllib.request.Request(url, headers={"Authorization": "Bearer " + os.environ["AUDIT_TOKEN"]})
    with urllib.request.urlopen(request, timeout=60) as response:
        page = json.load(response)
    if page.get("partial"):
        raise SystemExit("Ekspor partial; pulihkan storage sebelum membandingkan referensi")
    rows.extend(page["items"])
    cursor = page.get("next_cursor")
    if not cursor:
        break
    if cursor in seen:
        raise SystemExit("Cursor berulang")
    seen.add(cursor)
path = os.path.join(os.environ["EVIDENCE"], "p4-satellite.json")
with open(path, "w", encoding="utf-8") as output:
    json.dump(rows, output, ensure_ascii=False)
print("Exported", len(rows), "events to", path)
PY
python scripts/satellite-reference-diff.py \
  --actual "$EVIDENCE/p4-satellite.json" \
  --reference "$PRIVATE_DIR/reference.json" \
  --output "$EVIDENCE/p4-reference-diff.json"
```

Perintah diff memerlukan berkas referensi asli; email yang ditempel belum menyertakannya. Ekspor dengan pagination di atas tidak menjamin snapshot atomik selama feed terus mengubah data. Catat window/offset, gunakan pembanding yang konsisten, dan jelaskan perbedaan timing. Sajikan missing/extra groups, perbedaan field, dan identitas; jelaskan selisih yang tersisa. Skrip diff membatasi masing-masing input 64 MiB.

## 10. P5 - Sensor Pantai, overload, dan game day

**Narator utama: Razi.** Adha menjalankan `exam p5` lalu memantau tanpa intervensi selama game day. Razi menjalankan overload, mengikuti notifier, dan menghitung error budget. Dicky menampilkan penyimpanan/outbox dan merekam hasil.

**Narasi:** jalur sensor diisolasi dengan timeout, bulkhead, dan circuit breaker. Event kritis dikirim lewat broker ke notifier. Admission memberi prioritas Field Team ketika Media membanjiri layanan.

Catat awal jendela pengukuran dalam UTC. Pastikan notifier capture sudah berjalan dan data sensor pada jendela ini tetap tersedia sampai perhitungan selesai.

```bash
export GAME_START="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf '%s\n' "$GAME_START" > "$EVIDENCE/p5-start.txt"
kubectl exec -n tester-agent daemonset/tester-agent -- /app run exam p5
```

Tampilkan event kritis sampai `field_alert_delivered`, correlation ID, latensi, dan perubahan antrean/outbox. Selama sensor bermasalah, tampilkan laju request sensor, keadaan circuit breaker, waktu pulih, dan latensi BMKG. Data yang melanggar kontrak dasar harus diparkir dengan alasan; pisahkan dari event kritis valid yang wajib dikirim.

Pada runner, uji overload Media sambil Field Team tetap membaca:

```bash
k6 run -e SCENARIO=overload -e DURATION=150s \
  -e SUMMARY_JSON="$EVIDENCE/p5-overload.json" loadtest/m2.js \
  | tee "$EVIDENCE/p5-overload.log"
```

Runner harus mempunyai kredensial Media dan Field Team dari environment. Tampilkan 429/error per kelas, bukan hanya persentase gabungan. Untuk game day, biarkan pemulihan otomatis berlangsung tanpa intervensi operator selama window yang ditetapkan. Catat kegagalan, waktu pulih, dan bukti metrik/trace yang menjelaskan penyebabnya.

Setelah gangguan berakhir dan backlog notifikasi selesai, ambil perhitungan saat sensor ujian dan histori yang sama masih tersedia. Jalankan pada terminal operator yang mempunyai `GAME_START`, `SENSOR_URL`, dan `PANTAI_AUTH_TOKEN`:

```bash
kill "$NOTIFIER_CAPTURE_PID"
wait "$NOTIFIER_CAPTURE_PID" 2>/dev/null || true
python scripts/error-budget.py --sensor "$SENSOR_URL" --since "$GAME_START" \
  < "$EVIDENCE/notifier.log" | tee "$EVIDENCE/p5-error-budget.txt"
```

Target: nol event kritis valid hilang dan 99% dikirim dalam 5 detik. Skrip mengambil event yang diharapkan dari Sensor Pantai, sehingga kehilangan sebelum penyimpanan Atlas tetap dihitung. Jelaskan perbedaan latensi dari ingestion ke delivery dan dari `observed_at` ke delivery. Laporkan jumlah sampel; jendela tanpa event kritis tidak membuktikan target. Perhitungan membaca histori sampai waktu dijalankan, jadi catat juga waktu selesai pengambilan dan jangan mengganti pod sensor sebelum laporan dibuat.

## 11. Bukti yang harus terlihat dan disimpan

| Problem | Bukti minimum |
| --- | --- |
| P1 | Dataset resmi dan hash, jumlah per node/shard, dua lokasi salinan, pagination, pembacaan saat gangguan, checksum/himpunan ID sebelum-sesudah, refill, dan analitik. |
| P2 | Lease holder, failover time, epoch dan penolakan penulis basi, log sumber tanpa duplikasi, 10.000 VU, scaling 3/5/2, rasio throughput 3/1, rollout, serta penggunaan resource. |
| P3 | Body/hash/alasan pesan gagal, retry/parking, perubahan kebijakan, replay, scope akses, broker/inbox laporan lapangan, dan deduplikasi report ID. |
| P4 | Stream live, offset/lag, pengelompokan dan koreksi kebakaran, catch-up 60x, resume, ekspor lengkap, dan reference diff. |
| P5 | Profil exam sehat sebelum gangguan, alur event kritis ke notifier, breaker dan latensi BMKG, hasil per kelas client, game day tanpa intervensi, error budget, dan penjelasan insiden. |
| Semua | Request dengan HTTP status dan `time_total`, dashboard/metrik, database client, log Tester Agent, correlation ID, commit/image, topologi, dan semua anggota menjelaskan. |

Untuk mengambil bukti database tanpa mencetak password, buka `psql` pada pod Canonical Store menggunakan environment container dan tunjukkan schema yang relevan:

```bash
kubectl -n "$NS" exec -it canonical-store-0 -- sh -c \
  'psql -U "$POSTGRES_USER" -d canonical'
```

Gunakan `\dn`, `\dt *.*`, dan query terarah pada schema shard yang benar. Tunjukkan `hazard_events`, `unprocessed_messages`, atau `field_reports_inbox` sesuai bukti. Catat bahwa dua salinan fisik bukan dua event unik; jangan menjumlahkan seluruh replica sebagai total dataset logis.

## 12. Pemeriksaan video dan pengiriman

**Penanggung jawab pengiriman: Razi.** Dicky memeriksa kelengkapan dan keterbacaan video final; Adha memeriksa akses URL serta bukti `received_at` bersama Razi.

- [ ] Urutan tiga bagian wajib terpenuhi; semua anggota tampil dan menjelaskan.
- [ ] Build/deploy serta kesiapan service terlihat.
- [ ] P1-P5 direkam live, dengan log Tester Agent terlihat saat ujian.
- [ ] Hasil aktual dan batasannya dinyatakan; tidak ada klaim lulus dari uji yang belum dijalankan.
- [ ] Terminal, kode, dashboard, dan database terbaca; suara jelas.
- [ ] Token, kunci, password, isi Secret, dan `.env.m2` tidak terlihat.
- [ ] Video di YouTube public/unlisted, tanpa shortener, dengan judul dan deskripsi sesuai tutorial.
- [ ] Tautan dapat dibuka oleh penilai dan pengiriman dilakukan sebelum 29 November 2026 pukul 23.59 WIB.

Jalankan pengiriman setelah URL final tersedia, menggunakan token kelompok dari email. Input URL di bawah tidak mengandung token:

```bash
read -rp 'URL video YouTube: ' VIDEO_URL
VIDEO_BODY="$(python - "$VIDEO_URL" <<'PY'
import json, sys
print(json.dumps({"url": sys.argv[1]}))
PY
)"
curl --fail-with-body -sS -X POST \
  -H "Authorization: Bearer $SATELLITE_TOKEN" \
  -H 'Content-Type: application/json' --data-binary "$VIDEO_BODY" \
  https://satellite.naufarrel.dev/v1/video \
  | tee "$EVIDENCE/video-submission.json"
curl --fail-with-body -sS -H "Authorization: Bearer $SATELLITE_TOKEN" \
  https://satellite.naufarrel.dev/v1/video \
  | tee "$EVIDENCE/video-submission-check.json"
```

Pastikan respons POST HTTP 200 memuat `received_at` dan GET menampilkan URL yang benar. Simpan bukti pengiriman. Pengiriman ulang sebelum tenggat menggantikan kiriman sebelumnya; gunakan URL video final yang memang hendak dinilai.

## 13. Penanganan kendala yang sudah diketahui

| Gejala | Pemeriksaan atau tindak lanjut |
| --- | --- |
| `satellite_disabled` | Pastikan `.env.m2` mempunyai `SATELLITE_TOKEN`, deploy meneruskannya ke Secret, dan pod sudah restart setelah perubahan. Periksa keberadaan key tanpa mencetak nilainya. |
| Sensor belum berjalan dalam exam | Periksa profil, seed, image, Secret `pantai-key`, serta mount `/var/run/pantai/key`; ulangi patch setelah deploy jika profil kembali ke practice. |
| Tester tidak bisa menjalankan exam | Periksa digest image, Secret `tester-key` dengan data bernama `key`, kesiapan semua pod, dan log agent. |
| VM langsung mati lagi setelah wipe | Periksa status run `tester-run` dan tunggu run selesai sebelum recovery; jangan memutus skenario ujian yang aktif. |
| Request inspeksi gagal setelah pod mati | Periksa tunnel port forward dan jalankan ulang bila perlu; ulangi pengukuran melalui endpoint service langsung. |
| HTTP 401 pada request manual | Login ulang untuk kelas client yang benar. Jangan menampilkan respons login yang berisi token pada rekaman. |
| P3 belum recovered setelah replay | Periksa policy yang sesuai perubahan, sinkronisasi file ConfigMap, pemegang lease, limit batch, dan antrean replay. |
| Checksum P1 berbeda meski tidak hilang | Pisahkan dataset resmi dari seed/live event; bandingkan himpunan ID atau cutoff yang sama, periksa `partial` dan unit tidak tersedia. |
| Rasio throughput di bawah 2x | Periksa 429, Auth, CPU/RAM runner dan node, sebaran Client API, koneksi ke Aggregator, serta cache; laporkan hasil aktual. |
| Error budget menunjukkan nol sampel | Periksa window `observed_at`, profil sensor, token HTTP, dan histori; jalankan jendela yang benar-benar menghasilkan event kritis. |

`make demo` menjalankan pemeriksaan Compose untuk M1; perintah itu berguna untuk latihan lokal, tetapi bukti rekaman M2 memakai cluster dan skenario ujian di atas.
