# Aset demo M2 Atlas

Panduan per anggota dan urutan rekaman ada di [demo.md](../../demo.md). Folder ini menyimpan patch, snapshot manifest resmi, payload, request manual, fixture sintetis, dan helper yang dapat dibagikan. Nilai token, kunci ujian, dataset asli, dan backup environment disimpan pada mesin operator di luar Git.

Untuk menyalakan VM, memasang dependency OS, bootstrap k3s melalui SSH, dan menjalankan script demo per tahap, ikuti [QUICKSTART](QUICKSTART.md). Mulai dengan `python demo-assets/m2/cluster.py plan` dan `python demo-assets/m2/demo.py status`; keduanya tidak menghubungi cluster.

Shortcut operator M2 tersedia sebagai `make p1`, `make p2`, `make p3`, `make p4`, dan `make p5`. Tambahkan `RUN_ID=rekaman-01` agar evidence konsisten atau `DRY_RUN=1` untuk memeriksa alurnya tanpa infra. P1/P2/P3/P5 memicu satu skenario Tester Agent, sedangkan P4 mengekspor Audit SATELIT; start satelit tetap perintah terpisah. Detail ada pada bagian shortcut Make di QUICKSTART.

## Pemeriksaan tanpa cluster

Jalankan dari root repo dengan Python 3 dan kubectl tersedia pada PATH. Pasang parser YAML ke folder lokal yang diabaikan Git:

```bash
python -m pip install --target .deploy/demo-private/python-deps -r demo-assets/m2/requirements.txt
python demo-assets/m2/check-assets-local.py
```

Pemeriksaan merender overlay dasar dan overlay ujian dengan `kubectl kustomize`, memeriksa digest/mount/replica/ingestion/telemetry, serta menjalankan pembandingan referensi dengan fixture sintetis yang cocok dan berbeda. Perintah ini tidak menghubungi API cluster atau memulai sesi satelit. Render dan hasil pemeriksaan ditulis ke `.deploy/demo-private/`.

Untuk memeriksa kembali build dan unit test kode aplikasi setelah perubahan:

```bash
python demo-assets/m2/check-go-local.py
```

Go memilih toolchain yang diminta modul, saat ini 1.27.4. Helper membangun dan menguji sembilan modul runtime tanpa tag integrasi. PostgreSQL, RabbitMQ, dan cluster tidak diperlukan untuk suite ini. Log ditulis ke `.deploy/demo-private/checks/go/`; hasil ringkas ke `go-results.json` pada folder checks.

`prepare-local.py` dapat digunakan untuk meregenerasi aset aman dan mengunduh ulang snapshot manifest Tester Agent dari commit resmi yang dipin. Perintah tersebut memerlukan internet, tidak membuat kredensial, dan tidak menjalankan kubectl terhadap cluster:

```bash
python demo-assets/m2/prepare-local.py
```

## Kredensial lokal

Isi `.env.m2` melalui alur deploy Atlas dan tambahkan `SATELLITE_TOKEN` dari email. Simpan kunci Sensor Pantai dan Tester Agent sebagai `.deploy/demo-private/pantai.key` serta `tester.key`, masing-masing satu baris tanpa BOM. Pertahankan token HTTP `PANTAI_AUTH_TOKEN` sebagai nilai berbeda dari kunci ujian. Jangan menyalin isi kunci ke template yang akan di-commit.

Setelah kredensial lokal diisi, pemeriksaan berikut memvalidasi keberadaan nilai wajib, format berkas kunci, dan aturan Git ignore tanpa mencetak nilainya:

```bash
python demo-assets/m2/check-assets-local.py --check-private
```

Template Secret hanya menunjukkan nama namespace/key. Buat Secret aktual dari berkas kunci menggunakan langkah pada demo.md ketika cluster tersedia. Overlay ujian di folder ini adalah bahan review/render; `make deploy-demo` tetap diperlukan untuk bootstrap Secret aplikasi, placement, dan konfigurasi data.

## Isi paket

| Path | Kegunaan |
| --- | --- |
| `sensor-exam.patch.json` | Patch sensor menjadi exam, seed 42, digest resmi, dan mount kunci. |
| `tester-manifest.yaml` | Snapshot manifest resmi dengan digest sesuai email. |
| `exam-overlay/` | Overlay untuk memeriksa profil ujian dan telemetry tanpa cluster. |
| `templates/` | Struktur Secret dengan placeholder, bukan kunci asli. |
| `p1/conversion-contract.json` | Handoff konversi dataset kepada Dicky dan checksum resmi. |
| `p3/` | Kebijakan awal/perbaikan, payload versi skema/replay/laporan, serta koleksi request manual. |
| `p4/` | Koleksi request dan tiga fixture sintetis untuk memeriksa alat reference diff. |
| `export-audit.py` | Ekspor semua halaman Audit dengan pemeriksaan partial, cursor, dan identitas event. |

Untuk ekspor setelah Atlas tersedia, set `AUDIT_TOKEN` melalui environment lalu jalankan:

```bash
python demo-assets/m2/export-audit.py --source SATELIT --output .deploy/demo-private/satellite.json
```

Helper ini membaca endpoint Audit Atlas, tidak memulai sesi satelit. Ekspor bukan snapshot atomik; samakan window/offset dengan referensi asisten. Fixture `synthetic-*` hanya menguji alat pembanding, bukan membuktikan hasil feed ujian.

Koleksi `p4/requests.http` memuat request start sesi ujian yang dipisahkan. Jalankan request tersebut sekali saat recorder sudah berjalan; jangan memakai Send All. Request P3 yang melakukan replay dijalankan setelah gangguan selesai dan kebijakan yang sesuai sudah aktif.
