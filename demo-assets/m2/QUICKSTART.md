# Menyalakan VM sampai demo Atlas

Status saat panduan ini dibuat: aset/panduan sudah tersedia pada branch `demo`; pemeriksaan lokal tidak mengakses cluster dan belum memulai sesi satelit. Installer dan orkestrator berikut sudah disiapkan untuk dijalankan saat VM hidup. Keberhasilan instalasi dan pengujian live tetap harus diperiksa pada VM yang sebenarnya.

## 1. Target dan prasyarat

Target mengikuti bukti cluster sebelumnya: Ubuntu 24.04 amd64, k3s `v1.37.2+k3s1`, node-a `10.0.0.40`, node-b `.41`, dan node-c `.42`. Installer dependency juga menerima Ubuntu 22.04/26.04 amd64. Gunakan 4 vCPU/8 GiB untuk node-a dan 2 vCPU/2 GiB untuk tiap worker sebagai topologi awal yang dilaporkan; amati kebutuhan memori saat scaling. Runner 10.000 VU disediakan terpisah dari cluster.

Sebelum bootstrap, ketiga VM harus mempunyai IP LAN tersebut melalui konfigurasi VM/DHCP reservation, dapat diakses SSH, dan memiliki internet. Script tidak menebak interface/gateway atau mengganti Netplan. Aturan NetworkPolicy aplikasi masih mengikuti topologi ini; perubahan IP/CIDR memerlukan perubahan manifest aplikasi juga.

Pada mesin operator, siapkan alias SSH `atlas-node-a/b/c` di `~/.ssh/config`, misalnya:

```sshconfig
Host atlas-node-a
    HostName 10.0.0.40
    User adha
    IdentityFile ~/.ssh/id_ed25519
Host atlas-node-b
    HostName 10.0.0.41
    User adha
    IdentityFile ~/.ssh/id_ed25519
Host atlas-node-c
    HostName 10.0.0.42
    User adha
    IdentityFile ~/.ssh/id_ed25519
```

Ganti `User` dan key path sesuai VM. Verifikasi host key pada koneksi SSH pertama. Bootstrap memakai BatchMode, StrictHostKeyChecking, dan `sudo -n`; siapkan SSH key dan akses sudo noninteraktif untuk akun administrasi VM, atau gunakan alias ke akun root yang memang diizinkan. Bootstrap berhenti jika akses itu belum tersedia.

Firewall host/Proxmox harus mengizinkan SSH dari operator, TCP 6443 ke node-a, UDP 8472 antar-node, dan TCP 10250 antar-node. Script menambahkan aturan terbatas pada LAN/pod/service jika UFW aktif; script tidak mematikan firewall atau membuka VXLAN ke internet.

## 2. Dependency OS

Ambil branch `demo` pada mesin operator dan jalankan semua perintah Python dari root repo. Operator Windows dapat memakai Python, SSH/SCP, kubectl, Go, dan Docker Desktop yang sudah tersedia; pastikan Docker Desktop berjalan. Installer Bash berikut ditujukan untuk Ubuntu, bukan PowerShell host Windows.

Pada operator Ubuntu, jalankan sekali:

```bash
sudo bash demo-assets/m2/bootstrap/setup-os.sh --role operator
```

Script memasang dependency, Docker dari repositori resmi, Go 1.27.4, kubectl v1.37.2 dengan pemeriksaan checksum, k6, dan Python venv `/opt/atlas/demo-venv`. Buka login shell baru agar membership grup Docker berlaku, lalu:

```bash
source /opt/atlas/demo-venv/bin/activate
docker info
```

Pada runner Ubuntu terpisah, cukup jalankan `sudo bash demo-assets/m2/bootstrap/setup-os.sh --role runner`, lalu aktifkan venv yang sama. Clone repo pada runner agar `loadtest/m2.js`, `session.js`, dan helper tersedia. Dependency node dipasang otomatis oleh bootstrap SSH; Docker tidak dipasang pada node hanya untuk menjalankan k3s karena k3s sudah menyertakan containerd.

Periksa rencana installer tanpa memasang apa pun:

```bash
bash demo-assets/m2/bootstrap/setup-os.sh --role node --dry-run
bash demo-assets/m2/bootstrap/setup-os.sh --role operator --dry-run
```

Pada operator Windows atau Python tanpa venv paket demo, pasang parser YAML lokal:

```bash
python -m pip install --target .deploy/demo-private/python-deps -r demo-assets/m2/requirements.txt
```

`cryptography` dibutuhkan ketika membuat environment baru; gunakan venv pada Linux atau interpreter Windows yang sudah mempunyai paket tersebut.

## 3. Bootstrap tiga VM dari operator

```bash
python demo-assets/m2/cluster.py init-config
python demo-assets/m2/cluster.py plan
python demo-assets/m2/cluster.py --dry-run bootstrap
```

Config lokal berada di `.deploy/demo-private/cluster.json`. Sesuaikan alias SSH jika namanya berbeda. Setelah VM, IP, SSH, dan sudo siap, jalankan:

```bash
python demo-assets/m2/cluster.py bootstrap
```

Satu perintah ini memeriksa sudo pada semua VM, menyalin installer, memasang dependency node/k3s, mengambil join token secara privat, menggabungkan worker, menulis kubeconfig lokal, dan menunggu tiga node Ready. Script juga memverifikasi nama, InternalIP, versi k3s, dan boot ID yang berbeda. Token tidak dikirim sebagai argumen command line atau dicetak.

K3s yang sudah terpasang dengan versi sama dipakai kembali tanpa uninstall atau penghapusan data. Versi yang berbeda dihentikan untuk diperiksa, bukan di-upgrade/reset otomatis. Script menonaktifkan swap untuk instalasi baru dan menyimpan backup fstab sebelum perubahan. Jika node baru gagal karena jaringan atau download, periksa output dan state instalasinya sebelum mengulang.

Kubeconfig disimpan di `.deploy/demo-private/cluster/kubeconfig.yaml`, bukan menimpa `~/.kube/config`. Untuk cluster yang sudah ada dan cocok:

```bash
python demo-assets/m2/cluster.py kubeconfig
python demo-assets/m2/cluster.py verify
```

## 4. Environment, kunci, dan deploy aplikasi

Pada mesin ini environment/kunci sebelumnya sudah diisi. Pada operator lain, buat atau pertahankan kredensial service dan isi materi email dengan input tersembunyi:

```bash
python demo-assets/m2/demo.py init-env --interactive
python demo-assets/m2/demo.py status
python demo-assets/m2/check-assets-local.py --check-private
```

Enter mempertahankan nilai yang sudah ada. Kunci ujian berbeda dari token HTTP Sensor Pantai. Jangan menyalin nilai kunci ke folder versi Git.

Setelah bootstrap terverifikasi dan Docker/Go tersedia:

```bash
python demo-assets/m2/demo.py --dry-run deploy
python demo-assets/m2/demo.py deploy
```

Wrapper memeriksa topologi, memanggil deploy aplikasi yang sudah ada, membangun/mengimpor image, membuat Secret kunci dari berkas, mengaktifkan profil sensor exam/seed 42, memasang Tester Agent, serta menunggu readiness. Perintah deploy tidak memulai sesi satelit. Bila hanya mengulang konfigurasi ujian setelah deploy:

```bash
python demo-assets/m2/demo.py configure-exam
python demo-assets/m2/demo.py status --live
```

Status lokal saja tidak membuktikan cluster sehat. Gunakan `--live` setelah VM hidup; jangan menyebut dataset P1 sudah dimuat sebelum import serta pemeriksaan ID/checksum selesai. Lampiran dataset dan konversi tetap menjadi tugas Dicky pada [panduan utama](../../demo.md).

## 5. Terminal rekaman

Tetapkan satu run ID untuk capture, ujian, dan perhitungan P5, misalnya `rekaman-01`. Semua hasil berada di `.deploy/demo-private/evidence/<run-id>/`.

**Terminal A, port forward operator** (foreground; Ctrl+C menghentikan semuanya):

```bash
python demo-assets/m2/demo.py ports
```

**Terminal B, log** (mulai sebelum gangguan/P5; log Tester Agent tampil di layar, notifier/aggregator disimpan dan disambung ulang setelah pergantian pod):

```bash
python demo-assets/m2/demo.py --run-id rekaman-01 capture
```

**Terminal C, operator Adha:** setelah Dicky memastikan screen recorder berjalan, jalankan start sekali:

```bash
python demo-assets/m2/demo.py --run-id rekaman-01 start-satellite --recording-started
```

Wrapper menolak pengulangan token yang sudah mempunyai receipt lokal. Respons dicatat; jika tidak ada receipt lokal, jangan menyimpulkan sesi di server belum pernah dimulai. Verifikasi dengan asisten jika ada start dari mesin lain. Perintah setup, deploy, status lokal, dan dry-run tidak memulai sesi.

## 6. Perintah demo per problem

Jalankan satu skenario ujian, amati log/bukti, tunggu selesai serta pulihkan cluster, baru pindah problem. Jangan menjalankan semua exam dalam loop. Adha tetap operator cluster; Razi mengatur runner, Dicky memimpin bukti data dan recorder.

### P1, Dicky dan Adha

```bash
python demo-assets/m2/demo.py --run-id rekaman-01-before audit --source BMKG
python demo-assets/m2/demo.py --run-id rekaman-01 exam p1
python demo-assets/m2/demo.py --run-id rekaman-01-after audit --source BMKG
```

Tampilkan distribusi, dua lokasi salinan, database, request selama gangguan, partial marker, dan refill. Checksum source BMKG juga mencakup seed/live event; Dicky membandingkan himpunan dataset resmi pada cutoff yang sama. Perhatikan run wipe yang masih aktif sebelum menyalakan VM kembali; jangan menimpa skenario ujian yang belum selesai.

### P2, Razi dan Adha

```bash
python demo-assets/m2/demo.py --run-id rekaman-01 exam p2
```

Perlihatkan lease, epoch, failover, dan bukti fencing yang benar-benar terpicu. Freeze/resume worker tetap dilakukan secara eksplisit melalui Proxmox; helper tidak menebak VM mana yang harus dibekukan.

Siapkan akses langsung runner, menggunakan dua Service NodePort khusus benchmark:

```bash
python demo-assets/m2/demo.py runner-services
```

Default runner memakai `http://10.0.0.40:32562` untuk Client API dan `:31561` untuk Auth. Pastikan rute LAN/Tailscale tersedia dan firewall mengizinkan kedua port TCP dari IP runner. Jangan memakai tunnel port-forward sebagai jalur pengukuran kapasitas. Muat kredensial client pada `.env.m2` runner yang sesuai dengan Auth cluster; jangan membuat kredensial acak berbeda pada runner.

Pada operator, set Aggregator ke 3. Pada runner, mulai k6; pada operator, jalankan scale-sequence ketika beban mulai:

```bash
python demo-assets/m2/demo.py scale 3
```

```bash
python demo-assets/m2/demo.py --run-id p2-sustained load sustained --vus 10000 --duration 150s
```

```bash
python demo-assets/m2/demo.py scale-sequence
```

Untuk throughput, operator menjalankan `scale 1`, tunggu Ready, lalu Razi menjalankan run berikut. Ulangi dengan `scale 3` dan run ID berbeda agar hasil tidak tertimpa:

```bash
python demo-assets/m2/demo.py --run-id p2-one load throughput --vus 500 --duration 150s
python demo-assets/m2/demo.py --run-id p2-three load throughput --vus 500 --duration 150s
```

Bandingkan complete 200/s dengan pengaturan identik. Kedua baris di atas adalah dua run terpisah, masing-masing setelah operator mengatur jumlah replika yang sesuai.

Siapkan image pengganti pada semua node sebelum uji rollout; ini hanya build/import, tidak mengubah deployment:

```bash
python demo-assets/m2/demo.py prepare-rollout milestone-2-rollout
```

Pada runner jalankan `load rollout --duration 300s`, lalu pada operator jalankan `rollout milestone-2-rollout` saat beban berjalan. Periksa nol request gagal/429/dropped iteration. Sesudah selesai, kembalikan 3 replika.

### P3, Adha dibantu Dicky/Razi

```bash
python demo-assets/m2/demo.py --run-id rekaman-01 exam p3
python demo-assets/m2/demo.py --run-id rekaman-01 p3-list
```

Setelah gangguan selesai dan pesan gagal sudah dibuktikan, terapkan perbaikan yang sesuai:

```bash
python demo-assets/m2/demo.py p3-policy after
```

Tunggu projected ConfigMap pada pod tersinkron (sekitar satu menit), lalu:

```bash
python demo-assets/m2/demo.py --run-id rekaman-01 p3-replay
python demo-assets/m2/demo.py --run-id rekaman-01 p3-list
python demo-assets/m2/demo.py --run-id rekaman-01 field-report
python demo-assets/m2/demo.py --run-id rekaman-01 field-report --repeat
```

Default policy-after menerima rename ash dan string count. Hilangnya `volcano_id` tetap membutuhkan pemetaan report yang benar; jangan menebaknya. Balasan replay 202 berarti queued, bukan bukti seluruh pesan recovered. Dicky menunjukkan status recovered dan inbox laporan lapangan; `--repeat` mengirim payload persis sama untuk membuktikan deduplikasi.

### P4, Adha dan Dicky

Sesi sudah berjalan sejak awal rekaman. Tampilkan source SATELIT, offset/lag, pengelompokan, update/retract, dan resume. Ekspor semua halaman:

```bash
python demo-assets/m2/demo.py --run-id rekaman-01 export-satellite
```

Bandingkan file `p4-satellite.json` pada folder evidence run tersebut dengan referensi asli menggunakan `scripts/satellite-reference-diff.py`. Pastikan window/offset sama. Fixture sintetis di repo bukan referensi ujian.

### P5, Razi dan Adha

Pastikan capture masih berjalan dan port forward sensor aktif. Wrapper mengambil `generated_at` Sensor Pantai sebagai batas waktu observasi, bukan menganggap jam sensor 60x sama dengan jam operator:

```bash
python demo-assets/m2/demo.py --run-id rekaman-01 exam p5
```

Jika waktu sensor tidak bisa dibaca, berikan `--since <RFC3339-dengan-timezone>` berdasarkan window observasi yang benar. Pada runner Razi, jalankan `load overload --duration 150s`. Tampilkan circuit breaker, isolasi BMKG, prioritas Field Team, notifikasi, dan pemulihan otomatis tanpa intervensi selama game day.

Setelah gangguan selesai dan antrean notifikasi terkuras, hentikan capture dengan Ctrl+C pada Terminal B. Sebelum pod/histori sensor diganti:

```bash
python demo-assets/m2/demo.py --run-id rekaman-01 error-budget
```

Laporkan jumlah sampel, kehilangan, persentase di atas 5 detik, dan batas pengukuran. Nol sampel bukan bukti target tercapai.

## 7. Pengiriman dan status

Setelah video final di YouTube siap dan diperiksa semua anggota:

```bash
python demo-assets/m2/demo.py --run-id rekaman-01 submit-video 'https://www.youtube.com/watch?v=VIDEO_ID'
```

Perintah mengirim POST lalu membaca GET untuk verifikasi, menyimpan keduanya di evidence. Periksa respons HTTP 200 dan `received_at`. Tenggat: 29 November 2026 pukul 23.59 WIB.

Status saat ini tanpa menghubungi cluster:

```bash
python demo-assets/m2/demo.py status
```

Status live setelah VM siap: `python demo-assets/m2/demo.py status --live`. Kesiapan lokal dan hasil dry-run belum membuktikan instalasi VM atau kriteria demo sudah lulus.

## 8. Validasi script dan acuan installer

```bash
python -m unittest discover -s demo-assets/m2/tests -v
python demo-assets/m2/check-assets-local.py
```

Unit test memeriksa validasi topologi/SSH, pemisahan kubeconfig, transfer token lewat stdin, Secret yang tidak muncul pada argumen, larangan start satelit dari deploy, dan start eksplisit sekali setelah flag rekaman. Installer OS juga diperiksa dalam dry-run. Instalasi apt/systemd, jaringan VM, readiness, dan pengujian live memerlukan VM yang sudah hidup.

Acuan primer: [instalasi k3s](https://docs.k3s.io/quick-start), [persyaratan jaringan k3s](https://docs.k3s.io/installation/requirements), [release k3s yang dipin](https://github.com/k3s-io/k3s/releases/tag/v1.37.2%2Bk3s1), [Docker Ubuntu](https://docs.docker.com/engine/install/ubuntu/), [instalasi Go](https://go.dev/doc/install), [kubectl Linux](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/), dan [k6 Debian/Ubuntu](https://grafana.com/docs/k6/latest/set-up/install-k6/).
