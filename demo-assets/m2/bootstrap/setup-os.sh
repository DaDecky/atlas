#!/usr/bin/env bash
# Ubuntu dependencies for K3s nodes, the operator, or a separate k6 runner.
set -euo pipefail
role= dry_run=0
while (($#)); do
  case "$1" in
    --role) role=${2:?missing role}; shift 2 ;;
    --dry-run) dry_run=1; shift ;;
    -h|--help) echo 'sudo bash setup-os.sh --role node|operator|runner|all [--dry-run]'; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done
case "$role" in node|operator|runner|all) ;; *) echo '--role node|operator|runner|all is required' >&2; exit 2 ;; esac
if ((dry_run)); then
  echo "Plan: Ubuntu 22.04/24.04/26.04 amd64; role=$role"
  echo 'Base: curl, CA certificates, Python/venv, SSH, jq, git, make, PostgreSQL client, shellcheck.'
  [[ $role != node ]] && echo 'Operator/runner: isolated Python venv; k6 from the official Grafana apt repository.'
  [[ $role == operator || $role == all ]] && echo 'Operator: Docker Engine, Go 1.27.4, kubectl v1.37.2; official repositories/checksummed binaries.'
  echo 'No command was installed or executed.'
  exit 0
fi
[[ $(uname -s) == Linux && $(uname -m) == x86_64 ]] || { echo 'Requires Linux amd64' >&2; exit 2; }
[[ $EUID == 0 ]] || { echo 'Run with sudo on the Ubuntu machine' >&2; exit 2; }
. /etc/os-release
[[ $ID == ubuntu && $VERSION_ID =~ ^(22\.04|24\.04|26\.04)$ ]] || { echo 'Supported OS: Ubuntu 22.04, 24.04, 26.04' >&2; exit 2; }
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y ca-certificates curl gnupg jq openssh-client openssh-server \
  python3 python3-venv python3-pip python-is-python3 git make tar gzip unzip \
  iproute2 iptables conntrack socat postgresql-client shellcheck
systemctl enable --now ssh
work=$(mktemp -d /tmp/atlas-deps.XXXXXXXX)
trap 'rm -rf -- "$work"' EXIT

if [[ $role != node ]]; then
  install -d -m 0755 /etc/apt/keyrings /opt/atlas
  curl -fsSL https://dl.k6.io/key.gpg -o "$work/k6.key"
  gpg --batch --yes --dearmor -o /etc/apt/keyrings/atlas-k6.gpg "$work/k6.key"
  chmod 0644 /etc/apt/keyrings/atlas-k6.gpg
  printf '%s\n' 'deb [signed-by=/etc/apt/keyrings/atlas-k6.gpg] https://dl.k6.io/deb stable main' > /etc/apt/sources.list.d/atlas-k6.list
  apt-get update
  apt-get install -y k6
  python3 -m venv /opt/atlas/demo-venv
  /opt/atlas/demo-venv/bin/python -m pip install 'cryptography>=44' 'PyYAML==6.0.4'
fi

if [[ $role == operator || $role == all ]]; then
  # Keep pre-existing Docker installations/data; do not uninstall another runtime.
  if ! command -v docker >/dev/null; then
    for package in docker.io containerd runc; do
      if dpkg-query -W -f='${Status}' "$package" 2>/dev/null | grep -q '^install ok installed$'; then
        echo 'Existing distro container runtime detected. Configure Docker explicitly before continuing.' >&2
        exit 1
      fi
    done
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/atlas-docker.asc
    chmod 0644 /etc/apt/keyrings/atlas-docker.asc
    cat > /etc/apt/sources.list.d/atlas-docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: ${UBUNTU_CODENAME:-$VERSION_CODENAME}
Components: stable
Architectures: amd64
Signed-By: /etc/apt/keyrings/atlas-docker.asc
EOF
    apt-get update
    apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
  fi
  systemctl enable --now docker
  if [[ -n ${SUDO_USER:-} && $SUDO_USER != root ]]; then
    usermod -aG docker "$SUDO_USER"
    echo 'Docker group configured. Open a new login shell before deploying.'
  fi
  go_version=1.27.4
  archive="go${go_version}.linux-amd64.tar.gz"
  if [[ ! -x /opt/atlas/go${go_version}/bin/go ]]; then
    curl -fsSL 'https://go.dev/dl/?mode=json&include=all' -o "$work/go-releases.json"
    checksum=$(python3 - "$work/go-releases.json" "$archive" <<'PY'
import json, sys
releases = json.load(open(sys.argv[1], encoding='utf-8'))
matches = [item['sha256'] for release in releases for item in release['files'] if item['filename'] == sys.argv[2]]
if len(matches) != 1:
    raise SystemExit('Requested Go release/checksum is not available')
print(matches[0])
PY
)
    curl -fsSL "https://go.dev/dl/$archive" -o "$work/$archive"
    printf '%s  %s\n' "$checksum" "$work/$archive" | sha256sum -c -
    [[ ! -e /opt/atlas/go${go_version} ]] || { echo 'Incomplete Go install exists; inspect /opt/atlas first' >&2; exit 1; }
    install -d /opt/atlas/go${go_version}
    tar -xzf "$work/$archive" --strip-components=1 -C /opt/atlas/go${go_version}
  fi
  ln -sfn /opt/atlas/go${go_version}/bin/go /usr/local/bin/go
  ln -sfn /opt/atlas/go${go_version}/bin/gofmt /usr/local/bin/gofmt
  kubectl_version=v1.37.2
  curl -fsSL "https://dl.k8s.io/release/$kubectl_version/bin/linux/amd64/kubectl" -o "$work/kubectl"
  curl -fsSL "https://dl.k8s.io/release/$kubectl_version/bin/linux/amd64/kubectl.sha256" -o "$work/kubectl.sha256"
  printf '%s  %s\n' "$(cat "$work/kubectl.sha256")" "$work/kubectl" | sha256sum -c -
  install -m 0755 "$work/kubectl" /usr/local/bin/kubectl
  go version
  kubectl version --client=true
fi
echo "OS dependencies ready for role=$role"
[[ $role == node ]] || echo 'Activate Python with: source /opt/atlas/demo-venv/bin/activate'
