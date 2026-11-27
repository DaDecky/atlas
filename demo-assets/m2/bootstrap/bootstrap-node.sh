#!/usr/bin/env bash
# Run by cluster.py on one VM. Never uninstalls K3s or clears its data.
set -euo pipefail
role= node_name= node_ip= server_ip= version= token_file=
node_network=10.0.0.0/24 pod_cidr=10.42.0.0/16 service_cidr=10.43.0.0/16 dry_run=0
while (($#)); do
  case "$1" in
    --role) role=${2:?}; shift 2 ;;
    --name) node_name=${2:?}; shift 2 ;;
    --ip) node_ip=${2:?}; shift 2 ;;
    --server-ip) server_ip=${2:?}; shift 2 ;;
    --version) version=${2:?}; shift 2 ;;
    --token-file) token_file=${2:?}; shift 2 ;;
    --node-network) node_network=${2:?}; shift 2 ;;
    --pod-cidr) pod_cidr=${2:?}; shift 2 ;;
    --service-cidr) service_cidr=${2:?}; shift 2 ;;
    --dry-run) dry_run=1; shift ;;
    -h|--help) echo 'bootstrap-node.sh --role server|agent --name NAME --ip IP --server-ip IP --version VERSION [--token-file FILE] [--dry-run]'; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done
[[ $role == server || $role == agent ]] || { echo 'Invalid node role' >&2; exit 2; }
[[ $node_name =~ ^atlas-node-[abc]$ && $version =~ ^v1\.[0-9]+\.[0-9]+\+k3s[0-9]+$ ]] || { echo 'Invalid node name/version' >&2; exit 2; }
[[ $node_ip =~ ^[0-9.]+$ && $server_ip =~ ^[0-9.]+$ ]] || { echo 'Invalid IP address' >&2; exit 2; }
if ((dry_run)); then
  echo "Plan: $node_name ($node_ip), $role, K3s $version, server $server_ip"
  echo "Pod network $pod_cidr; service network $service_cidr; trusted node LAN $node_network"
  echo 'Install node dependencies, configure kernel/network, and install or reuse the matching K3s service.'
  exit 0
fi
[[ $EUID == 0 && $(uname -s) == Linux ]] || { echo 'Run as root on the Linux VM' >&2; exit 2; }
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
bash "$script_dir/setup-os.sh" --role node
python3 - "$node_ip" "$server_ip" "$node_network" "$pod_cidr" "$service_cidr" <<'PY'
import ipaddress, subprocess, sys
for value in sys.argv[1:3]: ipaddress.ip_address(value)
for value in sys.argv[3:]: ipaddress.ip_network(value)
addresses = subprocess.check_output(['ip', '-4', '-o', 'addr', 'show'], text=True)
if not any(part.split('/')[0] == sys.argv[1] for part in addresses.split() if '/' in part):
    raise SystemExit('Configured node IP is absent. Set the VM LAN address/DHCP reservation before bootstrap.')
PY
service=k3s
[[ $role == server ]] || service=k3s-agent
if command -v k3s >/dev/null; then
  actual=$(k3s --version | head -n 1 | awk '{print $3}')
  [[ $actual == "$version" ]] || { echo "Existing K3s version $actual differs from $version; no automatic upgrade/reset performed" >&2; exit 1; }
  systemctl enable --now "$service"
  echo "Reused existing K3s $version; cluster identity and IPs will be verified by the operator."
  exit 0
fi
modprobe overlay
modprobe br_netfilter
printf '%s\n' overlay br_netfilter > /etc/modules-load.d/atlas-k3s.conf
cat > /etc/sysctl.d/90-atlas-k3s.conf <<'EOF'
net.ipv4.ip_forward = 1
net.bridge.bridge-nf-call-iptables = 1
net.bridge.bridge-nf-call-ip6tables = 1
EOF
sysctl --system >/dev/null
swapoff -a
[[ -e /etc/fstab.atlas-before-k3s ]] || cp -a /etc/fstab /etc/fstab.atlas-before-k3s
python3 - <<'PY'
from pathlib import Path
p = Path('/etc/fstab')
lines = []
for line in p.read_text().splitlines():
    fields = line.split()
    if not line.lstrip().startswith('#') and len(fields) > 2 and fields[2] == 'swap':
        line = '# atlas-k3s disabled swap: ' + line
    lines.append(line)
p.write_text('\n'.join(lines) + '\n')
PY
timedatectl set-ntp true || echo 'Check time synchronisation before the demo.'
if command -v ufw >/dev/null && ufw status | grep -q '^Status: active'; then
  [[ $role != server ]] || ufw allow from "$node_network" to any port 6443 proto tcp
  ufw allow from "$node_network" to any port 8472 proto udp
  ufw allow from "$node_network" to any port 10250 proto tcp
  ufw allow from "$pod_cidr" to any
  ufw allow from "$service_cidr" to any
  ufw route allow from "$pod_cidr"
  ufw route allow to "$pod_cidr"
fi
install -d -m 0700 /etc/rancher/k3s
[[ ! -e /etc/rancher/k3s/config.yaml ]] || { echo 'An unmanaged K3s config exists; inspect it before installing' >&2; exit 1; }
if [[ $role == server ]]; then
  cat > /etc/rancher/k3s/config.yaml <<EOF
node-name: $node_name
node-ip: $node_ip
advertise-address: $node_ip
tls-san:
  - $server_ip
write-kubeconfig-mode: '0600'
cluster-cidr: $pod_cidr
service-cidr: $service_cidr
EOF
else
  [[ -f $token_file && -s $token_file ]] || { echo 'Agent join token file is missing' >&2; exit 1; }
  [[ $token_file == /etc/rancher/k3s/atlas-join.token ]] || { echo 'Unexpected token file path' >&2; exit 2; }
  chmod 0600 "$token_file"
  cat > /etc/rancher/k3s/config.yaml <<EOF
node-name: $node_name
node-ip: $node_ip
server: https://$server_ip:6443
token-file: $token_file
EOF
fi
chmod 0600 /etc/rancher/k3s/config.yaml
installer=$(mktemp /tmp/atlas-k3s-install.XXXXXXXX)
trap 'rm -f -- "$installer"' EXIT
curl -fsSL https://get.k3s.io -o "$installer"
INSTALL_K3S_VERSION="$version" INSTALL_K3S_EXEC="$role" sh "$installer"
systemctl is-active --quiet "$service"
echo "$node_name K3s service is active; waiting for node readiness is handled by cluster.py."
