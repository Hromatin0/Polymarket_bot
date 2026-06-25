"""
Determines the IP where Live Server is actually running (port 5500)
and updates liveServer.settings.host in .vscode/settings.json
"""

import socket
import json
from pathlib import Path


def get_all_ips() -> list:
    """All local IPv4 except loopback and link-local."""
    ips = []
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None):
            ip = info[4][0]
            if (":" not in ip
                    and ip != "127.0.0.1"
                    and not ip.startswith("169.254")
                    and ip not in ips):
                ips.append(ip)
    except Exception:
        pass
    return ips


def find_live_server_ip(port=5500):
    """Checks which IP Live Server is actually listening on."""
    ips = get_all_ips()
    print(f"IP addresses found: {len(ips)}")

    working = []
    for ip in ips:
        try:
            s = socket.create_connection((ip, port), timeout=1)
            s.close()
            print(f"  ✅ {ip}:{port} — available")
            working.append(ip)
        except Exception:
            print(f"  ❌ {ip}:{port} — unavailable")

    if not working:
        return None
    if len(working) == 1:
        return working[0]

    # Multiple working IPs — ask the user
    print(f"\nMultiple working addresses, choose the desired one:")
    for i, ip in enumerate(working):
        print(f"  {i+1}. {ip}")
    while True:
        try:
            choice = int(input("Number: ")) - 1
            if 0 <= choice < len(working):
                return working[choice]
        except (ValueError, KeyboardInterrupt):
            break
    return working[0]


def update_settings(ip):
    script_dir = Path(__file__).parent
    settings_path = script_dir / ".vscode" / "settings.json"
    if not settings_path.exists():
        settings_path = script_dir.parent / ".vscode" / "settings.json"
    settings_path.parent.mkdir(parents=True, exist_ok=True)

    data = {}
    if settings_path.exists():
        try:
            with open(settings_path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            pass

    old_ip = data.get("liveServer.settings.host", "—")
    data["liveServer.settings.host"] = ip

    with open(settings_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    return old_ip, settings_path


if __name__ == "__main__":
    PORT = 5500

    print("Searching for Live Server...\n")
    ip = find_live_server_ip(PORT)

    if not ip:
        print(f"\n Live Server not found on any IP.")
        print(f"   Make sure it is running (Go Live in VS Code), then repeat.")
    else:
        old_ip, path = update_settings(ip)
        print(f"\nIP:    {old_ip}  ->  {ip}")
        print(f"File:  {path}")
        print(f"Link: http://{ip}:{PORT}/dashboard.html")