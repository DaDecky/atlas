#!/usr/bin/env python3
"""Practice-only PVMBG HTTP proxy for malformed-boundary demonstrations.

It forwards headers in memory without logging them. Binds loopback by default;
configure the Aggregator's PVMBG_BASE_URL to this origin. It is not deployed in
the demo overlay. Each response retains a valid first record when possible.
"""
import argparse
import http.server
import json
from pathlib import Path
import urllib.error
import urllib.request


def corrupt(raw, mode):
    value = json.loads(raw)
    if not isinstance(value, list) or not value:
        raise ValueError("proxy requires a nonempty upstream array")
    # Inclusive `since` can repeat the checkpoint record. Use the newest
    # upstream record so repeated fault responses still expose forward progress.
    first = json.dumps(value[-1], separators=(",", ":")).encode()
    if mode == "truncated":
        return b"[" + first + b',{"unfinished":'
    if mode == "utf8":
        return b"[" + first + b',{"text":"\xff"}]'
    if mode == "depth":
        return b"[" + first + b',{"extra":' + b"[" * 2000 + b"0" + b"]" * 2000 + b"}]"
    if mode == "oversize":
        return b"[" + first + b',{"extra":"' + b"x" * (4 << 20) + b'"}]'
    return raw


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", required=True, help="real PVMBG HTTP origin")
    parser.add_argument("--listen", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18082)
    parser.add_argument("--mode", choices=["pass", "truncated", "utf8", "depth", "oversize"], required=True)
    parser.add_argument("--mode-file", type=Path, help="practice harness may change mode without restarting the poller")
    args = parser.parse_args()
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if not self.path.startswith("/volcanic-reports"):
                self.send_error(404)
                return
            headers = {key: value for key, value in self.headers.items() if key.lower() in ["authorization", "x-correlation-id"]}
            req = urllib.request.Request(args.upstream.rstrip("/") + self.path, headers=headers)
            try:
                with opener.open(req, timeout=5) as response:
                    raw = response.read((4 << 20) + 1)
                    if len(raw) > 4 << 20:
                        raise ValueError("upstream response exceeds proxy boundary")
                    mode = args.mode_file.read_text().strip() if args.mode_file else args.mode
                    if mode not in ["pass", "truncated", "utf8", "depth", "oversize"]:
                        raise ValueError("unknown practice mode")
                    body = corrupt(raw, mode)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except urllib.error.HTTPError as error:
                self.send_error(error.code)
            except (OSError, ValueError):
                self.send_error(502)

        def log_message(self, *_):
            pass

    print("Practice proxy listening on", args.listen + ":" + str(args.port), "mode=" + args.mode, flush=True)
    http.server.ThreadingHTTPServer((args.listen, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
