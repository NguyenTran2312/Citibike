"""GBFS 2.3 giả lập cho integration test của poller (chỉ dùng thư viện chuẩn).

Phục vụ /gbfs.json, /station_status.json, /station_information.json với
STATIONS trạm. Hành vi của hai feed được điều khiển bằng /control/behavior.json:

    {"generation": 3, "delay_seconds": 25, "fail_first": 2}

- delay_seconds: chờ bao lâu trước khi trả lời mỗi request feed.
- fail_first: số request feed đầu tiên (tính từ khi `generation` đổi) trả 503.

Mỗi request feed được ghi một dòng JSON vào /control/requests.log.
"""
import json
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

STATIONS = 2519
BASE_URL = "http://gbfs-mock:8000"
CONTROL = Path("/control")

lock = threading.Lock()
state = {"generation": None, "count": 0}


def behavior():
    try:
        return json.loads((CONTROL / "behavior.json").read_text())
    except (OSError, ValueError):
        return {}


def log(entry):
    with lock, (CONTROL / "requests.log").open("a") as file:
        file.write(json.dumps(entry) + "\n")


def feed(name, now):
    if name == "station_status":
        stations = [
            {
                "station_id": f"mock-{i}",
                "num_bikes_available": i % 20,
                "num_docks_available": 20 - i % 20,
                "num_ebikes_available": i % 3,
                "is_installed": 1,
                "is_renting": 1,
                "is_returning": 1,
                "last_reported": now - 30,
            }
            for i in range(STATIONS)
        ]
    else:
        stations = [
            {
                "station_id": f"mock-{i}",
                "name": f"Mock Station {i}",
                "lat": 40.7 + i / 100000,
                "lon": -73.9 - i / 100000,
                "short_name": f"{i}.00",
                "capacity": 20,
            }
            for i in range(STATIONS)
        ]
    return {"last_updated": now, "ttl": 60, "version": "2.3", "data": {"stations": stations}}


class Handler(BaseHTTPRequestHandler):
    def send_json(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        path = self.path.strip("/")
        if path == "gbfs.json":
            feeds = [
                {"name": n, "url": f"{BASE_URL}/{n}.json"}
                for n in ("station_status", "station_information")
            ]
            return self.send_json(200, {"data": {"en": {"feeds": feeds}}})

        name = path.removesuffix(".json")
        if name not in ("station_status", "station_information"):
            return self.send_json(404, {"error": "not found"})

        rules = behavior()
        with lock:
            if rules.get("generation") != state["generation"]:
                state.update(generation=rules.get("generation"), count=0)
            index = state["count"]
            state["count"] += 1

        received = datetime.now(timezone.utc).isoformat()
        log({"event": "received", "feed": name, "received": received,
             "generation": rules.get("generation"), "index": index})
        time.sleep(float(rules.get("delay_seconds", 0)))
        status = 503 if index < int(rules.get("fail_first", 0)) else 200
        now = int(time.time())
        try:
            if status == 503:
                self.send_json(503, {"error": "mock unavailable"})
            else:
                self.send_json(200, feed(name, now))
            sent = True
        except OSError:
            sent = False   # client đã ngắt kết nối (ví dụ poller bị kill)
        log({"event": "answered", "feed": name, "status": status, "received": received,
             "answered": datetime.now(timezone.utc).isoformat(), "sent": sent,
             "generation": rules.get("generation"), "index": index})

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
