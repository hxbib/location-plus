#!/usr/bin/env python3

import asyncio
import json
import logging
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid as uuid_mod
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import uvicorn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("locationplus")

PORT = 8042
VENV_PYTHON = sys.executable
HOLD_INTERVAL_DEFAULT = 1.0
RECONNECT_WINDOW_DEFAULT = 1800
DATA_DIR = Path.home() / ".locationplus"
AUDIT_DIR = DATA_DIR / ".internal" / ".audit"
SERVER_START_TIME = datetime.now(timezone.utc)

NETWORK_SUBPROCESS_TIMEOUT = 25
USB_SUBPROCESS_TIMEOUT = 15

_file_lock = threading.Lock()

def _ensure_data_dir():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def _load_json(filename: str, default=None):
    primary = DATA_DIR / filename
    backup = DATA_DIR / (filename + ".bak")

    for path, label in [(primary, "primary"), (backup, "backup")]:
        if path.exists():
            try:
                text = path.read_text()
                if text.strip():
                    data = json.loads(text)
                    if label == "backup":
                        log.warning("Recovered %s from backup (primary was corrupt/missing)", filename)
                    return data
            except Exception as e:
                log.warning("Failed to read %s %s: %s", label, filename, e)
    return default

def _save_json(filename: str, data):
    _ensure_data_dir()
    target = DATA_DIR / filename
    backup = DATA_DIR / (filename + ".bak")

    with _file_lock:
        fd = None
        tmp_path = None
        try:
            fd, tmp_path = tempfile.mkstemp(dir=str(DATA_DIR), suffix=".tmp")
            with os.fdopen(fd, "w") as f:
                fd = None
                json.dump(data, f, indent=2, default=str)
                f.flush()
                os.fsync(f.fileno())

            if target.exists():
                try:
                    shutil.copy2(str(target), str(backup))
                except Exception:
                    pass

            os.replace(tmp_path, str(target))
            tmp_path = None
        except Exception as e:
            log.error("Atomic save failed for %s: %s", filename, e)
        finally:
            if fd is not None:
                try:
                    os.close(fd)
                except Exception:
                    pass
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass

def _audit_log(event: str, data: dict):
    try:
        AUDIT_DIR.mkdir(parents=True, exist_ok=True)
        entry = {"ts": datetime.now(timezone.utc).isoformat(), "event": event, **data}
        log_file = AUDIT_DIR / "location_changes.jsonl"
        with open(log_file, "a") as f:
            f.write(json.dumps(entry, default=str) + "\n")
            f.flush()
            os.fsync(f.fileno())
    except Exception as e:
        log.debug("Audit log write failed: %s", e)

@dataclass
class DeviceInfo:
    udid: str
    name: str
    model: str
    os_version: str
    connection: str
    raw: dict = field(default_factory=dict)

@dataclass
class DeviceSession:
    udid: str
    device_name: str
    connection_mode: str
    lat: float
    lng: float
    active: bool = True
    use_tunnel: bool = False
    hold_thread: Optional[threading.Thread] = None
    stop_event: threading.Event = field(default_factory=threading.Event)
    last_set_at: Optional[datetime] = None
    last_enforced_at: Optional[datetime] = None
    enforce_count: int = 0
    enforce_failures: int = 0
    consecutive_failures: int = 0
    reconnecting: bool = False
    reconnect_since: Optional[datetime] = None
    reconnect_since_mono: Optional[float] = None
    location_proc: Optional[subprocess.Popen] = None

sessions: dict[str, DeviceSession] = {}
sessions_lock = threading.Lock()

_device_cache: list[DeviceInfo] = []
_device_cache_time: float = 0
_DEVICE_CACHE_TTL = 30.0

def _get_cached_devices() -> list[DeviceInfo]:
    global _device_cache, _device_cache_time
    now = time.monotonic()
    if now - _device_cache_time < _DEVICE_CACHE_TTL and _device_cache:
        return _device_cache
    fresh = _list_devices()
    if fresh:
        _device_cache = fresh
        _device_cache_time = now
    return _device_cache if _device_cache else fresh

def _invalidate_device_cache():
    global _device_cache_time
    _device_cache_time = 0

hold_interval: float = HOLD_INTERVAL_DEFAULT
reconnect_window: float = RECONNECT_WINDOW_DEFAULT

connected_ws: list[WebSocket] = []
_event_loop: Optional[asyncio.AbstractEventLoop] = None

history: deque = deque(maxlen=50)
history_lock = threading.Lock()

def _load_history():
    global history
    data = _load_json("history.json", [])
    with history_lock:
        history = deque(data, maxlen=50)

def _save_history():
    with history_lock:
        _save_json("history.json", list(history))

DEFAULT_LOCATIONS = [
    {"id": "preset-ts", "name": "Times Square", "lat": 40.7580, "lng": -73.9855, "is_preset": True},
    {"id": "preset-cp", "name": "Central Park", "lat": 40.7829, "lng": -73.9654, "is_preset": True},
    {"id": "preset-bk", "name": "Brooklyn Bridge", "lat": 40.7061, "lng": -73.9969, "is_preset": True},
    {"id": "preset-sf", "name": "San Francisco", "lat": 37.7749, "lng": -122.4194, "is_preset": True},
    {"id": "preset-london", "name": "London", "lat": 51.5074, "lng": -0.1278, "is_preset": True},
    {"id": "preset-tokyo", "name": "Tokyo", "lat": 35.6762, "lng": 139.6503, "is_preset": True},
    {"id": "preset-dubai", "name": "Dubai", "lat": 25.2048, "lng": 55.2708, "is_preset": True},
]

def _load_saved_locations() -> list[dict]:
    data = _load_json("saved_locations.json", None)
    if data is None:
        _save_json("saved_locations.json", DEFAULT_LOCATIONS)
        return list(DEFAULT_LOCATIONS)
    return data

def _save_saved_locations(locations: list[dict]):
    _save_json("saved_locations.json", locations)

def _save_state():
    snap = {}
    with sessions_lock:
        for udid, s in sessions.items():
            if s.active:
                snap[udid] = {
                    "udid": s.udid,
                    "device_name": s.device_name,
                    "connection_mode": s.connection_mode,
                    "lat": s.lat,
                    "lng": s.lng,
                    "use_tunnel": s.use_tunnel,
                    "last_set_at": s.last_set_at.isoformat() if s.last_set_at else None,
                }
    _save_json("state.json", {
        "sessions": snap,
        "hold_interval": hold_interval,
        "reconnect_window": reconnect_window,
    })

def _load_previous_state() -> Optional[dict]:
    return _load_json("state.json", None)

class TunneldManager:
    def __init__(self):
        self.process: Optional[subprocess.Popen] = None
        self._lock = threading.Lock()
        self._log_thread: Optional[threading.Thread] = None

    def _is_port_in_use(self) -> bool:
        import socket
        for port in [49151, 49152]:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(1)
                    if s.connect_ex(("127.0.0.1", port)) == 0:
                        return True
            except Exception:
                pass
        return False

    def is_running(self) -> bool:
        if self.process and self.process.poll() is None:
            return True
        return self._is_port_in_use()

    def start(self) -> dict:
        with self._lock:
            if self.is_running():
                return {
                    "success": True,
                    "message": "Already running",
                    "pid": self.process.pid if self.process and self.process.poll() is None else None,
                }

            cmd = [VENV_PYTHON, "-m", "pymobiledevice3", "remote", "tunneld"]
            is_root = hasattr(os, "geteuid") and os.geteuid() == 0
            if not is_root:
                cmd = ["sudo", VENV_PYTHON, "-m", "pymobiledevice3", "remote", "tunneld"]

            try:
                self.process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                    text=True,
                )
                self._log_thread = threading.Thread(target=self._stream_logs, daemon=True)
                self._log_thread.start()

                time.sleep(2)
                if self.process.poll() is not None:
                    output = ""
                    if self.process.stdout:
                        output = self.process.stdout.read()
                    self.process = None
                    if "password" in output.lower() or "sorry" in output.lower():
                        return {
                            "success": False,
                            "needs_terminal": True,
                            "error": "sudo requires a password. Use 'Launch in Terminal' button.",
                        }
                    return {"success": False, "error": f"tunneld exited: {output[:500]}"}

                log.info("tunneld started (pid=%d)", self.process.pid)
                return {"success": True, "pid": self.process.pid}
            except FileNotFoundError:
                return {"success": False, "error": "pymobiledevice3 not installed"}
            except Exception as e:
                return {"success": False, "error": str(e)}

    def launch_terminal(self) -> dict:
        cmd_str = f"sudo {VENV_PYTHON} -m pymobiledevice3 remote tunneld"
        try:
            subprocess.Popen([
                "osascript", "-e",
                f'tell application "Terminal" to do script "{cmd_str}"',
            ])
            return {"success": True, "message": "Terminal opened with tunneld command"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def stop(self) -> dict:
        with self._lock:
            if self.process and self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                self.process = None
                log.info("tunneld stopped (managed process)")
                return {"success": True}

            try:
                r = subprocess.run(
                    ["lsof", "-ti:49151"],
                    capture_output=True, text=True, timeout=5,
                )
                pids = r.stdout.strip().split()
                if pids:
                    needs_sudo = False
                    for pid in pids:
                        try:
                            os.kill(int(pid), 15)
                        except PermissionError:
                            needs_sudo = True
                        except ProcessLookupError:
                            pass

                    if not needs_sudo:
                        time.sleep(1)
                        if not self._is_port_in_use():
                            log.info("tunneld stopped (external, killed pid %s)", pids)
                            return {"success": True, "message": "Killed external tunneld"}

                    pid_str = " ".join(pids)
                    try:
                        subprocess.Popen([
                            "osascript", "-e",
                            f'tell application "Terminal" to do script "sudo kill {pid_str}"',
                        ])
                        return {"success": True, "message": "Terminal opened to kill tunneld with sudo"}
                    except Exception:
                        pass
            except Exception:
                pass

            self.process = None
            if self._is_port_in_use():
                return {"success": False, "error": "Could not stop external tunneld. Kill it manually."}
            return {"success": True, "message": "Not running"}

    def status(self) -> dict:
        managed = self.process is not None and self.process.poll() is None
        running = managed or self._is_port_in_use()
        return {
            "running": running,
            "managed": managed,
            "pid": self.process.pid if managed else None,
        }

    def _stream_logs(self):
        if not self.process or not self.process.stdout:
            return
        try:
            for line in self.process.stdout:
                log.info("[tunneld] %s", line.rstrip())
        except Exception:
            pass

tunneld_mgr = TunneldManager()

def _check_pymobiledevice3() -> bool:
    try:
        r = subprocess.run(
            [VENV_PYTHON, "-m", "pymobiledevice3", "--help"],
            capture_output=True, text=True, timeout=10,
        )
        return r.returncode == 0
    except Exception:
        return False

def _list_devices() -> list[DeviceInfo]:
    devices = []
    try:
        r = subprocess.run(
            [VENV_PYTHON, "-m", "pymobiledevice3", "usbmux", "list"],
            capture_output=True, text=True, timeout=USB_SUBPROCESS_TIMEOUT,
        )
        if r.returncode != 0:
            log.warning("Device list CLI failed: %s", r.stderr[:300])
            return devices

        raw = r.stdout.strip()
        if not raw:
            return devices

        try:
            entries = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("Could not parse device list JSON: %s", raw[:200])
            return devices

        for entry in entries:
            conn_type = entry.get("ConnectionType", "USB")
            conn_label = "WiFi" if conn_type.lower() == "network" else "USB"
            udid = (
                entry.get("UniqueDeviceID")
                or entry.get("Identifier")
                or entry.get("SerialNumber", "")
            )
            devices.append(DeviceInfo(
                udid=udid,
                name=entry.get("DeviceName", "iOS Device"),
                model=entry.get("ProductType", "Unknown"),
                os_version=entry.get("ProductVersion", "?"),
                connection=conn_label,
                raw=entry,
            ))
    except subprocess.TimeoutExpired:
        log.warning("Device list timed out")
    except Exception as e:
        log.warning("Device enumeration failed: %s", e)
    return devices

def _browse_network_devices(timeout: int = 8) -> list[DeviceInfo]:
    devices = []
    try:
        r = subprocess.run(
            [VENV_PYTHON, "-m", "pymobiledevice3", "remote", "browse"],
            capture_output=True, text=True, timeout=timeout,
        )
        if r.returncode != 0:
            return devices

        raw = r.stdout.strip()
        if not raw:
            return devices

        try:
            entries = json.loads(raw)
        except json.JSONDecodeError:
            return devices

        for entry in entries:
            udid = (
                entry.get("Identifier")
                or entry.get("UniqueDeviceID")
                or entry.get("udid", "")
            )
            if not udid:
                continue
            devices.append(DeviceInfo(
                udid=udid,
                name=entry.get("name", entry.get("DeviceName", "iOS Device")),
                model=entry.get("ProductType", "Unknown"),
                os_version=entry.get("ProductVersion", "?"),
                connection="WiFi",
                raw=entry,
            ))
    except subprocess.TimeoutExpired:
        pass
    except Exception:
        pass
    return devices

def _is_device_known(udid: str) -> bool:
    for d in _get_cached_devices():
        if d.udid == udid:
            return True
    return False

def _group_devices(devices: list[DeviceInfo]) -> list[dict]:
    grouped: dict[str, dict] = {}
    for d in devices:
        if d.udid not in grouped:
            grouped[d.udid] = {
                "udid": d.udid,
                "name": d.name,
                "model": d.model,
                "os_version": d.os_version,
                "connections": [d.connection],
                "connection": d.connection,
            }
        else:
            g = grouped[d.udid]
            if d.connection not in g["connections"]:
                g["connections"].append(d.connection)
            if d.connection == "USB" or g["connection"] != "USB":
                if d.name != "iOS Device":
                    g["name"] = d.name
                if d.os_version != "?":
                    g["os_version"] = d.os_version
                if d.model != "Unknown":
                    g["model"] = d.model
            if d.connection == "USB":
                g["connection"] = "USB"
    return list(grouped.values())

def _set_location_cli(udid: str, lat: float, lng: float, use_tunnel: bool = False, timeout: int = 0) -> dict:
    if timeout <= 0:
        timeout = NETWORK_SUBPROCESS_TIMEOUT if use_tunnel else USB_SUBPROCESS_TIMEOUT
    cmd = [VENV_PYTHON, "-m", "pymobiledevice3", "developer", "dvt", "simulate-location", "set"]
    if use_tunnel:
        cmd += ["--tunnel", udid]
    else:
        cmd += ["--udid", udid]
    cmd += ["--", str(lat), str(lng)]

    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )

        try:
            stdout, stderr = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            stderr_partial = ""
            try:
                import select
                if proc.stderr and select.select([proc.stderr], [], [], 0)[0]:
                    stderr_partial = proc.stderr.read() or ""
            except Exception:
                pass

            proc.kill()
            try:
                proc.wait(timeout=3)
            except Exception:
                pass

            combined = stderr_partial.lower()
            if "error" in combined or "traceback" in combined or "failed" in combined:
                _RETRYABLE_CLI = ("tunneld", "trying again", "connectionreset",
                                  "connection refused", "broken pipe", "eof",
                                  "timed out", "network is unreachable",
                                  "no route to host", "host is down")
                if not use_tunnel and any(s in combined for s in _RETRYABLE_CLI):
                    log.info("USB DVT failed for %s, retrying with --tunnel", udid)
                    return _set_location_cli(udid, lat, lng, use_tunnel=True)
                return {"success": False, "error": stderr_partial[:500] or "Command timed out"}

            method = "cli_tunnel" if use_tunnel else "cli_usb"
            log.info("Location set for %s (process didn't exit cleanly, assumed success)", udid)
            return {"success": True, "method": method, "used_tunnel": use_tunnel}

        if proc.returncode == 0:
            method = "cli_tunnel" if use_tunnel else "cli_usb"
            return {"success": True, "method": method, "used_tunnel": use_tunnel}

        combined = (stderr or "") + (stdout or "")
        combined_lower = combined.lower()
        _RETRYABLE_CLI2 = ("tunneld", "trying again", "connectionreset",
                           "connection refused", "broken pipe", "eof",
                           "timed out", "network is unreachable",
                           "no route to host", "host is down")
        if not use_tunnel and any(s in combined_lower for s in _RETRYABLE_CLI2):
            log.info("USB DVT failed for %s, retrying with --tunnel", udid)
            return _set_location_cli(udid, lat, lng, use_tunnel=True)

        return {"success": False, "error": combined[:500] or "Command failed"}
    except FileNotFoundError:
        return {"success": False, "error": "pymobiledevice3 not found"}
    except Exception as e:
        return {"success": False, "error": str(e)}

def _clear_location_cli(udid: str, use_tunnel: bool = False) -> dict:
    cmd = [VENV_PYTHON, "-m", "pymobiledevice3", "developer", "dvt", "simulate-location", "clear"]
    if use_tunnel:
        cmd += ["--tunnel", udid]
    else:
        cmd += ["--udid", udid]

    clear_timeout = NETWORK_SUBPROCESS_TIMEOUT if use_tunnel else USB_SUBPROCESS_TIMEOUT
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            stdout, stderr = proc.communicate(timeout=clear_timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.wait(timeout=3)
            except Exception:
                pass
            return {"success": True}

        if proc.returncode == 0:
            return {"success": True}

        combined = (stderr or "") + (stdout or "")
        combined_lower = combined.lower()
        _RETRYABLE_CLR = ("tunneld", "trying again", "connectionreset",
                          "connection refused", "broken pipe", "eof",
                          "timed out", "network is unreachable",
                          "no route to host", "host is down")
        if not use_tunnel and any(s in combined_lower for s in _RETRYABLE_CLR):
            return _clear_location_cli(udid, use_tunnel=True)

        return {"success": False, "error": combined[:500] or "Command failed"}
    except Exception as e:
        return {"success": False, "error": str(e)}

def _start_location_process(
    udid: str,
    lat: float,
    lng: float,
    use_tunnel: bool = False,
    startup_timeout: int = 5,
) -> Optional[subprocess.Popen]:
    cmd = [VENV_PYTHON, "-m", "pymobiledevice3", "developer", "dvt", "simulate-location", "set"]
    if use_tunnel:
        cmd += ["--tunnel", udid]
    else:
        cmd += ["--udid", udid]
    cmd += ["--", str(lat), str(lng)]

    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            proc.wait(timeout=startup_timeout)
            stderr = proc.stderr.read() if proc.stderr else ""
            if proc.returncode == 0:
                return None
            lower = stderr.lower()
            _RETRYABLE = ("tunneld", "trying again", "connectionreset",
                          "connection refused", "broken pipe", "eof",
                          "timed out", "network is unreachable",
                          "no route to host", "host is down")
            if not use_tunnel and any(s in lower for s in _RETRYABLE):
                log.info("USB DVT failed for %s, starting tunnel process...", udid)
                return _start_location_process(udid, lat, lng, use_tunnel=True,
                                               startup_timeout=startup_timeout)
            log.warning("Location process exited immediately for %s: %s", udid, stderr[:200])
            return None
        except subprocess.TimeoutExpired:
            log.info(
                "Location process started for %s (pid %d, tunnel=%s)",
                udid, proc.pid, use_tunnel,
            )
            return proc
    except Exception as e:
        log.error("Failed to start location process for %s: %s", udid, e)
        return None

def _kill_location_proc(session: DeviceSession):
    proc = session.location_proc
    if proc is None:
        return
    session.location_proc = None
    try:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
                try:
                    proc.wait(timeout=2)
                except Exception:
                    pass
        for pipe in (proc.stdout, proc.stderr, proc.stdin):
            if pipe:
                try:
                    pipe.close()
                except Exception:
                    pass
    except Exception:
        pass

PROACTIVE_REFRESH_SECONDS = 300

def _hold_location_loop(session: DeviceSession):
    log.info(
        "Hold loop started for %s (%s) at (%s, %s) tunnel=%s",
        session.device_name, session.udid, session.lat, session.lng, session.use_tunnel,
    )
    retry_backoff = 2.0
    max_retry_backoff = 30.0
    last_wake_mono = time.monotonic()
    last_expected_wait = hold_interval
    last_refresh_time = time.monotonic()
    SLEEP_THRESHOLD_S = 10.0

    def _try_start(use_tunnel: bool, startup_timeout=None):
        kwargs = {"use_tunnel": use_tunnel}
        if startup_timeout is not None:
            kwargs["startup_timeout"] = startup_timeout
        return _start_location_process(session.udid, session.lat, session.lng, **kwargs)

    def _mark_success(from_reconnect=False, connection_mode=None):
        nonlocal retry_backoff, last_refresh_time
        session.last_enforced_at = datetime.now(timezone.utc)
        session.enforce_count += 1
        session.consecutive_failures = 0
        retry_backoff = 2.0
        last_refresh_time = time.monotonic()
        if connection_mode:
            session.connection_mode = connection_mode
        if from_reconnect or session.reconnecting:
            session.reconnecting = False
            session.reconnect_since = None
            session.reconnect_since_mono = None
            log.info("Device %s reconnected!", session.device_name)
            _broadcast_threadsafe({
                "type": "device_reconnected",
                "udid": session.udid,
                "device_name": session.device_name,
                "connection_mode": session.connection_mode,
            })
            _audit_log("device_reconnected", {"udid": session.udid, "device_name": session.device_name})

    while not session.stop_event.is_set():
        now_mono = time.monotonic()
        slept_for = now_mono - last_wake_mono

        drift = slept_for - last_expected_wait
        if drift > SLEEP_THRESHOLD_S:
            log.info(
                "Wake detected for %s (drift %.1fs, expected %.1fs) -- refreshing",
                session.device_name, drift, last_expected_wait,
            )
            _kill_location_proc(session)
            retry_backoff = 2.0
            if session.reconnect_since_mono is not None:
                session.reconnect_since_mono += drift
                if session.reconnect_since:
                    try:
                        from datetime import timedelta
                        session.reconnect_since = session.reconnect_since + timedelta(seconds=drift)
                    except Exception:
                        pass

        proc = session.location_proc
        proc_alive = proc is not None and proc.poll() is None

        if proc_alive:
            session.last_enforced_at = datetime.now(timezone.utc)
            session.enforce_count += 1
            session.consecutive_failures = 0
            if session.reconnecting:
                _mark_success(from_reconnect=True)
            retry_backoff = 2.0

            elapsed_since_refresh = time.monotonic() - last_refresh_time
            if elapsed_since_refresh >= PROACTIVE_REFRESH_SECONDS:
                log.info("Proactive refresh for %s (%.0fs since last)", session.device_name, elapsed_since_refresh)
                new_proc = _try_start(session.use_tunnel)
                if new_proc:
                    _kill_location_proc(session)
                    session.location_proc = new_proc
                    last_refresh_time = time.monotonic()
                    _audit_log("proactive_refresh", {"udid": session.udid, "device_name": session.device_name})
                else:
                    log.warning("Proactive refresh failed for %s, keeping existing process", session.device_name)
        else:
            _invalidate_device_cache()
            _kill_location_proc(session)

            recovered = False
            conn_mode_used = session.connection_mode

            new_proc = _try_start(session.use_tunnel)
            if new_proc:
                recovered = True

            if not recovered and not session.use_tunnel:
                fallback_proc = _try_start(True)
                if fallback_proc:
                    new_proc = fallback_proc
                    session.use_tunnel = True
                    conn_mode_used = "tunnel"
                    log.info("Session %s switched to tunnel mode", session.udid)
                    recovered = True

            if not recovered and session.consecutive_failures > 0 \
                    and session.consecutive_failures % 3 == 0:
                log.info(
                    "Attempting network browse for %s (failure #%d)...",
                    session.device_name, session.consecutive_failures,
                )
                net_devs = _browse_network_devices(timeout=10)
                found_on_net = any(d.udid == session.udid for d in net_devs)
                if found_on_net:
                    log.info(
                        "Device %s found on WiFi — retrying tunnel with extended timeout",
                        session.device_name,
                    )
                    net_proc = _try_start(True, startup_timeout=10)
                    if net_proc:
                        new_proc = net_proc
                        session.use_tunnel = True
                        conn_mode_used = "wifi"
                        recovered = True
                else:
                    log.debug(
                        "Device %s not found on network (%d network devices seen)",
                        session.device_name, len(net_devs),
                    )

            if recovered:
                session.location_proc = new_proc
                _mark_success(from_reconnect=session.reconnecting, connection_mode=conn_mode_used)
            else:
                session.enforce_failures += 1
                session.consecutive_failures += 1

                now = datetime.now(timezone.utc)
                now_mono2 = time.monotonic()
                if not session.reconnecting:
                    session.reconnecting = True
                    session.reconnect_since = now
                    session.reconnect_since_mono = now_mono2
                    log.warning("Device %s disconnected, entering reconnect mode (will retry forever)", session.device_name)
                    _audit_log("device_disconnected", {"udid": session.udid, "device_name": session.device_name})

                elapsed = (
                    now_mono2 - session.reconnect_since_mono
                    if session.reconnect_since_mono is not None else 0
                )
                _broadcast_threadsafe({
                    "type": "device_reconnecting",
                    "udid": session.udid,
                    "device_name": session.device_name,
                    "connection_mode": session.connection_mode,
                    "elapsed_seconds": round(elapsed),
                    "window_seconds": -1,
                    "consecutive_failures": session.consecutive_failures,
                })
                retry_backoff = min(retry_backoff * 1.5, max_retry_backoff)

        _broadcast_threadsafe({
            "type": "enforcement",
            "udid": session.udid,
            "device_name": session.device_name,
            "lat": session.lat,
            "lng": session.lng,
            "connection_mode": session.connection_mode,
            "last_enforced_at": session.last_enforced_at.isoformat() if session.last_enforced_at else None,
            "enforce_count": session.enforce_count,
            "enforce_failures": session.enforce_failures,
            "consecutive_failures": session.consecutive_failures,
            "active": session.active,
            "reconnecting": session.reconnecting,
        })

        wait = retry_backoff if session.consecutive_failures > 0 else hold_interval
        last_expected_wait = wait
        last_wake_mono = time.monotonic()
        session.stop_event.wait(wait)

    _kill_location_proc(session)
    log.info("Hold loop stopped for %s", session.device_name)

def _broadcast_threadsafe(msg: dict):
    if _event_loop:
        try:
            asyncio.run_coroutine_threadsafe(_broadcast(msg), _event_loop)
        except Exception:
            pass

class SetLocationRequest(BaseModel):
    lat: float
    lng: float
    device_udids: list[str]
    connection_mode: str = "usb"

class SaveLocationRequest(BaseModel):
    name: str
    lat: float
    lng: float

class SettingsRequest(BaseModel):
    hold_interval: Optional[float] = None
    reconnect_window: Optional[float] = None

class ForceStopRequest(BaseModel):
    device_udid: Optional[str] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _event_loop, hold_interval, reconnect_window
    _event_loop = asyncio.get_event_loop()
    _ensure_data_dir()
    _load_history()

    prev = _load_previous_state()
    if prev:
        hold_interval = prev.get("hold_interval", HOLD_INTERVAL_DEFAULT)
        reconnect_window = prev.get("reconnect_window", RECONNECT_WINDOW_DEFAULT)

    if _check_pymobiledevice3() and not tunneld_mgr.is_running():
        log.info("Auto-starting tunneld...")
        result = tunneld_mgr.start()
        if result.get("success"):
            log.info("tunneld started successfully")
        else:
            log.warning("tunneld auto-start failed: %s", result.get("error", "unknown"))

    if prev and prev.get("sessions"):
        names = [s.get("device_name", "?") for s in prev["sessions"].values()]
        log.info("Previous sessions found: %s", ", ".join(names))

    log.info("Location+ starting on http://localhost:%d", PORT)

    async def _periodic_state_save():
        while True:
            await asyncio.sleep(30)
            try:
                _save_state()
            except Exception:
                pass

    _save_task = asyncio.create_task(_periodic_state_save())

    async def _tunneld_watchdog():
        while True:
            await asyncio.sleep(20)
            try:
                with sessions_lock:
                    has_active = any(s.active for s in sessions.values())
                if has_active and not tunneld_mgr.is_running():
                    log.warning("tunneld died while sessions active — restarting")
                    tunneld_mgr.start()
            except Exception:
                pass

    _tunneld_task = asyncio.create_task(_tunneld_watchdog())

    yield

    _tunneld_task.cancel()
    _save_task.cancel()

    with sessions_lock:
        for s in sessions.values():
            s.stop_event.set()
            _kill_location_proc(s)
    _save_state()
    _save_history()
    tunneld_mgr.stop()
    _event_loop = None
    log.info("Location+ shutting down")

app = FastAPI(title="Location+", version="5.0.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest

class NoCacheAPIMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: StarletteRequest, call_next):
        response = await call_next(request)
        if request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
        return response

app.add_middleware(NoCacheAPIMiddleware)

@app.get("/")
async def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")

@app.get("/favicon.ico")
async def favicon():
    import base64
    pixel = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVQI12NgAAIABQAB"
        "Nl7BcQAAAABJRU5ErkJggg=="
    )
    from fastapi.responses import Response
    return Response(content=pixel, media_type="image/png",
                    headers={"Cache-Control": "public, max-age=604800"})

@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "platform": platform.system(),
        "python_version": sys.version.split()[0],
        "pymobiledevice3": _check_pymobiledevice3(),
        "tunneld": tunneld_mgr.status(),
        "uptime_seconds": int((datetime.now(timezone.utc) - SERVER_START_TIME).total_seconds()),
    }

@app.get("/api/devices")
async def api_list_devices():
    raw_devices = _get_cached_devices()
    grouped = _group_devices(raw_devices)

    with sessions_lock:
        for dev in grouped:
            s = sessions.get(dev["udid"])
            if s and s.active:
                dev["session"] = {
                    "lat": s.lat,
                    "lng": s.lng,
                    "connection_mode": s.connection_mode,
                    "use_tunnel": s.use_tunnel,
                    "last_enforced_at": s.last_enforced_at.isoformat() if s.last_enforced_at else None,
                    "enforce_count": s.enforce_count,
                    "enforce_failures": s.enforce_failures,
                    "consecutive_failures": s.consecutive_failures,
                    "reconnecting": s.reconnecting,
                    "last_set_at": s.last_set_at.isoformat() if s.last_set_at else None,
                }
            else:
                dev["session"] = None

    return {"devices": grouped}

@app.post("/api/set-location")
async def set_location(req: SetLocationRequest):
    if not req.device_udids:
        raise HTTPException(status_code=400, detail="No devices specified")

    results = []
    available = _get_cached_devices()
    available_udids = {d.udid for d in available}

    tunnel_running = tunneld_mgr.is_running()

    for udid in req.device_udids:
        if udid not in available_udids:
            results.append({"udid": udid, "success": False, "error": f"Device {udid} not found"})
            continue

        dev = next(d for d in available if d.udid == udid)

        with sessions_lock:
            existing = sessions.get(udid)
            if existing and existing.active:
                existing.stop_event.set()
                if existing.hold_thread and existing.hold_thread.is_alive():
                    existing.hold_thread.join(timeout=3)
                _finalize_history_entry(udid)

        use_tunnel = tunnel_running

        proc = _start_location_process(udid, req.lat, req.lng, use_tunnel=use_tunnel)

        if not proc and not use_tunnel:
            log.info("USB set failed for %s, trying tunnel...", udid)
            proc = _start_location_process(udid, req.lat, req.lng, use_tunnel=True)
            if proc:
                use_tunnel = True

        if not proc:
            proc = _start_location_process(udid, req.lat, req.lng, use_tunnel=True)
            if proc:
                use_tunnel = True

        if not proc:
            results.append({"udid": udid, "success": False, "error": "Failed to start location process"})
            continue

        method = "cli_tunnel" if use_tunnel else "cli_usb"
        now = datetime.now(timezone.utc)
        session = DeviceSession(
            udid=udid,
            device_name=dev.name,
            connection_mode="usb",
            lat=req.lat,
            lng=req.lng,
            use_tunnel=use_tunnel,
            last_set_at=now,
            last_enforced_at=now,
            enforce_count=1,
            location_proc=proc,
        )

        with history_lock:
            history.appendleft({
                "id": f"{udid}_{uuid_mod.uuid4().hex[:8]}",
                "lat": req.lat,
                "lng": req.lng,
                "device_name": dev.name,
                "device_udid": udid,
                "set_at": now.isoformat(),
                "stopped_at": None,
                "duration_seconds": None,
            })
        _save_history()
        _audit_log("location_set", {
            "udid": udid, "device_name": dev.name,
            "lat": req.lat, "lng": req.lng, "method": method, "use_tunnel": use_tunnel,
        })

        session.hold_thread = threading.Thread(
            target=_hold_location_loop, args=(session,), daemon=True,
        )
        session.hold_thread.start()

        with sessions_lock:
            sessions[udid] = session

        _save_state()
        results.append({
            "udid": udid,
            "success": True,
            "device_name": dev.name,
            "method": method,
        })

    await _broadcast({
        "type": "location_set",
        "lat": req.lat,
        "lng": req.lng,
        "results": results,
    })

    return {"success": any(r["success"] for r in results), "results": results}

@app.post("/api/stop")
async def stop_location(req: ForceStopRequest = None):
    target_udid = req.device_udid if req else None

    with sessions_lock:
        targets = (
            {target_udid: sessions[target_udid]}
            if target_udid and target_udid in sessions
            else dict(sessions)
        )

    stopped = []
    for udid, s in targets.items():
        if not s.active:
            continue
        s.stop_event.set()
        _kill_location_proc(s)
        if s.hold_thread and s.hold_thread.is_alive():
            s.hold_thread.join(timeout=3)

        _clear_location_cli(udid, use_tunnel=s.use_tunnel)
        _finalize_history_entry(udid)
        s.active = False
        stopped.append(udid)

    with sessions_lock:
        for udid in stopped:
            sessions.pop(udid, None)

    _save_state()
    _audit_log("location_stopped", {"devices": stopped})
    await _broadcast({"type": "location_cleared", "devices": stopped})
    return {"success": True, "stopped": stopped}

@app.post("/api/force-stop")
async def force_stop(req: ForceStopRequest = None):
    target_udid = req.device_udid if req else None

    with sessions_lock:
        targets = (
            {target_udid: sessions[target_udid]}
            if target_udid and target_udid in sessions
            else dict(sessions)
        )

    stopped = []
    for udid, s in targets.items():
        s.stop_event.set()
        _kill_location_proc(s)
        if s.hold_thread and s.hold_thread.is_alive():
            s.hold_thread.join(timeout=2)
        try:
            _clear_location_cli(udid, use_tunnel=s.use_tunnel)
        except Exception:
            pass
        _finalize_history_entry(udid)
        s.active = False
        stopped.append(udid)

    with sessions_lock:
        for udid in stopped:
            sessions.pop(udid, None)

    _save_state()
    _audit_log("location_force_stopped", {"devices": stopped})
    await _broadcast({"type": "location_cleared", "devices": stopped})
    return {"success": True, "stopped": stopped}

def _finalize_history_entry(udid: str):
    now = datetime.now(timezone.utc)
    with history_lock:
        for entry in history:
            if entry.get("device_udid") == udid and entry.get("stopped_at") is None:
                entry["stopped_at"] = now.isoformat()
                if entry.get("set_at"):
                    try:
                        sa = datetime.fromisoformat(entry["set_at"])
                        entry["duration_seconds"] = round((now - sa).total_seconds(), 1)
                    except Exception:
                        pass
                break
    _save_history()

@app.get("/api/status")
async def get_status():
    with sessions_lock:
        active = []
        for s in sessions.values():
            if s.active:
                active.append({
                    "udid": s.udid,
                    "device_name": s.device_name,
                    "connection_mode": s.connection_mode,
                    "lat": s.lat,
                    "lng": s.lng,
                    "use_tunnel": s.use_tunnel,
                    "last_set_at": s.last_set_at.isoformat() if s.last_set_at else None,
                    "last_enforced_at": s.last_enforced_at.isoformat() if s.last_enforced_at else None,
                    "enforce_count": s.enforce_count,
                    "enforce_failures": s.enforce_failures,
                    "consecutive_failures": s.consecutive_failures,
                    "reconnecting": s.reconnecting,
                    "reconnect_since": s.reconnect_since.isoformat() if s.reconnect_since else None,
                })
    return {
        "active_count": len(active),
        "sessions": active,
        "hold_interval": hold_interval,
        "reconnect_window": reconnect_window,
    }

@app.get("/api/tunneld/status")
async def tunneld_status():
    return tunneld_mgr.status()

@app.post("/api/tunneld/start")
async def tunneld_start():
    result = tunneld_mgr.start()
    if not result.get("success"):
        if result.get("needs_terminal"):
            raise HTTPException(status_code=403, detail=result.get("error", "Needs terminal"))
        raise HTTPException(status_code=500, detail=result.get("error", "Failed"))
    return result

@app.post("/api/tunneld/stop")
async def tunneld_stop():
    return tunneld_mgr.stop()

@app.post("/api/tunneld/launch-terminal")
async def tunneld_launch_terminal():
    return tunneld_mgr.launch_terminal()

@app.get("/api/settings")
async def get_settings():
    return {"hold_interval": hold_interval, "reconnect_window": reconnect_window}

@app.post("/api/settings")
async def update_settings(req: SettingsRequest):
    global hold_interval, reconnect_window
    if req.hold_interval is not None:
        if not (0.5 <= req.hold_interval <= 10.0):
            raise HTTPException(status_code=400, detail="hold_interval must be 0.5-10.0")
        hold_interval = req.hold_interval
    if req.reconnect_window is not None:
        if not (30 <= req.reconnect_window <= 3600):
            raise HTTPException(status_code=400, detail="reconnect_window must be 30-3600")
        reconnect_window = req.reconnect_window
    _save_state()
    return {"success": True, "hold_interval": hold_interval, "reconnect_window": reconnect_window}

@app.get("/api/locations")
async def list_locations():
    return {"locations": _load_saved_locations()}

@app.post("/api/locations")
async def save_location_endpoint(req: SaveLocationRequest):
    locations = _load_saved_locations()
    new_loc = {
        "id": str(uuid_mod.uuid4()),
        "name": req.name,
        "lat": req.lat,
        "lng": req.lng,
        "is_preset": False,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    locations.append(new_loc)
    _save_saved_locations(locations)
    _audit_log("location_saved", {"name": req.name, "lat": req.lat, "lng": req.lng, "id": new_loc["id"]})
    return new_loc

@app.delete("/api/locations/{location_id}")
async def delete_location(location_id: str):
    locations = _load_saved_locations()
    deleted = [loc for loc in locations if loc.get("id") == location_id]
    locations = [loc for loc in locations if loc.get("id") != location_id]
    _save_saved_locations(locations)
    if deleted:
        _audit_log("location_deleted", {"id": location_id, "name": deleted[0].get("name")})
    return {"success": True}

@app.get("/api/history")
async def get_history():
    with history_lock:
        return {"history": list(history)}

@app.delete("/api/history")
async def clear_history():
    with history_lock:
        count = len(history)
        history.clear()
    _save_history()
    _audit_log("history_cleared", {"count": count})
    return {"success": True}

@app.get("/api/previous-session")
async def get_previous_session():
    with sessions_lock:
        if sessions:
            active_sessions = {}
            for udid, s in sessions.items():
                if s.active:
                    active_sessions[udid] = {
                        "udid": s.udid,
                        "device_name": s.device_name,
                        "lat": s.lat,
                        "lng": s.lng,
                        "connection_mode": s.connection_mode,
                        "use_tunnel": s.use_tunnel,
                    }
            if active_sessions:
                return {"has_previous": True, "sessions": active_sessions, "is_live": True}

    prev = _load_previous_state()
    if prev and prev.get("sessions"):
        return {"has_previous": True, "sessions": prev["sessions"], "is_live": False}
    return {"has_previous": False}

def _snapshot_sessions() -> list[dict]:
    with sessions_lock:
        out = []
        for s in sessions.values():
            if s.active:
                out.append({
                    "udid": s.udid,
                    "device_name": s.device_name,
                    "lat": s.lat,
                    "lng": s.lng,
                    "connection_mode": s.connection_mode,
                    "use_tunnel": s.use_tunnel,
                    "last_set_at": s.last_set_at.isoformat() if s.last_set_at else None,
                    "last_enforced_at": s.last_enforced_at.isoformat() if s.last_enforced_at else None,
                    "enforce_count": s.enforce_count,
                    "enforce_failures": s.enforce_failures,
                    "consecutive_failures": s.consecutive_failures,
                    "reconnecting": s.reconnecting,
                    "reconnect_since": s.reconnect_since.isoformat() if s.reconnect_since else None,
                })
        return out

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    connected_ws.append(ws)

    async def heartbeat():
        try:
            while True:
                await asyncio.sleep(10)
                try:
                    await ws.send_json({
                        "type": "ping",
                        "ts": datetime.now(timezone.utc).isoformat(),
                    })
                except Exception:
                    return
        except asyncio.CancelledError:
            pass

    hb_task = asyncio.create_task(heartbeat())

    try:
        await ws.send_json({
            "type": "status",
            "sessions": _snapshot_sessions(),
            "active_count": len(_snapshot_sessions()),
            "hold_interval": hold_interval,
            "reconnect_window": reconnect_window,
            "tunneld": tunneld_mgr.status(),
        })
        while True:
            try:
                data = await ws.receive_text()
            except WebSocketDisconnect:
                break
            if not data:
                continue
            try:
                incoming = json.loads(data)
            except Exception:
                continue
            if incoming.get("type") == "resync":
                try:
                    await ws.send_json({
                        "type": "status",
                        "sessions": _snapshot_sessions(),
                        "active_count": len(_snapshot_sessions()),
                        "hold_interval": hold_interval,
                        "reconnect_window": reconnect_window,
                        "tunneld": tunneld_mgr.status(),
                    })
                except Exception:
                    break
    except WebSocketDisconnect:
        pass
    except Exception as e:
        log.debug("WebSocket error: %s", e)
    finally:
        hb_task.cancel()
        try:
            connected_ws.remove(ws)
        except ValueError:
            pass

async def _broadcast(msg: dict):
    dead = []
    for ws in connected_ws[:]:
        try:
            await ws.send_json(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        try:
            connected_ws.remove(ws)
        except ValueError:
            pass

if __name__ == "__main__":
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=PORT,
        log_level="info",
        reload=False,
    )
