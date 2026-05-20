# Location+

**A local, no-jailbreak iOS GPS spoofer with a Leaflet web UI, built for macOS.**

Plug your iPhone in via USB, open your browser, drop a pin, and your iPhone reports that coordinate to every app on the device until you stop. Works on iOS 16 over USB and on iOS 17+ over Apple's CoreDevice tunnel (`tunneld`). Everything runs on your own machine — no accounts, no cloud, nothing.

![Python](https://img.shields.io/badge/Python-3.13+-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.104+-green)
![Platform](https://img.shields.io/badge/Platform-macOS-lightgrey)

---

## Overview

Location+ is a project that pairs a **FastAPI + WebSocket** Python backend with a **single-file vanilla-JS frontend** (Leaflet, dark/light theme, service-worker offline support) to drive `[pymobiledevice3](https://github.com/doronz88/pymobiledevice3)` — the open-source Python re-implementation of Apple's Mobile Device protocols.

The hard part is *keeping* the spoof *pinned*: Apple's `simulate-location` is held only while a controlling process stays alive, and on iOS 17+ that controlling process has to talk through a privileged tunnel. Location+ wraps both, plus accounts for when the device or laptop sleeps, the cable disconnects, or the tunnel dies mid-session for whatever reason.

---

## Why I Built This

My friends and I all share our location with each other via Apple's Find My, and occasionally when someone is crossing a bridge, or near a body of water whilst having spotty service, the location accuracy is way off, and will show someone being deep in the body of water. Sometimes when this happens, some of us will text each other, jokingly asking if we're going for a swim. I figured it would be funny if when checking everyone elses location, someone saw my location in some random place, like Antarctica, Mount Everest, or the middle of the Atlantic Ocean. It was fun to make, and cool to see everyone ask why, how, and what I'm doing in Antarctica.

---

## Features

- **Pin-and-hold GPS spoofing** for one or more iPhones at a time, with the spoofed coordinate continually re-asserted in a per-device background thread.
- **Automatic USB-to-tunnel fallback.** If a USB `simulate-location` call returns a retryable error (`tunneld`, `connection refused`, `EOF`, timeout, ...), the request is silently retried with `--tunnel <UDID>` .
- **Built-in tunnel management** with a "Launch in Terminal" escape hatch (via `osascript`) for when `sudo` needs an interactive password.
- **Live UI updates over WebSocket** — push counters, failure counters, reconnect badges, and connection mode (USB / Tunnel / WiFi) stream in real time.
- **Wake-from-sleep & network-flap recovery.** The hold loop detects wall-clock drift over its expected wait and refreshes the controlling subprocess; the frontend has `visibilitychange`, `focus`, `pageshow`, `online`/`offline`, and a timer-drift check that all converge on a single resync path.
- **Session persistence and resume.** Active sessions, the 50-entry history, saved bookmarks, and slider settings all live in `~/.locationplus/` and survive server restarts. If a session was active when the server stopped, a "Resume" banner offers to pick it back up.
- **Atomic JSON writes** with `tempfile` -> `fsync` -> `os.replace` and a `.bak` snapshot, so a crash mid-write can't corrupt state.
- **Append-only JSONL audit log** of every mutating event in `~/.locationplus/.internal/.audit/` .
- **Map UX:** Leaflet + CartoDB tiles, dark/light themes with CSS-var swap, draggable pin, right-click context menu, live cursor coordinates, search via OpenStreetMap Nominatim, keyboard shortcuts (Enter = set, Esc = stop).
- **Service-worker offline support.** Three named caches (app shell, CDN libs, map tiles) with stale-while-revalidate / cache-first / network-first routing, plus a `postMessage`-driven batch tile-prefetch protocol.
- **One-click launcher** for macOS — `locationctl.command` creates the venv, prompts once for sudo, starts the tunnel and the server, opens the browser, and cleans both up on exit.

---

## Tech Stack


| Layer        | Tech                                                                                     |
| ------------ | ---------------------------------------------------------------------------------------- |
| Backend      | Python 3.13, FastAPI, uvicorn, pydantic                                                  |
| iOS bridge   | [pymobiledevice3](https://github.com/doronz88/pymobiledevice3) (invoked as a subprocess) |
| Frontend     | Vanilla HTML / CSS / JS in a single file                                                 |
| Map          | Leaflet 1.9.4, CartoDB tiles                                                             |
| Geocoding    | OpenStreetMap Nominatim                                                                  |
| Offline      | Service Worker + Cache Storage API                                                       |
| Live updates | WebSocket with heartbeat + auto-reconnect                                                |
| Launcher     | Bash (`.command`), with macOS-specific tooling (`osascript`, `lsof`, `open`, `sudo`)     |


---

## Architecture

```
Browser (static/index.html)
   | HTTP + WebSocket on :8042
FastAPI server (server.py)
   | subprocess (CLI)
pymobiledevice3
   | USB + iOS 17+ CoreDevice tunnel
iPhone (Developer Mode)
```

---

## How It Works

The key to a *persistent* spoof is that Apple's location simulation only lasts as long as the controlling process. Location+ runs `pymobiledevice3 developer dvt simulate-location set --udid <UDID> -- <lat> <lng>` (or `--tunnel <UDID>` on iOS 17+) and **leaves the subprocess alive**. A dedicated thread per device monitors that subprocess:

1. If the subprocess is alive, count one successful enforcement, then sleep `hold_interval` seconds (default 1.0 s) — and every 5 minutes, proactively kill + restart the subprocess to prevent stale connections **preemptively**, so that there is no gap in location being spoofed.
2. If it's dead, retry on the same transport; fall back from USB to tunnel; on every third consecutive failure, run a Wi-Fi browse (`pymobiledevice3 remote browse`) to see if the device is reachable that way and reconnect over the network.
3. Every iteration emits a JSON event over the WebSocket so the UI can update the per-device push count, failure count, and "Reconnecting..." badge live.

On laptop wake, the loop notices that the actual sleep duration exceeded its expected wait by more than 10 seconds, kills its (definitely stale) child, and starts fresh. The browser side has a matching `onWake()` that force-reconnects the WebSocket and refetches device + session state.

---

## Installation / Setup

### Prerequisites

- macOS (Apple Silicon or Intel).
- Python 3.13+ (`brew install python@3.13`).
- An iPhone with **Developer Mode** enabled (Settings > Privacy & Security > Developer Mode).
- A USB data cable + click "Trust This Computer" on first connect.

### One-shot launcher

Double-click the `locationctl.command` file in Finder. It creates the venv, asks for your `sudo` password once, starts `tunneld` and the server, waits for both ports, opens the browser, and cleans up on Ctrl-C exit.

---

## Usage

1. Open `http://localhost:8042`.
2. Plug in the iPhone; tap "Trust" if prompted.
3. Wait for the device card to appear in the sidebar (or click **Scan**).
4. Pick a target location by doing any of the following: clicking the map, typing latitude/longitude, searching via search bar, clicking a saved bookmark, or right-click > "Pin location here".
5. Click **Set Device Location** (or press **Enter**).
6. Click **Stop All & Restore GPS** (or press **Escape**) at any time to release the spoof.

### Right-click on the map

- Pin location here
- Save this location (prompts for a name)
- Copy coordinates

