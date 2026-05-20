#!/bin/bash

cd "$(dirname "$0")" || exit 1

printf '\033]0;Location+ by Habib for Ishtat <3\007'

GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
RESET='\033[0m'

status()  { echo -e "${GREEN}[✓]${RESET} $1"; }
info()    { echo -e "${CYAN}[·]${RESET} $1"; }
warn()    { echo -e "${YELLOW}[!]${RESET} $1"; }
fail()    { echo -e "${RED}[✗]${RESET} $1"; }

TUNNELD_PID=""
SERVER_PID=""
SUDO_LOOP_PID=""
TUNNELD_STARTED_BY_US=false

cleanup() {
    echo ""
    info "Shutting down..."

    if [ -n "$SUDO_LOOP_PID" ]; then
        kill "$SUDO_LOOP_PID" 2>/dev/null
        wait "$SUDO_LOOP_PID" 2>/dev/null
    fi

    if [ -n "$SERVER_PID" ]; then
        kill "$SERVER_PID" 2>/dev/null

        for i in 1 2 3; do
            kill -0 "$SERVER_PID" 2>/dev/null || break
            sleep 1
        done
        kill -9 "$SERVER_PID" 2>/dev/null
        wait "$SERVER_PID" 2>/dev/null
    fi

    if [ "$TUNNELD_STARTED_BY_US" = true ] && [ -n "$TUNNELD_PID" ]; then
        info "Stopping tunneld (pid $TUNNELD_PID)..."
        sudo kill "$TUNNELD_PID" 2>/dev/null
        for i in 1 2 3; do
            sudo kill -0 "$TUNNELD_PID" 2>/dev/null || break
            sleep 1
        done
        sudo kill -9 "$TUNNELD_PID" 2>/dev/null
    fi

    sudo -k 2>/dev/null

    status "Shutdown complete."
    echo -e "${DIM}You can close this window.${RESET}"
}

trap cleanup EXIT INT TERM

echo ""
echo -e "${BOLD}${CYAN}  ╔═══════════════════════════════╗${RESET}"
echo -e "${BOLD}${CYAN}  ║       L o c a t i o n         ║${RESET}"
echo -e "${BOLD}${CYAN}  ║     made with luv by habib    ║${RESET}"
echo -e "${BOLD}${CYAN}  ╚═══════════════════════════════╝${RESET}"
echo ""

PYTHON=""
if command -v python3.13 &>/dev/null; then
    PYTHON="python3.13"
elif command -v python3 &>/dev/null; then
    PYTHON="python3"
else
    fail "Python 3 not found. Install with: brew install python@3.13"
    exit 1
fi

if [ ! -f ".venv/bin/python" ]; then
    if [ -d ".venv" ]; then
        warn "Broken venv detected, recreating..."
        rm -rf .venv
    fi
    info "Creating virtual environment..."
    $PYTHON -m venv .venv || { fail "Failed to create venv"; exit 1; }
    info "Installing dependencies..."
    .venv/bin/pip install --quiet -r requirements.txt || { fail "Failed to install dependencies"; exit 1; }
    status "Environment ready"
else
    status "Virtual environment found"
fi

if nc -z 127.0.0.1 8042 2>/dev/null; then
    warn "Location+ is already running on port 8042"
    open http://localhost:8042
    info "Opened browser to existing instance."

    trap - EXIT INT TERM
    exit 0
fi

echo -e "${BOLD}Tunneld requires administrator privileges.${RESET}"
echo -e "${DIM}Enter your password below (you won't need to type it again):${RESET}"
echo ""
sudo -v || { fail "Authentication failed or cancelled."; exit 1; }

(while true; do sudo -n -v 2>/dev/null; sleep 50; done) &
SUDO_LOOP_PID=$!

if nc -z 127.0.0.1 49151 2>/dev/null; then
    status "tunneld already running"
else
    info "Starting tunneld..."
    sudo .venv/bin/python -m pymobiledevice3 remote tunneld > /dev/null 2>&1 &
    TUNNELD_PID=$!
    TUNNELD_STARTED_BY_US=true

    TUNNELD_READY=false
    for i in $(seq 1 30); do
        if nc -z 127.0.0.1 49151 2>/dev/null; then
            TUNNELD_READY=true
            break
        fi
        sleep 1
    done

    if [ "$TUNNELD_READY" = true ]; then
        status "tunneld is ready"
    else
        warn "tunneld did not start in 30s — continuing without it"
        warn "Location spoofing may still work over USB"
    fi
fi

info "Starting Location+ server..."
.venv/bin/python server.py &
SERVER_PID=$!

SERVER_READY=false
for i in $(seq 1 15); do
    if nc -z 127.0.0.1 8042 2>/dev/null; then
        SERVER_READY=true
        break
    fi

    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        fail "Server failed to start. Check the output above for errors."
        exit 1
    fi
    sleep 1
done

if [ "$SERVER_READY" = true ]; then
    status "Server is ready"
else
    fail "Server did not start in 15s"
    exit 1
fi

open http://localhost:8042
status "Opened browser"

echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo -e "${BOLD}${GREEN}  Location+ is running${RESET}"
echo -e "${DIM}  http://localhost:8042${RESET}"
echo -e "${DIM}  Press Ctrl+C or close this window to stop${RESET}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo ""

wait $SERVER_PID
