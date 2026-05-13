"""Silent Windows launcher for GENRRI Trading View.

Double-click → starts the Flask server in the background with no console
window, waits for it to bind port 5001, then opens the app in the default
browser. If the server is already running, just opens the browser.
"""
import os
import socket
import subprocess
import sys
import time
import webbrowser

APP_DIR = os.path.dirname(os.path.abspath(__file__))
URL = "http://127.0.0.1:5001"
PORT = 5001


def port_bound(port=PORT, host="127.0.0.1"):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(0.5)
    try:
        s.connect((host, port))
        return True
    except OSError:
        return False
    finally:
        s.close()


def spawn_server():
    # No console window, detached so it survives this launcher script.
    CREATE_NO_WINDOW = 0x08000000
    DETACHED_PROCESS = 0x00000008
    # sys.executable when running under pythonw.exe is pythonw.exe — exactly
    # what we want for the child too.
    subprocess.Popen(
        [sys.executable, "app.py"],
        cwd=APP_DIR,
        creationflags=CREATE_NO_WINDOW | DETACHED_PROCESS,
        close_fds=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def main():
    if not port_bound():
        spawn_server()
        # Poll up to 60s for the server to bind
        for _ in range(60):
            time.sleep(1)
            if port_bound():
                break
    webbrowser.open(URL)


if __name__ == "__main__":
    main()
