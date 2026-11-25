import threading, time, uvicorn
from app import app
_server = None
_thread = None
def start_server():
    global _server, _thread
    if _thread and _thread.is_alive(): return "already-running"
    _server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=8765, log_level="warning"))
    _thread = threading.Thread(target=_server.run, daemon=True)
    _thread.start()
    for _ in range(40):
        if _server.started: return "started"
        time.sleep(.25)
    return "starting"
