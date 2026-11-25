import threading, time, traceback, uvicorn
_server = None
_thread = None
_error = None

def start_server():
    global _server, _thread, _error
    if _thread and _thread.is_alive():
        if _server and _server.started:
            return "started"
        if _error:
            return "ERROR: " + _error
    try:
        from app import app
        _server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=8765, log_level="error"))
        _thread = threading.Thread(target=_server.run, daemon=True)
        _thread.start()
        for _ in range(80):
            if _server.started:
                return "started"
            if not _thread.is_alive():
                break
            time.sleep(.25)
        return "ERROR: server thread did not start in time"
    except Exception:
        _error = traceback.format_exc()
        return "ERROR: " + _error
