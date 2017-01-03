"""TCP hole punching and peer communication."""

import socket
import threading
from typing import Callable


class Error(Exception):
    pass


class Tunnel:
    """High-level packet transmission."""


class Listener:
    """Automatic tunnel creation on port."""

    def __init__(self, port: int, backlog: int = None):
        self._sock = socket.socket()
        self._sock.bind(('', port))
        self._sock.listen(backlog)

        self._lock = threading.RLock()
        self._running = False
        self._thread = None  # type: threading.Thread

    @property
    def running(self):
        """Return whether listener is currently running."""
        with self._lock:
            return self._running and (self._thread is not None)

    def start(self, handler: Callable):
        """Spawn thread to act as listener."""
        with self._lock:
            if self._thread is not None:
                raise Error('listener already started')
            self._running = True
            self._thread = threading.Thread(target=self._loop, args=(handler,), daemon=True)
            self._thread.start()

    def _loop(self, handler: Callable):
        """Spawn new threads to handle incoming connections."""
        while self.running:
            threading.Thread(target=handler, args=(self._sock.accept(),)).start()

    def stop(self):
        with self._lock:
            self._running = False
            self._thread.join()
            self._thread = None
