from pyble.core import node
import threading
import struct
from io import RawIOBase
from pathlib import Path
from contextlib import ExitStack


OPT_FMT = 'QQQQ'
OPT_SIZE = struct.calcsize(OPT_FMT)


def open_store(name: str):
    file = open(name, 'rb+' if Path(name).exists() else 'wb+')  # type: RawIOBase
    opt = Store.opt_load(file)
    return Store(file,
                 stored=opt[0],
                 size=opt[1],
                 limit=opt[2],
                 previous=opt[3])


class Store:
    """Fast file storage for nodes.

    Format = info[32] + storage[1KB * size]

    """

    def __init__(self, buffer: RawIOBase, *, stored=0, size=16, limit=2**20, previous=None):
        self._buffer = buffer
        self._buf_lock = threading.RLock()  # always try to minimize time locked

        self._opt_lock = threading.RLock()
        self._stored = stored
        self._size = size
        self._limit = limit
        self._previous = previous  # previous size if currently resizing

    def opt_save(self):
        """Save the current options into the buffer."""
        b = struct.pack(OPT_FMT, self._stored, self._size, self._limit, self._previous)  # prepare bytes before lock
        with self._buf_lock:
            self._buffer.seek(0)
            self._buffer.write(b)

    @staticmethod
    def opt_load(buffer: RawIOBase):
        """Load options from the buffer."""
        buffer.seek(0)
        b = buffer.read(OPT_SIZE)
        return struct.unpack(OPT_FMT, b)

    def optimize(self, collide=2):
        pass

    def resize(self, size: int, progressive=True):
        pass

    def seek_block(self, i: int):
        """Seek to the ``i``th node."""
        with self._buf_lock:
            self._buffer.seek(OPT_SIZE + node.TOTAL_SIZE * i)

    @staticmethod
    def identify_block(b: bytes):
        try:
            return node.Node.from_bytes(b)
        except ValueError:
            pass  # todo

    def close(self):
        """Close the buffer and save the store's options."""
        with ExitStack() as es:
            es.enter_context(self._buf_lock)
            es.enter_context(self._opt_lock)
            self.opt_save()
            self._buffer.close()
