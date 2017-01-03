"""Node class module."""

import struct
from hashlib import sha512
from binascii import hexlify
from weakref import WeakSet


# CONSTANTS
DIGEST_SIZE = sha512(b'').digest_size
STORY_SIZE = DIGEST_SIZE * (2 ** 4 - 2)
TOTAL_SIZE = STORY_SIZE + 2*DIGEST_SIZE  # 1 Kilobyte
N_FMT = '%ss%ss%ss' % (DIGEST_SIZE, STORY_SIZE, DIGEST_SIZE)  # node format


# define quick hash
def q_hash(b: bytes): return sha512(b).digest()


class SyncError(Exception):
    pass


class Node:
    """Container for story branch."""

    def __init__(self, story: str, parent=None):
        if len(story) > STORY_SIZE:
            raise ValueError('story too long %s' % len(story))
        if '\0' in story:
            raise ValueError('null byte in story')

        if (parent is not None) and (not isinstance(parent, (self.__class__, bytes))):
            raise TypeError('invalid parent %r' % parent)

        self._story = story
        self._parent = parent

        # declare caches
        self._sid = None
        self._bytes = None

        # register parent if it exists
        if parent is not None:
            not_pis = parent not in self._all
            not_pic = parent not in self._children
            if not_pis & not_pic:
                self._all[parent] = parent
                self._children[parent] = WeakSet()
            elif not_pis ^ not_pic:
                raise SyncError('only one class dictionary contained parent')
            # add self to parent children
            self._children[parent].add(self)

        # register node
        self._all[self] = self
        if self not in self._children:
            self._children[self] = WeakSet()

    __slots__ = (
        '__weakref__',
        '_story',
        '_parent',
        '_sid',
        '_bytes'
    )

    _all = {}  # type: dict[Node, Node]
    _children = {}  # type: dict[Node, WeakSet]

    def __eq__(self, other):
        if isinstance(other, bytes):
            return other == self.sid  # perform id comparison if bytes
        elif isinstance(other, self.__class__):
            try:  # compare all unique attributes if node
                assert other.pid == self.pid
                assert other.story == self.story
                return True
            except AssertionError:
                return False
        else:
            return NotImplemented

    def __hash__(self):
        return hash(self.sid)

    @property
    def story(self):
        return self._story

    @property
    def parent(self):
        try:
            return self._all[self._parent]
        except KeyError:
            return self._parent

    @property
    def children(self):
        return self._children[self].copy()

    @property
    def sid(self) -> bytes:
        """Generate id for node."""
        # check cache first
        if self._sid is not None:
            return self._sid

        h = q_hash(self.pid + self.story.encode('utf-8'))
        assert len(h) == DIGEST_SIZE
        self._sid = h
        return h

    @property
    def pid(self) -> bytes:
        """Get parent id."""
        # determine parent id
        if self.parent is None:
            p_id = b''
        elif isinstance(self.parent, bytes):
            p_id = self.parent
        else:  # parent is Node
            p_id = self.parent.sid

        return p_id

    def __repr__(self):
        """Return representation with first 8 digits of id."""
        return '<%s : %s>' % (self.__class__.__qualname__, hexlify(self.sid)[:8].decode('utf-8'))

    def to_bytes(self) -> bytes:
        """Convert node into bytes.

        Format = parent_id[64] + story_section[896] + self_id[64]

        """
        # check cache first
        if self._bytes is not None:
            return self._bytes

        b = struct.pack(N_FMT, self.pid, self.story.encode('utf-8'), self.sid)
        assert len(b) == TOTAL_SIZE, 'incorrect node size %r' % self
        self._bytes = b
        return b

    @classmethod
    def from_bytes(cls, b: bytes):
        """Convert bytes into node."""

        parent, story, sid = struct.unpack(N_FMT, b)
        assert isinstance(parent, bytes)
        assert isinstance(story, bytes)
        assert isinstance(sid, bytes)
        story = story.rstrip(b'\0')  # remove null byte padding

        n = cls(story.decode('utf-8'), parent if parent != b'\0'*DIGEST_SIZE else None)
        if sid != n.sid:  # verify node sid
            raise ValueError('attempt to unpack invalid node %r != %r' % (sid, n.sid))
        return n

    ###################
    # node operations #
    ###################

    def branch(self, story: str):
        """Create and return a child node."""
        node = self.__class__(story, self)
        self._children[self].add(node)
        return node

    def retrace(self, stop=None) -> list:
        """Retrace all nodes from current to ``stop``.

        Order of list is ``[self, ..., stop]``.

        Note:
            A retrace does not use recursive function calls and is therefore not limited by the Python stack.

        """
        if (stop is not None) and (not isinstance(stop, (self.__class__, bytes))):
            raise TypeError('stop must be Node or None %r' % stop)

        current = self
        l = [current]
        while current.parent != stop and not isinstance(current.parent, bytes):
            if current.parent is None:  # handle if stop was not in trace
                break
            current = current.parent  # get previous node
            l.append(current)

        return l
