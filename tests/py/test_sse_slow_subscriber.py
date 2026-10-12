"""
An SSE subscriber that stops reading must not hold up the others: events are
written to every subscriber with the SSE locks held, so each blocked write
delays all subscribers and the box's RTNL connection.

    make test_py TESTS=sse_slow_subscriber
"""

import os
import re
import socket
import sys
import threading
import time
from pathlib import Path
from urllib.parse import urlparse

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sim import rtnl  # noqa: E402
from sim.box import Box  # noqa: E402

BASE = urlparse(os.environ.get("TEDDYCLOUD_BASE_URL", "http://127.0.0.1:80"))
SANDBOX = os.environ.get("TC_SANDBOX")
HTTPS_API_PORT = int(os.environ.get("TEDDYCLOUD_HTTPS_API_PORT", "0"))

# A write to the stalled subscriber may block for up to two SSE send timeouts
# (SSE_SEND_TIMEOUT_MS, 2 s): one that still gets part of the data out, one that
# gets nothing out and fails. Longer means it held things up again; without a
# send timeout of its own the wait is the 60 s HTTP send timeout, long enough
# for the other subscribers to be dropped as dead (SSE_TIMEOUT_S).
MAX_DELAY_S = 10
SEQUENCE = re.compile(rb'"sequence":\s*(\d+)')

pytestmark = pytest.mark.skipif(not SANDBOX or not HTTPS_API_PORT, reason="needs the sandbox from tests/py/with_server.sh")


def subscribe(rcvbuf=None):
    sock = socket.socket()
    if rcvbuf:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
    sock.connect((BASE.hostname, BASE.port))
    sock.sendall(f"GET /api/sse HTTP/1.1\r\nHost: {BASE.hostname}:{BASE.port}\r\n\r\n".encode())
    return sock


class Reader:
    """Reads an SSE stream and notes when each event sequence first arrives."""

    def __init__(self):
        self.sock = subscribe()
        self.seen = {}
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        buf = b""
        while True:
            try:
                chunk = self.sock.recv(65536)
            except OSError:
                return
            if not chunk:
                return
            buf = (buf + chunk)[-65536:]
            now = time.time()
            for m in SEQUENCE.finditer(buf):
                self.seen.setdefault(int(m.group(1)), now)

    def delay(self, stream, sequence):
        sent = time.time()
        stream.sendall(rtnl.frame(rtnl.tag_placed(1, sequence=sequence)))
        while sequence not in self.seen:
            if time.time() - sent > MAX_DELAY_S + 5:
                return None
            time.sleep(0.02)
        return self.seen[sequence] - sent


def test_a_stalled_subscriber_does_not_hold_up_the_others():
    box = Box("deadbeef0005", SANDBOX, BASE.hostname, HTTPS_API_PORT)
    reader = Reader()
    stalled = subscribe(rcvbuf=2048)
    stalled.recv(64)  # the response header; after that it never reads again
    time.sleep(0.5)
    stream = box.rtnl(keep_open=True)
    stream.settimeout(MAX_DELAY_S + 10)
    try:
        assert reader.delay(stream, 1) is not None, "no event reached the reading subscriber at all"

        # fill the stalled subscriber's buffers until a write to it blocks
        sequence = 100
        delays = []
        for portion in range(1, 41):
            for _ in range(500):
                sequence += 1
                stream.sendall(rtnl.frame(rtnl.volume_changed(5, -20, sequence=sequence)))
            delays.append(reader.delay(stream, 100000 + portion))
            if delays[-1] is None or delays[-1] > 1:
                break
        for i in range(3):
            delays.append(reader.delay(stream, 200000 + i))

        slowest = None if None in delays else max(delays)
        assert slowest is not None and slowest < MAX_DELAY_S, (
            f"events reached the reading subscriber after {['>%ds' % (MAX_DELAY_S + 5) if d is None else '%.1fs' % d for d in delays]}"
        )
    finally:
        stream.close()
        stalled.close()
        reader.sock.close()
        box.close()
