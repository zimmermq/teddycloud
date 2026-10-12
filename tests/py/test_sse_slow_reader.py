"""
A subscriber that reads its SSE stream slowly, with pauses shorter than the
SSE send timeout, must keep its subscription and get every event intact.

    make test_py TESTS=sse_slow_reader
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

EVENTS = 3000
# below SSE_SEND_TIMEOUT_MS (2 s): the reader is slow, never stuck
PAUSE_S = 1.0
SEQUENCE = re.compile(r'"sequence":\s*(\d+)')

pytestmark = pytest.mark.skipif(not SANDBOX or not HTTPS_API_PORT, reason="needs the sandbox from tests/py/with_server.sh")


class SlowReader:
    """Reads 4 KB at a time with a pause every 50 reads, and checks the chunked framing as it goes."""

    def __init__(self):
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        self.sock.connect((BASE.hostname, BASE.port))
        self.sock.sendall(f"GET /api/sse HTTP/1.1\r\nHost: {BASE.hostname}:{BASE.port}\r\n\r\n".encode())
        self.sock.settimeout(60)
        self.raw = b""
        self.body = b""
        self.framing_error = None
        self.closed = False
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        reads = 0
        while True:
            try:
                chunk = self.sock.recv(4096)
            except OSError:
                chunk = b""
            if not chunk:
                self.closed = True
                return
            self.raw += chunk
            self._dechunk()
            reads += 1
            time.sleep(PAUSE_S if reads % 50 == 0 else 0.02)

    def _dechunk(self):
        if b"\r\n\r\n" in self.raw and not self.raw.startswith(b"HTTP/1.1 200"):
            self.framing_error = f"bad status line: {self.raw[:40]!r}"
            return
        head, sep, rest = self.raw.partition(b"\r\n\r\n")
        if not sep:
            return
        while True:
            size_line, sep, after = rest.partition(b"\r\n")
            if not sep:
                break
            try:
                size = int(size_line, 16)
            except ValueError:
                self.framing_error = f"bad chunk size {size_line[:20]!r}"
                return
            if len(after) < size + 2:
                break
            if after[size:size + 2] != b"\r\n":
                self.framing_error = "chunk not terminated by CRLF"
                return
            self.body += after[:size]
            rest = after[size + 2:]
        self.raw = head + b"\r\n\r\n" + rest

    def sequences(self):
        text = self.body.decode(errors="replace")
        return [int(m.group(1)) for event in text.split("\n\n") if event.startswith("event: rtnl-raw-log2")
                for m in [SEQUENCE.search(event)] if m]


def test_a_slow_subscriber_keeps_its_stream():
    box = Box("deadbeef0006", SANDBOX, BASE.hostname, HTTPS_API_PORT)
    reader = SlowReader()
    time.sleep(0.5)
    stream = box.rtnl(keep_open=True)
    stream.settimeout(120)
    try:
        for sequence in range(1, EVENTS + 1):
            stream.sendall(rtnl.frame(rtnl.volume_changed(5, -20, sequence=sequence)))
        deadline = time.time() + 120
        while time.time() < deadline and not reader.framing_error and not reader.closed:
            seen = reader.sequences()
            if seen and seen[-1] == EVENTS:
                break
            time.sleep(0.2)

        assert not reader.framing_error, f"the slow subscriber's stream broke: {reader.framing_error}"
        assert not reader.closed, "the slow subscriber was dropped"
        assert reader.sequences() == list(range(1, EVENTS + 1)), "the slow subscriber missed events"
    finally:
        stream.close()
        reader.sock.close()
        box.close()
