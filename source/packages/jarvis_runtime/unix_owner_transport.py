"""Attach-only Linux Unix/WebSocket transport for one already-pinned Codex owner.

No process creation, server bootstrap, socket cleanup, chmod or credential handling.
The pin is read-only launch evidence, not a promise that the execution survives.
Codex 0.159.2 speaks RFC6455 over its Unix listener; its proxy is a raw relay.
"""
from __future__ import annotations

import base64
from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import secrets
import socket
import stat
import struct
import threading
import time


class OwnerTransportError(RuntimeError):
    def __init__(self, kind: str, *, may_have_sent: bool = False):
        super().__init__("Unix owner transport unavailable: " + kind)
        self.kind = kind
        self.may_have_sent = may_have_sent
        self.execution_unknown = True


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                     allow_nan=False).encode()).hexdigest()


def _process_identity(pid: int) -> dict:
    if type(pid) is not int or pid <= 0:
        raise OwnerTransportError('invalid_owner_pid')
    root = Path('/proc') / str(pid)
    # comm may contain spaces and ')'; starttime is field 22, after final ')'.
    fields = (root / 'stat').read_text().rsplit(')', 1)[1].split()
    if fields[0] in {'Z', 'X', 'x'}:
        raise OwnerTransportError('owner_exited')
    return {'pid': pid, 'start_ticks': int(fields[19]),
            'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
            'pid_namespace': os.readlink(root / 'ns/pid'),
            'mount_namespace': os.readlink(root / 'ns/mnt'),
            'uid': root.stat().st_uid, 'executable': os.readlink(root / 'exe')}


def _socket_identity(path: str) -> dict:
    p = Path(path)
    if not p.is_absolute() or str(p) != path or '..' in p.parts:
        raise OwnerTransportError('invalid_socket_path')
    link = p.lstat()
    target = p.resolve(strict=True)
    info = target.stat()
    if not stat.S_ISSOCK(info.st_mode):
        raise OwnerTransportError('not_a_socket')
    # Never weaken existing permissions. A cross-user-writable socket is not an
    # eligible owner endpoint. Directory ownership is enforced by native startup.
    if info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise OwnerTransportError('socket_not_private')
    return {'path': path, 'resolved_path': str(target),
            'path_dev': link.st_dev, 'path_ino': link.st_ino,
            'path_ctime_ns': link.st_ctime_ns,
            'socket_dev': info.st_dev, 'socket_ino': info.st_ino,
            'socket_ctime_ns': info.st_ctime_ns, 'socket_uid': info.st_uid,
            'socket_mode': stat.S_IMODE(info.st_mode)}


@dataclass(frozen=True)
class OwnerEndpoint:
    """Exact locally observed process/socket identity; persist to_dict() at launch.

    capture() only reads metadata. The expected PID must come from the explicitly
    owned launch, not a guessed PID or stale health projection. Cross-namespace
    peers that cannot be identified are rejected. Pins are never refreshed by
    reconnect. instance_id is a digest of kernel identity, not a server token.
    """
    process_json: str
    socket_json: str
    instance_id: str

    @classmethod
    def capture(cls, path: str, pid: int) -> 'OwnerEndpoint':
        try:
            process = _process_identity(pid)
            endpoint = _socket_identity(path)
            if process['uid'] != os.getuid():
                raise OwnerTransportError('owner_uid_mismatch')
            value = {'process': process, 'socket': endpoint}
            return cls(json.dumps(process, sort_keys=True),
                       json.dumps(endpoint, sort_keys=True), _digest(value))
        except (OSError, ValueError, IndexError) as exc:
            raise OwnerTransportError('identity_unavailable') from exc

    def to_dict(self):
        return {'schema': 'jarvis-unix-owner/v1', 'process': json.loads(self.process_json),
                'socket': json.loads(self.socket_json), 'instance_id': self.instance_id}

    @classmethod
    def from_dict(cls, value):
        if not isinstance(value, dict) or set(value) != {'schema', 'process', 'socket', 'instance_id'}:
            raise OwnerTransportError('invalid_pin')
        if value['schema'] != 'jarvis-unix-owner/v1':
            raise OwnerTransportError('invalid_pin')
        obj = cls(json.dumps(value['process'], sort_keys=True),
                  json.dumps(value['socket'], sort_keys=True), value['instance_id'])
        obj.verify()
        return obj

    def verify(self, peer: socket.socket | None = None):
        try:
            process, endpoint = json.loads(self.process_json), json.loads(self.socket_json)
            if self.instance_id != _digest({'process': process, 'socket': endpoint}):
                raise OwnerTransportError('instance_pin_mismatch')
            observed = OwnerEndpoint.capture(endpoint['path'], process['pid'])
            if observed.instance_id != self.instance_id:
                raise OwnerTransportError('owner_identity_changed')
            if peer is not None:
                pid, uid, _gid = struct.unpack('3i', peer.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
                if pid != process['pid'] or uid != process['uid']:
                    raise OwnerTransportError('peer_identity_mismatch')
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise OwnerTransportError('identity_unavailable') from exc


class UnixOwnerConnection:
    MAX_MESSAGE = 8 * 1024 * 1024
    MAX_HEADER = 16384
    MAX_FRAGMENTS = 1024
    MESSAGE_SECONDS = 10.0

    def __init__(self, owner: OwnerEndpoint):
        self.owner = owner
        self.socket = None
        self.closed = threading.Event()
        self.failure_kind = None
        self._buffer = bytearray()
        self._send_lock = threading.Lock()

    def connect(self, *, deadline: float):
        if self.socket is not None or self.closed.is_set():
            raise OwnerTransportError('connection_already_used')
        self.owner.verify()
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket = s
        try:
            s.settimeout(max(.001, min(deadline - time.monotonic(), 5.0)))
            if time.monotonic() >= deadline:
                raise OwnerTransportError('handshake_timeout')
            s.connect(json.loads(self.owner.socket_json)['path'])
            self.owner.verify(s)
            s.settimeout(.1)
            key = base64.b64encode(secrets.token_bytes(16)).decode('ascii')
            request = (f'GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\n'
                       f'Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n'
                       'Sec-WebSocket-Version: 13\r\n\r\n').encode('ascii')
            self._send_bytes(request, deadline)
            while b'\r\n\r\n' not in self._buffer:
                if len(self._buffer) >= self.MAX_HEADER:
                    raise OwnerTransportError('handshake_header_limit')
                self._receive(deadline)
            header, rest = bytes(self._buffer).split(b'\r\n\r\n', 1)
            if len(header) + 4 > self.MAX_HEADER:
                raise OwnerTransportError('handshake_header_limit')
            self._buffer = bytearray(rest)
            lines = header.decode('ascii').split('\r\n')
            if lines[0] != 'HTTP/1.1 101 Switching Protocols':
                raise OwnerTransportError('invalid_handshake_status')
            headers = {}
            for line in lines[1:]:
                name, sep, value = line.partition(':')
                if not sep or not name or name.strip() != name or name.lower() in headers:
                    raise OwnerTransportError('invalid_handshake_headers')
                headers[name.lower()] = value.strip()
            accept = base64.b64encode(hashlib.sha1((key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
            if (headers.get('upgrade', '').lower() != 'websocket'
                    or 'upgrade' not in [t.strip().lower() for t in headers.get('connection', '').split(',')]
                    or headers.get('sec-websocket-accept') != accept
                    or 'sec-websocket-extensions' in headers
                    or 'sec-websocket-protocol' in headers):
                raise OwnerTransportError('invalid_handshake_headers')
            self.owner.verify(s)  # Pin is unchanged across handshake and path races.
        except BaseException as exc:
            self.close()
            if isinstance(exc, OwnerTransportError):
                raise
            raise OwnerTransportError('handshake_failed') from exc

    def _receive(self, deadline):
        while not self.closed.is_set():
            if time.monotonic() >= deadline:
                raise OwnerTransportError('read_deadline')
            try:
                chunk = self.socket.recv(65536)
            except socket.timeout:
                continue
            except OSError as exc:
                raise OwnerTransportError('connection_lost') from exc
            if not chunk:
                raise OwnerTransportError('eof')
            self._buffer.extend(chunk)
            return
        raise OwnerTransportError('connection_closed')

    def _take(self, size, deadline):
        while len(self._buffer) < size:
            self._receive(deadline)
        out = bytes(self._buffer[:size]); del self._buffer[:size]
        return out

    def _send_bytes(self, value, deadline):
        pos = 0
        while pos < len(value):
            if self.closed.is_set() or time.monotonic() >= deadline:
                raise OwnerTransportError('write_unavailable', may_have_sent=pos > 0)
            try:
                count = self.socket.send(value[pos:])
            except socket.timeout:
                continue
            except OSError as exc:
                raise OwnerTransportError('write_lost', may_have_sent=True) from exc
            if not count:
                raise OwnerTransportError('write_eof', may_have_sent=True)
            pos += count

    def _send_frame(self, payload, opcode, deadline):
        if len(payload) > self.MAX_MESSAGE:
            raise OwnerTransportError('message_limit')
        mask = secrets.token_bytes(4)
        size = len(payload)
        head = bytes([0x80 | opcode])
        head += (bytes([0x80 | size]) if size < 126 else
                 b'\xfe' + struct.pack('!H', size) if size < 65536 else
                 b'\xff' + struct.pack('!Q', size))
        wire = head + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        with self._send_lock:
            self._send_bytes(wire, deadline)

    def send_text(self, text: str, *, deadline: float):
        self.owner.verify(self.socket)
        self._send_frame(text.encode('utf-8'), 1, deadline)

    def receive_text(self):
        # Idle is allowed. Once any frame starts, the entire fragmented message
        # has one bounded deadline, including interleaved control frames.
        while not self._buffer:
            if self.closed.is_set():
                raise OwnerTransportError('connection_closed')
            try:
                self._receive(time.monotonic() + .2)
            except OwnerTransportError as exc:
                if exc.kind != 'read_deadline':
                    raise
        deadline = time.monotonic() + self.MESSAGE_SECONDS
        chunks = bytearray(); fragmented = False
        for _ in range(self.MAX_FRAGMENTS):
            a, b = self._take(2, deadline)
            final, opcode = bool(a & 0x80), a & 0x0f
            if a & 0x70 or b & 0x80 or opcode not in {0, 1, 8, 9, 10}:
                raise OwnerTransportError('protocol_frame')
            length = b & 0x7f
            if opcode >= 8 and (not final or length > 125):
                raise OwnerTransportError('protocol_control')
            if length == 126:
                length = struct.unpack('!H', self._take(2, deadline))[0]
                if length < 126:
                    raise OwnerTransportError('protocol_length')
            elif length == 127:
                length = struct.unpack('!Q', self._take(8, deadline))[0]
                if length < 65536 or length & (1 << 63):
                    raise OwnerTransportError('protocol_length')
            if length > self.MAX_MESSAGE or len(chunks) + length > self.MAX_MESSAGE:
                raise OwnerTransportError('message_limit')
            payload = self._take(length, deadline)
            if opcode == 8:
                if len(payload) == 1:
                    raise OwnerTransportError('protocol_close')
                if payload:
                    code = struct.unpack('!H', payload[:2])[0]
                    if code not in {1000, 1001, 1002, 1003, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014} and not 3000 <= code <= 4999:
                        raise OwnerTransportError('protocol_close')
                    try: payload[2:].decode('utf-8')
                    except UnicodeDecodeError as exc: raise OwnerTransportError('protocol_utf8') from exc
                raise OwnerTransportError('peer_closed')
            if opcode == 9:
                self._send_frame(payload, 10, deadline)
                continue
            if opcode == 10:
                continue
            if (opcode == 0) != fragmented:
                raise OwnerTransportError('protocol_fragment')
            chunks.extend(payload)
            if final:
                try: text = chunks.decode('utf-8')
                except UnicodeDecodeError as exc: raise OwnerTransportError('protocol_utf8') from exc
                self.owner.verify(self.socket)
                return text
            fragmented = True
        raise OwnerTransportError('fragment_limit')

    def close(self):
        self.closed.set()
        s = self.socket
        if s is not None:
            try: s.shutdown(socket.SHUT_RDWR)
            except OSError: pass
            s.close()
