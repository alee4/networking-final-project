"""

Message:
| type (1 byte) | payload length (4 bytes, big-endian) | payload |

Reciever reads 5 byte header first, then the length of payload bytes

Payloads:
    PUT_BEGIN  size (u64) + sha256 (32 B) + filename (UTF8, rest of payload)
    PUT_CHUNK  raw file bytes
    PUT_END    empty
    GET        filename
    DATA       raw file bytes
    LIST       empty
    LIST_RESP  repeated: name length (u16) + name + size (u64) + sha256 (32 B)
    DELETE     filename
    OK         after GET: size (u64) + sha256 (32 B); otherwise a short text
    ERR        error code (u16) + UTF8 message
"""

import struct

PUT_BEGIN, PUT_CHUNK, PUT_END, GET, DATA, LIST, LIST_RESP, DELETE, OK, ERR = range(1, 11)

ERR_BAD_REQUEST = 1
ERR_NOT_FOUND = 2
ERR_CHECKSUM = 3
ERR_NOT_IMPLEMENTED = 4

HEADER = struct.Struct("!BI")     # type (u8), payload length (u32)
META = struct.Struct("!Q32s")     # size (u64), sha256 (32 raw bytes)
CHUNK_SIZE = 64 * 1024
MAX_PAYLOAD = 1024 * 1024


def recv_exact(sock, n):
    """Keep calling recv() until exactly n bytes have arrived."""
    buf = bytearray()

    while len(buf) < n:
        part = sock.recv(n - len(buf))

        #empty read = peer closed
        if not part:
            raise ConnectionError("connection closed by peer")
        buf += part

    return bytes(buf)


def send_msg(sock, msg_type, payload=b""):
    sock.sendall(HEADER.pack(msg_type, len(payload)) + payload)


def recv_msg(sock):
    msg_type, length = HEADER.unpack(recv_exact(sock, HEADER.size))

    if length > MAX_PAYLOAD:
        raise ValueError(f"payload too large: {length} bytes")

    return msg_type, recv_exact(sock, length)


def send_err(sock, code, message):
    send_msg(sock, ERR, struct.pack("!H", code) + message.encode())


def parse_err(payload):
    (code,) = struct.unpack("!H", payload[:2])
    return code, payload[2:].decode()


def pack_list(entries):

    # entries: list of (name, size, sha256).
    out = bytearray()

    for name, size, sha in entries:
        n = name.encode()
        out += struct.pack("!H", len(n)) + n + META.pack(size, sha)

    return bytes(out)


def unpack_list(payload):
    entries, pos = [], 0
    while pos < len(payload):
        (n,) = struct.unpack_from("!H", payload, pos)
        pos += 2
        name = payload[pos:pos + n].decode()
        pos += n
        size, sha = META.unpack_from(payload, pos)
        pos += META.size
        entries.append((name, size, sha))
    return entries
