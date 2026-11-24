"""
command-line client for the file-storage server

    python client.py [--host H] [--port P] [--delay S] COMMAND ...

    put FILE [--name NAME]    upload a file
    get NAME [-o OUT]         download a file
    list                      list stored files
    delete NAME               delete a stored file
    roundtrip FILE            representative op: upload, download, compare checksums
"""

import argparse
import hashlib
import os
import socket
import sys
import time

from protocol import *


class ServerError(Exception):
    pass


def log(msg):
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


def expect(sock, want):
    msg_type, payload = recv_msg(sock)
    if msg_type == ERR:
        code, message = parse_err(payload)
        raise ServerError(f"ERR {code}: {message}")
    if msg_type != want:
        raise ServerError(f"expected message type {want}, got {msg_type}")
    return payload


def put(sock, name, data, delay=0.0):
    sha = hashlib.sha256(data).digest()
    send_msg(sock, PUT_BEGIN, META.pack(len(data), sha) + name.encode())
    expect(sock, OK)

    for i in range(0, len(data), CHUNK_SIZE):
        send_msg(sock, PUT_CHUNK, data[i:i + CHUNK_SIZE])

        if delay:
            time.sleep(delay)
            sent = min(i + CHUNK_SIZE, len(data))

            # about every 1Mib
            if (i // CHUNK_SIZE) % 16 == 15:
                log(f"sent {sent} / {len(data)} bytes")

    send_msg(sock, PUT_END)
    expect(sock, OK)


def get(sock, name):
    send_msg(sock, GET, name.encode())
    parts = []

    while True:
        msg_type, payload = recv_msg(sock)

        if msg_type == DATA:
            parts.append(payload)
        elif msg_type == OK:
            break
        elif msg_type == ERR:
            code, message = parse_err(payload)
            raise ServerError(f"ERR {code}: {message}")
        else:
            raise ServerError(f"unexpected message type {msg_type}")

    data = b"".join(parts)
    size, sha = META.unpack(payload)

    if len(data) != size or hashlib.sha256(data).digest() != sha:
        raise ServerError("downloaded data failed the checksum check")
    
    return data


def list_files(sock):
    send_msg(sock, LIST)
    return unpack_list(expect(sock, LIST_RESP))


def delete(sock, name):
    # TODO: DELETE message
    raise NotImplementedError("delete is not implemented yet")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--delay", type=float, default=0.0, help="seconds to sleep after each chunk (slow uploads for Exp. 4)")

    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("put"); p.add_argument("path"); p.add_argument("--name")
    p = sub.add_parser("get"); p.add_argument("name"); p.add_argument("-o", "--output")

    sub.add_parser("list")
    p = sub.add_parser("delete"); p.add_argument("name")
    p = sub.add_parser("roundtrip"); p.add_argument("path")
    args = ap.parse_args()

    try:
        with socket.create_connection((args.host, args.port), timeout=30) as sock:
            log(f"connected to {args.host}:{args.port}")
            t0 = time.perf_counter()

            if args.cmd == "put":
                with open(args.path, "rb") as f:
                    data = f.read()
                name = args.name or os.path.basename(args.path)
                put(sock, name, data, args.delay)
                log(f"uploaded {name} ({len(data)} bytes)")

            elif args.cmd == "get":
                data = get(sock, args.name)
                with open(args.output or args.name, "wb") as f:
                    f.write(data)
                log(f"downloaded {args.name} ({len(data)} bytes, checksum OK)")

            elif args.cmd == "list":
                entries = list_files(sock)
                for name, size, sha in entries:
                    print(f"{name:<30} {size:>12}  {sha.hex()[:16]}...")
                print(f"{len(entries)} file(s)")

            elif args.cmd == "delete":
                delete(sock, args.name)
                log(f"deleted {args.name}")

            elif args.cmd == "roundtrip":
                with open(args.path, "rb") as f:
                    data = f.read()
                name = os.path.basename(args.path)
                put(sock, name, data, args.delay)
                got = get(sock, name)
                same = hashlib.sha256(got).digest() == hashlib.sha256(data).digest()
                log("checksums match" if same else "CHECKSUM MISMATCH")

            log(f"done in {(time.perf_counter() - t0) * 1000:.1f} ms")

    except ServerError as e:
        log(f"server error: {e}")
        return 1
    
    except (ConnectionError, socket.timeout) as e:
        log(f"connection lost: {e}")
        # TODO reconnect and retry a few times
        return 2
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
