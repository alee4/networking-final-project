"""
threaded file-storage server.

Usage:
python server.py [--host 0.0.0.0] [--port 9000] [--root storage]

One thread per client. A lock protects the shared file index. Uploads go to
a temp file and only become visible after PUT_END passes the size + sha256
check, so incomplete uploads never show up as real files.
"""

import argparse
import hashlib
import logging
import os
import socket
import threading
import time
import uuid

from protocol import *

log = logging.getLogger("server")

index = {}                       # filename -> (size, sha256)
index_lock = threading.Lock()
FILES_DIR = TMP_DIR = None


def valid_name(name):
    return name not in ("", ".", "..") and "/" not in name and "\\" not in name


def startup(root):

    # Clean up partial uploads from a crash, then rebuild the index from disk.
    global FILES_DIR, TMP_DIR
    FILES_DIR = os.path.join(root, "files")
    TMP_DIR = os.path.join(root, "tmp")
    os.makedirs(FILES_DIR, exist_ok=True)
    os.makedirs(TMP_DIR, exist_ok=True)

    leftovers = os.listdir(TMP_DIR)

    for f in leftovers:
        os.remove(os.path.join(TMP_DIR, f))

    if leftovers:
        log.warning("removed %d partial upload(s) left over from a crash", len(leftovers))

    for name in os.listdir(FILES_DIR):
        path = os.path.join(FILES_DIR, name)

        with open(path, "rb") as f:
            sha = hashlib.sha256(f.read()).digest()

        index[name] = (os.path.getsize(path), sha)


def handle_client(sock, addr):
    up = None                    # state of the in-progress upload, if any
    try:
        while True:
            msg_type, payload = recv_msg(sock)

            if msg_type == PUT_BEGIN:
                size, sha = META.unpack_from(payload)
                name = payload[META.size:].decode()

                if up is not None:
                    send_err(sock, ERR_BAD_REQUEST, "upload already in progress")
                    continue

                if not valid_name(name):
                    send_err(sock, ERR_BAD_REQUEST, f"invalid filename: {name!r}")
                    continue

                tmp = os.path.join(TMP_DIR, uuid.uuid4().hex + ".tmp")
                up = {"name": name, "size": size, "sha": sha, "tmp": tmp, "file": open(tmp, "wb"), "hash": hashlib.sha256(), "got": 0, "start": time.perf_counter()}
                log.info("PUT_BEGIN %r (%d bytes)", name, size)

                send_msg(sock, OK, b"ready")

            elif msg_type == PUT_CHUNK:
                if up is None:
                    send_err(sock, ERR_BAD_REQUEST, "PUT_CHUNK without PUT_BEGIN")
                    continue

                up["file"].write(payload)
                up["hash"].update(payload)
                up["got"] += len(payload)

            elif msg_type == PUT_END:
                if up is None:
                    send_err(sock, ERR_BAD_REQUEST, "PUT_END without PUT_BEGIN")
                    continue

                up["file"].close()
                if up["got"] != up["size"] or up["hash"].digest() != up["sha"]:
                    os.remove(up["tmp"])
                    log.warning("PUT %r rejected: size or checksum mismatch", up["name"])
                    send_err(sock, ERR_CHECKSUM, "size or sha256 mismatch")

                else:
                    with index_lock:
                        os.replace(up["tmp"], os.path.join(FILES_DIR, up["name"]))
                        index[up["name"]] = (up["size"], up["sha"])
                    ms = (time.perf_counter() - up["start"]) * 1000
                    log.info("PUT %r stored (%d bytes, %.1f ms)", up["name"], up["size"], ms)
                    send_msg(sock, OK, b"stored")

                up = None

            elif msg_type == GET:
                name = payload.decode()

                with index_lock:     # look up + open together so they match
                    meta = index.get(name)
                    f = open(os.path.join(FILES_DIR, name), "rb") if meta else None

                if f is None:
                    send_err(sock, ERR_NOT_FOUND, f"no such file: {name}")
                    continue

                with f:
                    while chunk := f.read(CHUNK_SIZE):
                        send_msg(sock, DATA, chunk)

                send_msg(sock, OK, META.pack(*meta))
                log.info("GET %r (%d bytes)", name, meta[0])

            elif msg_type == LIST:
                with index_lock:
                    entries = [(n, s, h) for n, (s, h) in sorted(index.items())]

                send_msg(sock, LIST_RESP, pack_list(entries))
                log.info("LIST -> %d file(s)", len(entries))

            elif msg_type == DELETE:
                # TODO: decode the filename, then while holding index_lock remove
                # it from index and delete the file in FILES_DIR. Reply OK, or
                # ERR_NOT_FOUND if it isn't in the index.
                send_err(sock, ERR_NOT_IMPLEMENTED, "DELETE not implemented yet")

            else:
                send_err(sock, ERR_BAD_REQUEST, f"unexpected message type {msg_type}")

    except ConnectionError:
        if up is not None:
            log.warning("connection lost mid-upload of %r", up["name"])
        else:
            log.info("client %s:%d disconnected", *addr)

    except Exception as e:
        log.warning("error: %s; closing connection", e)

    finally:
        # never leave a partial upload behind
        if up is not None:       
            up["file"].close()
            os.remove(up["tmp"])
            log.warning("discarded partial upload (%d of %d bytes received)",
                        up["got"], up["size"])
            
        sock.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--root", default="storage")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, datefmt="%H:%M:%S", format="%(asctime)s.%(msecs)03d [%(threadName)s] %(message)s")
    startup(args.root)

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # quick restart after a crash
    srv.bind((args.host, args.port))
    srv.listen()
    log.info("listening on %s:%d (%d stored file(s))", args.host, args.port, len(index))

    count = 0
    try:
        while True:
            sock, addr = srv.accept()
            count += 1
            threading.Thread(target=handle_client, args=(sock, addr),
                             name=f"client-{count}", daemon=True).start()
            log.info("accepted client-%d from %s:%d (active connections: %d)",
                     count, addr[0], addr[1], threading.active_count() - 1)
            
    except KeyboardInterrupt:
        log.info("shutting down")
        # TODO send each connected client an ERR before exiting on connection drop

    finally:
        srv.close()


if __name__ == "__main__":
    main()
