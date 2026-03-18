from __future__ import annotations

import os
import pickle
import socket
import socketserver
import struct
from typing import Any

from app.mini_redis.core import MiniRedis


_HEADER_STRUCT = struct.Struct("!I")

mini_redis = MiniRedis(max_cache_entries=int(os.getenv("MINI_REDIS_MAX_CACHE_ENTRIES", "256")))


def _recv_exact(sock: socket.socket, length: int) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError("Client closed the connection.")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _success(data: Any = None) -> dict[str, Any]:
    return {"ok": True, "data": data}


def _error(message: str) -> dict[str, Any]:
    return {"ok": False, "error": message}


def _dispatch(payload: dict[str, Any]) -> dict[str, Any]:
    command = payload.get("command")

    if command == "ping":
        return _success({"service": "mini_redis_tcp", "status": "ok"})
    if command == "set":
        return _success(mini_redis.set(str(payload["key"]), payload.get("value")))
    if command == "get":
        return _success(mini_redis.get(str(payload["key"])))
    if command == "delete":
        return _success(mini_redis.delete(str(payload["key"])))
    if command == "incr":
        return _success(mini_redis.incr(str(payload["key"]), int(payload.get("amount", 1))))
    if command == "expire":
        return _success(mini_redis.expire(str(payload["key"]), int(payload["seconds"])))
    if command == "ttl":
        return _success(mini_redis.ttl(str(payload["key"])))
    if command == "keys":
        return _success(mini_redis.keys())
    if command == "dumpall":
        return _success(mini_redis.dumpall())
    if command == "zincrby":
        return _success(
            mini_redis.zincrby(
                str(payload["key"]),
                float(payload["score"]),
                str(payload["member"]),
            )
        )
    if command == "zrange":
        items = mini_redis.zrange(
            str(payload["key"]),
            int(payload.get("top_n", 5)),
            desc=bool(payload.get("desc", False)),
        )
        return _success([item.model_dump() for item in items])
    if command == "record_view":
        return _success(
            mini_redis.record_view(
                str(payload["view_key"]),
                str(payload["ranking_key"]),
                str(payload["member"]),
                int(payload.get("amount", 1)),
            )
        )
    if command == "flush_pending_views":
        return _success(mini_redis.flush_pending_views())
    if command == "pending_write_stats":
        return _success(mini_redis.pending_write_stats())
    return _error(f"Unknown command: {command}")


def _dispatch_pipeline(commands: list[dict[str, Any]]) -> dict[str, Any]:
    results: list[Any] = []
    for command in commands:
        response = _dispatch(command)
        if not response.get("ok", False):
            return response
        results.append(response.get("data"))
    return _success(results)


class MiniRedisTCPHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        sock: socket.socket = self.request
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        while True:
            try:
                header = _recv_exact(sock, _HEADER_STRUCT.size)
            except ConnectionError:
                break

            try:
                (payload_size,) = _HEADER_STRUCT.unpack(header)
                payload_raw = _recv_exact(sock, payload_size)
                payload = pickle.loads(payload_raw)
                if isinstance(payload, dict) and "pipeline" in payload:
                    response = _dispatch_pipeline(list(payload["pipeline"]))
                else:
                    response = _dispatch(payload)
            except Exception as error:  # noqa: BLE001 - return protocol error to client
                response = _error(str(error))

            body = pickle.dumps(response, protocol=pickle.HIGHEST_PROTOCOL)
            sock.sendall(_HEADER_STRUCT.pack(len(body)))
            sock.sendall(body)


class ThreadedMiniRedisTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 256


def main() -> None:
    host = os.getenv("MINI_REDIS_HOST", "0.0.0.0")
    port = int(os.getenv("MINI_REDIS_PORT", "6380"))
    with ThreadedMiniRedisTCPServer((host, port), MiniRedisTCPHandler) as server:
        print(f"Mini Redis TCP server listening on {host}:{port}", flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()
