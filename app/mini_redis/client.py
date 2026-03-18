from __future__ import annotations

import pickle
import queue
import socket
import struct
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse


_HEADER_STRUCT = struct.Struct("!I")


def _recv_exact(sock: socket.socket, length: int) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError("Mini Redis server closed the connection.")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


@dataclass
class _PooledConnection:
    sock: socket.socket

    def close(self) -> None:
        self.sock.close()


class RemoteMiniRedisClient:
    def __init__(
        self,
        base_url: str = "tcp://localhost:6380",
        timeout: float = 5.0,
        max_connections: int = 32,
    ) -> None:
        parsed = urlparse(base_url)
        if parsed.scheme not in {"tcp", ""}:
            raise ValueError("RemoteMiniRedisClient only supports tcp:// URLs.")
        self.host = parsed.hostname or "localhost"
        self.port = parsed.port or 6380
        self.timeout = timeout
        self._closed = False
        self._pool: queue.LifoQueue[_PooledConnection] = queue.LifoQueue(maxsize=max_connections)

    def _connect(self) -> _PooledConnection:
        sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        sock.settimeout(self.timeout)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        return _PooledConnection(sock=sock)

    def _acquire(self) -> _PooledConnection:
        if self._closed:
            raise RuntimeError("Mini Redis client is closed.")
        try:
            return self._pool.get_nowait()
        except queue.Empty:
            return self._connect()

    def _release(self, connection: _PooledConnection) -> None:
        if self._closed:
            connection.close()
            return
        try:
            self._pool.put_nowait(connection)
        except queue.Full:
            connection.close()

    def _discard(self, connection: _PooledConnection) -> None:
        connection.close()

    def _request(self, command: str, **payload: Any) -> Any:
        return self._send_payload({"command": command, **payload})

    def _send_payload(self, payload: Any) -> Any:
        connection = self._acquire()
        try:
            body = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
            connection.sock.sendall(_HEADER_STRUCT.pack(len(body)))
            connection.sock.sendall(body)

            header = _recv_exact(connection.sock, _HEADER_STRUCT.size)
            (size,) = _HEADER_STRUCT.unpack(header)
            response_raw = _recv_exact(connection.sock, size)
            response = pickle.loads(response_raw)

            if not response.get("ok", False):
                raise RuntimeError(response.get("error", "Mini Redis request failed."))

            self._release(connection)
            return response.get("data")
        except Exception:
            self._discard(connection)
            raise

    def pipeline(self, commands: list[dict[str, Any]]) -> list[Any]:
        return list(self._send_payload({"pipeline": commands}))

    def close(self) -> None:
        self._closed = True
        while True:
            try:
                connection = self._pool.get_nowait()
            except queue.Empty:
                break
            connection.close()

    def ping(self) -> dict[str, Any]:
        return dict(self._request("ping"))

    def set(self, key: str, value: Any) -> bool:
        return bool(self._request("set", key=key, value=value))

    def get(self, key: str) -> Any | None:
        return self._request("get", key=key)

    def delete(self, key: str) -> bool:
        return bool(self._request("delete", key=key))

    def incr(self, key: str, amount: int = 1) -> int:
        return int(self._request("incr", key=key, amount=amount))

    def expire(self, key: str, seconds: int) -> bool:
        return bool(self._request("expire", key=key, seconds=seconds))

    def ttl(self, key: str) -> int:
        return int(self._request("ttl", key=key))

    def keys(self) -> list[str]:
        return list(self._request("keys"))

    def dumpall(self) -> dict[str, Any]:
        return dict(self._request("dumpall"))

    def zincrby(self, key: str, score: float, member: str) -> float:
        return float(self._request("zincrby", key=key, score=score, member=member))

    def record_view(self, view_key: str, ranking_key: str, member: str, amount: int = 1) -> dict[str, float | int]:
        return dict(
            self._request(
                "record_view",
                view_key=view_key,
                ranking_key=ranking_key,
                member=member,
                amount=amount,
            )
        )

    def flush_pending_views(self) -> dict[str, int]:
        return dict(self._request("flush_pending_views"))

    def pending_write_stats(self) -> dict[str, int]:
        return dict(self._request("pending_write_stats"))

    def zrange(self, key: str, top_n: int, desc: bool = False) -> list[Any]:
        return list(self._request("zrange", key=key, top_n=top_n, desc=desc))
