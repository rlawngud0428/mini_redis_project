from __future__ import annotations

import copy
import threading
import time
from dataclasses import dataclass
from typing import Any


@dataclass
class RankedMember:
    member: str
    score: float


class MiniRedis:
    def __init__(self) -> None:
        self.store: dict[str, Any] = {}
        self.expire_map: dict[str, float] = {}
        self.sorted_sets: dict[str, dict[str, float]] = {}
        self.lock = threading.RLock()

    def _is_expired_unlocked(self, key: str) -> bool:
        expires_at = self.expire_map.get(key)
        if expires_at is None:
            return False
        if time.time() < expires_at:
            return False
        self._delete_unlocked(key)
        return True

    def _delete_unlocked(self, key: str) -> bool:
        existed = key in self.store or key in self.sorted_sets or key in self.expire_map
        self.store.pop(key, None)
        self.sorted_sets.pop(key, None)
        self.expire_map.pop(key, None)
        return existed

    def _ensure_key_alive_unlocked(self, key: str) -> bool:
        if self._is_expired_unlocked(key):
            return False
        return key in self.store or key in self.sorted_sets

    def set(self, key: str, value: Any) -> bool:
        with self.lock:
            self.store[key] = copy.deepcopy(value)
            return True

    def get(self, key: str) -> Any | None:
        with self.lock:
            if not self._ensure_key_alive_unlocked(key):
                return None
            return copy.deepcopy(self.store.get(key))

    def delete(self, key: str) -> bool:
        with self.lock:
            return self._delete_unlocked(key)

    def incr(self, key: str, amount: int = 1) -> int:
        with self.lock:
            if self._is_expired_unlocked(key):
                current = 0
            else:
                current = self.store.get(key, 0)
            if not isinstance(current, int):
                raise TypeError(f"Value for key '{key}' is not an integer.")
            new_value = current + amount
            self.store[key] = new_value
            return new_value

    def expire(self, key: str, seconds: int) -> bool:
        if seconds < 0:
            raise ValueError("seconds must be non-negative")
        with self.lock:
            if not self._ensure_key_alive_unlocked(key):
                return False
            self.expire_map[key] = time.time() + seconds
            return True

    def ttl(self, key: str) -> int:
        with self.lock:
            if not self._ensure_key_alive_unlocked(key):
                return -2
            expires_at = self.expire_map.get(key)
            if expires_at is None:
                return -1
            remaining = int(expires_at - time.time())
            if remaining < 0:
                self._delete_unlocked(key)
                return -2
            return remaining

    def zincrby(self, key: str, score: float, member: str) -> float:
        with self.lock:
            if self._is_expired_unlocked(key):
                zset = {}
                self.sorted_sets[key] = zset
            else:
                zset = self.sorted_sets.setdefault(key, {})
            new_score = float(zset.get(member, 0.0) + score)
            zset[member] = new_score
            return new_score

    def zrange(self, key: str, top_n: int, desc: bool = False) -> list[RankedMember]:
        with self.lock:
            if not self._ensure_key_alive_unlocked(key):
                return []
            members = self.sorted_sets.get(key, {})
            if desc:
                ordered = sorted(members.items(), key=lambda item: (-item[1], item[0]))
            else:
                ordered = sorted(members.items(), key=lambda item: (item[1], item[0]))
            sliced = ordered[:top_n]
            return [RankedMember(member=member, score=score) for member, score in sliced]

    def debug_state(self) -> dict[str, Any]:
        with self.lock:
            self._cleanup_expired_unlocked()
            keys = sorted(set(self.store.keys()) | set(self.sorted_sets.keys()))
            ttl_map: dict[str, int] = {}
            alive_keys: list[str] = []
            for key in list(keys):
                ttl_value = self.ttl(key)
                if ttl_value != -2:
                    alive_keys.append(key)
                    ttl_map[key] = ttl_value
            return {
                "keys": alive_keys,
                "store": copy.deepcopy(self.store),
                "ttl": ttl_map,
                "sorted_sets": copy.deepcopy(self.sorted_sets),
            }

    def export_state(self) -> dict[str, Any]:
        with self.lock:
            self._cleanup_expired_unlocked()
            return {
                "store": copy.deepcopy(self.store),
                "expire_map": copy.deepcopy(self.expire_map),
                "sorted_sets": copy.deepcopy(self.sorted_sets),
            }

    def import_state(self, state: dict[str, Any]) -> None:
        with self.lock:
            self.store = copy.deepcopy(state.get("store", {}))
            self.expire_map = copy.deepcopy(state.get("expire_map", {}))
            self.sorted_sets = copy.deepcopy(state.get("sorted_sets", {}))
            self._cleanup_expired_unlocked()

    def _cleanup_expired_unlocked(self) -> None:
        expired_keys = [
            key
            for key, expires_at in self.expire_map.items()
            if time.time() >= expires_at
        ]
        for key in expired_keys:
            self._delete_unlocked(key)
