from __future__ import annotations

import copy
import threading
import time
from typing import Any, Generic, TypeVar

from pydantic import BaseModel


K = TypeVar("K")
V = TypeVar("V")
_MISSING = object()


class RankedMember(BaseModel):
    member: str
    score: float


class ViewRecord(BaseModel):
    views: int
    ranking_score: float


class PendingWriteStats(BaseModel):
    pending_post_count: int
    pending_view_count: int


class _HashEntry(BaseModel, Generic[K, V]):
    key: K
    value: V


class HashTable(Generic[K, V]):
    def __init__(self, initial_capacity: int = 16) -> None:
        capacity = max(8, initial_capacity)
        self._buckets: list[list[_HashEntry[K, V]]] = [[] for _ in range(capacity)]
        self._size = 0

    def __contains__(self, key: K) -> bool:
        return self._find_entry(key) is not None

    def __len__(self) -> int:
        return self._size

    def __getitem__(self, key: K) -> V:
        entry = self._find_entry(key)
        if entry is None:
            raise KeyError(key)
        return entry.value

    def __setitem__(self, key: K, value: V) -> None:
        bucket = self._bucket_for_key(key)
        for entry in bucket:
            if entry.key == key:
                entry.value = value
                return
        bucket.append(_HashEntry(key=key, value=value))
        self._size += 1
        if self._size / len(self._buckets) > 0.75:
            self._resize()

    def get(self, key: K, default: Any = None) -> V | Any:
        entry = self._find_entry(key)
        if entry is None:
            return default
        return entry.value

    def pop(self, key: K, default: Any = _MISSING) -> V | Any:
        bucket = self._bucket_for_key(key)
        for index, entry in enumerate(bucket):
            if entry.key == key:
                bucket.pop(index)
                self._size -= 1
                return entry.value
        if default is _MISSING:
            raise KeyError(key)
        return default

    def keys(self) -> list[K]:
        return [entry.key for bucket in self._buckets for entry in bucket]

    def items(self) -> list[tuple[K, V]]:
        return [(entry.key, entry.value) for bucket in self._buckets for entry in bucket]

    def _bucket_for_key(self, key: K) -> list[_HashEntry[K, V]]:
        index = hash(key) % len(self._buckets)
        return self._buckets[index]

    def _find_entry(self, key: K) -> _HashEntry[K, V] | None:
        bucket = self._bucket_for_key(key)
        for entry in bucket:
            if entry.key == key:
                return entry
        return None

    def _resize(self) -> None:
        old_items = self.items()
        self._buckets = [[] for _ in range(len(self._buckets) * 2)]
        self._size = 0
        for key, value in old_items:
            self[key] = value


class MiniRedis:
    def __init__(self, max_cache_entries: int = 256) -> None:
        self.store: HashTable[str, Any] = HashTable()
        self.counters: HashTable[str, int] = HashTable()
        self.pending_view_deltas: HashTable[str, int] = HashTable()
        self.expire_map: HashTable[str, float] = HashTable()
        self.last_access_map: HashTable[str, float] = HashTable()
        self.sorted_sets: HashTable[str, HashTable[str, float]] = HashTable()
        self.counter_sorted_sets: HashTable[str, HashTable[str, float]] = HashTable()
        self.max_cache_entries = max(1, max_cache_entries)
        self.lock = threading.RLock()

    def _touch_key_unlocked(self, key: str) -> None:
        if key in self.store:
            self.last_access_map[key] = time.time()

    def _is_expired_unlocked(self, key: str) -> bool:
        expires_at = self.expire_map.get(key)
        if expires_at is None:
            return False
        if time.time() < expires_at:
            return False
        self._delete_unlocked(key)
        return True

    def _delete_unlocked(self, key: str) -> bool:
        existed = (
            key in self.store
            or key in self.counters
            or key in self.pending_view_deltas
            or key in self.sorted_sets
            or key in self.counter_sorted_sets
            or key in self.expire_map
        )
        self.store.pop(key, None)
        self.counters.pop(key, None)
        self.pending_view_deltas.pop(key, None)
        self.sorted_sets.pop(key, None)
        self.counter_sorted_sets.pop(key, None)
        self.expire_map.pop(key, None)
        self.last_access_map.pop(key, None)
        return existed

    def _evict_if_needed_unlocked(self) -> None:
        self._cleanup_expired_unlocked()
        while len(self.store) > self.max_cache_entries:
            oldest_key: str | None = None
            oldest_access = float("inf")
            for key, last_access in self.last_access_map.items():
                if key not in self.store:
                    continue
                if last_access < oldest_access:
                    oldest_key = key
                    oldest_access = last_access
            if oldest_key is None:
                break
            self._delete_unlocked(oldest_key)

    def _ensure_key_alive_unlocked(self, key: str) -> bool:
        if self._is_expired_unlocked(key):
            return False
        return (
            key in self.store
            or key in self.counters
            or key in self.pending_view_deltas
            or key in self.sorted_sets
            or key in self.counter_sorted_sets
        )

    def _get_or_create_sorted_set(
        self,
        root: HashTable[str, HashTable[str, float]],
        key: str,
    ) -> HashTable[str, float]:
        zset = root.get(key)
        if zset is None:
            zset = HashTable()
            root[key] = zset
        return zset

    def set(self, key: str, value: Any) -> bool:
        with self.lock:
            self._cleanup_expired_unlocked()
            self.store[key] = copy.deepcopy(value)
            self._touch_key_unlocked(key)
            self._evict_if_needed_unlocked()
            return True

    def get(self, key: str) -> Any | None:
        with self.lock:
            if not self._ensure_key_alive_unlocked(key):
                return None
            if key in self.counters:
                return self.counters.get(key)
            self._touch_key_unlocked(key)
            return copy.deepcopy(self.store.get(key))

    def delete(self, key: str) -> bool:
        with self.lock:
            return self._delete_unlocked(key)

    def incr(self, key: str, amount: int = 1) -> int:
        with self.lock:
            if self._is_expired_unlocked(key):
                current = 0
            else:
                current = self.counters.get(key, self.store.get(key, 0))
            if not isinstance(current, int):
                raise TypeError(f"Value for key '{key}' is not an integer.")
            new_value = current + amount
            self.counters[key] = new_value
            self.store.pop(key, None)
            self.last_access_map.pop(key, None)
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
                zset = HashTable[str, float]()
                self.sorted_sets[key] = zset
            else:
                zset = self._get_or_create_sorted_set(self.sorted_sets, key)
            new_score = float(zset.get(member, 0.0) + score)
            zset[member] = new_score
            return new_score

    def record_view(self, view_key: str, ranking_key: str, member: str, amount: int = 1) -> dict[str, float | int]:
        with self.lock:
            if self._is_expired_unlocked(view_key):
                current_views = 0
            else:
                current_views = self.counters.get(view_key, 0)
            if not isinstance(current_views, int):
                raise TypeError(f"Value for key '{view_key}' is not an integer.")
            new_views = current_views + amount
            self.counters[view_key] = new_views
            self.pending_view_deltas[member] = self.pending_view_deltas.get(member, 0) + amount
            self.store.pop(view_key, None)
            self.last_access_map.pop(view_key, None)

            if self._is_expired_unlocked(ranking_key):
                zset = HashTable[str, float]()
                self.counter_sorted_sets[ranking_key] = zset
            else:
                zset = self._get_or_create_sorted_set(self.counter_sorted_sets, ranking_key)
            new_score = float(zset.get(member, 0.0) + amount)
            zset[member] = new_score

            return ViewRecord(views=new_views, ranking_score=new_score).model_dump()

    def flush_pending_views(self) -> dict[str, int]:
        with self.lock:
            flushed = {member: amount for member, amount in self.pending_view_deltas.items()}
            self.pending_view_deltas = HashTable()
            return flushed

    def pending_write_stats(self) -> dict[str, int]:
        with self.lock:
            pending_items = self.pending_view_deltas.items()
            return PendingWriteStats(
                pending_post_count=len(pending_items),
                pending_view_count=sum(amount for _, amount in pending_items),
            ).model_dump()

    def keys(self) -> list[str]:
        with self.lock:
            self._cleanup_expired_unlocked()
            unique_keys = {
                *self.store.keys(),
                *self.counters.keys(),
                *self.sorted_sets.keys(),
                *self.counter_sorted_sets.keys(),
            }
            return sorted(unique_keys)

    def dumpall(self) -> dict[str, Any]:
        with self.lock:
            self._cleanup_expired_unlocked()
            return {
                "store": {
                    key: {
                        "value": copy.deepcopy(value),
                        "ttl_seconds": self.ttl(key),
                    }
                    for key, value in self.store.items()
                },
                "counters": {
                    key: {
                        "value": value,
                        "ttl_seconds": self.ttl(key),
                    }
                    for key, value in self.counters.items()
                },
                "sorted_sets": {
                    key: {
                        "members": {member: score for member, score in zset.items()},
                        "ttl_seconds": self.ttl(key),
                    }
                    for key, zset in self.sorted_sets.items()
                },
                "counter_sorted_sets": {
                    key: {
                        "members": {member: score for member, score in zset.items()},
                        "ttl_seconds": self.ttl(key),
                    }
                    for key, zset in self.counter_sorted_sets.items()
                },
                "pending_view_deltas": {
                    member: amount for member, amount in self.pending_view_deltas.items()
                },
            }

    def zrange(self, key: str, top_n: int, desc: bool = False) -> list[RankedMember]:
        with self.lock:
            if not self._ensure_key_alive_unlocked(key):
                return []
            members = self.counter_sorted_sets.get(key)
            if members is None:
                members = self.sorted_sets.get(key)
            if members is None:
                return []
            if desc:
                ordered = sorted(members.items(), key=lambda item: (-item[1], item[0]))
            else:
                ordered = sorted(members.items(), key=lambda item: (item[1], item[0]))
            sliced = ordered[:top_n]
            return [RankedMember(member=member, score=score) for member, score in sliced]

    def _cleanup_expired_unlocked(self) -> None:
        expired_keys = [
            key
            for key, expires_at in self.expire_map.items()
            if time.time() >= expires_at
        ]
        for key in expired_keys:
            self._delete_unlocked(key)
