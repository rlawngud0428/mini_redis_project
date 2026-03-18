from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.mini_redis.core import MiniRedis


class MiniRedisPersistence:
    def __init__(self, file_path: str = "data/mini_redis_snapshot.json") -> None:
        self.file_path = Path(file_path)

    def save(self, mini_redis: MiniRedis) -> dict[str, Any]:
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        state = mini_redis.export_state()
        key_count = len(set(state["store"].keys()) | set(state["sorted_sets"].keys()))
        with self.file_path.open("w", encoding="utf-8") as file:
            json.dump(state, file, ensure_ascii=False, indent=2)
        return {"saved": True, "file_path": str(self.file_path), "key_count": key_count}

    def load(self, mini_redis: MiniRedis) -> dict[str, Any]:
        if not self.file_path.exists():
            return {"loaded": False, "reason": "snapshot_not_found", "file_path": str(self.file_path)}
        with self.file_path.open("r", encoding="utf-8") as file:
            state = json.load(file)
        mini_redis.import_state(state)
        key_count = len(set(state.get("store", {}).keys()) | set(state.get("sorted_sets", {}).keys()))
        return {"loaded": True, "file_path": str(self.file_path), "key_count": key_count}
