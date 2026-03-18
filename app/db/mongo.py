from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any

from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo import ReturnDocument
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError

try:
    import mongomock
except ImportError:  # pragma: no cover
    mongomock = None


class MongoRepository:
    def __init__(
        self,
        mongo_uri: str | None = None,
        db_name: str = "mini_redis_db",
        collection_name: str = "posts",
        force_mock: bool | None = None,
        connect_timeout_ms: int | None = None,
        max_retries: int | None = None,
        retry_delay_seconds: float | None = None,
    ) -> None:
        self.mongo_uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://localhost:27017")
        self.db_name = db_name
        self.collection_name = collection_name
        self._force_mock = force_mock if force_mock is not None else os.getenv("USE_MOCK_MONGO", "false").lower() == "true"
        self.connect_timeout_ms = connect_timeout_ms or int(os.getenv("MONGO_CONNECT_TIMEOUT_MS", "5000"))
        self.max_retries = max_retries or int(os.getenv("MONGO_CONNECT_RETRIES", "10"))
        self.retry_delay_seconds = retry_delay_seconds or float(os.getenv("MONGO_CONNECT_RETRY_DELAY", "2"))
        self._client = self._build_client()
        self.collection: Collection = self._client[self.db_name][self.collection_name]

    def _build_client(self):
        if self._force_mock:
            if mongomock is None:
                raise RuntimeError("mongomock is required when USE_MOCK_MONGO=true")
            return mongomock.MongoClient()
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=self.connect_timeout_ms)
                client.admin.command("ping")
                return client
            except (ServerSelectionTimeoutError, PyMongoError) as error:
                last_error = error
                if attempt == self.max_retries:
                    break
                time.sleep(self.retry_delay_seconds)
        raise RuntimeError(
            "Failed to connect to MongoDB after retries. Start a real MongoDB instance or set USE_MOCK_MONGO=true explicitly."
        ) from last_error

    def seed_posts(self, count: int = 10, content_size: int = 128) -> dict[str, Any]:
        self.collection.delete_many({})
        now = datetime.now(timezone.utc)
        chunk = "Mini Redis keeps reads fast while MongoDB remains the source of truth. "
        categories = ["tech", "backend", "database", "cache", "infra"]
        docs = [
            {
                "post_id": index,
                "title": f"Sample Post {index}",
                "content": (f"Content for sample post {index}. " + chunk * max(1, content_size // len(chunk)))[:content_size],
                "author": f"Author {((index - 1) % 12) + 1}",
                "category": categories[(index - 1) % len(categories)],
                "tags": [
                    f"tag-{((index - 1) % 5) + 1}",
                    f"topic-{((index + 1) % 7) + 1}",
                    "mini-redis",
                ],
                "summary": f"Summary for sample post {index} focused on cache and database trade-offs.",
                "reaction_count": index * 3,
                "view_count": 0,
                "created_at": now,
            }
            for index in range(1, count + 1)
        ]
        if docs:
            self.collection.insert_many(docs)
        return {"inserted_count": len(docs), "content_size": content_size}

    def list_posts(self) -> list[dict[str, Any]]:
        docs = list(self.collection.find({}, {"_id": 0}).sort("post_id", 1))
        return [self._serialize_post(doc) for doc in docs]

    def get_post(self, post_id: int) -> dict[str, Any] | None:
        doc = self.collection.find_one({"post_id": post_id}, {"_id": 0})
        if not doc:
            return None
        return self._serialize_post(doc)

    def increment_post_views(self, post_id: int, amount: int = 1) -> dict[str, Any] | None:
        doc = self.collection.find_one_and_update(
            {"post_id": post_id},
            {"$inc": {"view_count": amount}},
            projection={"_id": 0},
            return_document=ReturnDocument.AFTER,
        )
        if not doc:
            return None
        return self._serialize_post(doc)

    def increment_post_views_slow_path(self, post_id: int, amount: int = 1) -> dict[str, Any] | None:
        current = self.collection.find_one({"post_id": post_id}, {"_id": 0, "view_count": 1})
        if not current:
            return None
        current_views = int(current.get("view_count", 0))
        new_views = current_views + amount
        update_result = self.collection.update_one(
            {"post_id": post_id},
            {"$set": {"view_count": new_views}},
        )
        if update_result.matched_count == 0:
            return None
        latest = self.collection.find_one({"post_id": post_id}, {"_id": 0})
        if not latest:
            return None
        return self._serialize_post(latest)

    def apply_view_deltas(self, view_deltas: dict[int, int]) -> dict[str, Any]:
        operations = [
            UpdateOne({"post_id": post_id}, {"$inc": {"view_count": amount}})
            for post_id, amount in view_deltas.items()
            if amount > 0
        ]
        if not operations:
            return {"matched_count": 0, "modified_count": 0, "applied_posts": 0, "applied_views": 0}
        result = self.collection.bulk_write(operations, ordered=False)
        return {
            "matched_count": int(result.matched_count),
            "modified_count": int(result.modified_count),
            "applied_posts": len(operations),
            "applied_views": sum(view_deltas.values()),
        }

    def top_posts_by_views(self, limit: int = 5) -> list[dict[str, Any]]:
        docs = list(self.collection.find({}, {"_id": 0}).sort("view_count", -1).limit(limit))
        return [self._serialize_post(doc) for doc in docs]

    def health(self) -> str:
        try:
            self._client.admin.command("ping")
            if mongomock is not None and isinstance(self._client, mongomock.MongoClient):
                return "mock"
            return "mongo"
        except PyMongoError:
            return "unavailable"

    @staticmethod
    def _serialize_post(doc: dict[str, Any]) -> dict[str, Any]:
        serialized = dict(doc)
        created_at = serialized.get("created_at")
        if isinstance(created_at, datetime):
            serialized["created_at"] = created_at.isoformat()
        return serialized
