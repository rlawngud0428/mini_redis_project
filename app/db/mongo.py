from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from pymongo import MongoClient
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
    ) -> None:
        self.mongo_uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://localhost:27017")
        self.db_name = db_name
        self.collection_name = collection_name
        self._force_mock = force_mock if force_mock is not None else os.getenv("USE_MOCK_MONGO", "false").lower() == "true"
        self._client = self._build_client()
        self.collection: Collection = self._client[self.db_name][self.collection_name]

    def _build_client(self):
        if self._force_mock:
            if mongomock is None:
                raise RuntimeError("mongomock is required when USE_MOCK_MONGO=true")
            return mongomock.MongoClient()
        try:
            client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=1500)
            client.admin.command("ping")
            return client
        except (ServerSelectionTimeoutError, PyMongoError):
            if mongomock is None:
                raise
            return mongomock.MongoClient()

    def seed_posts(self, count: int = 10, content_size: int = 128) -> dict[str, Any]:
        self.collection.delete_many({})
        now = datetime.now(timezone.utc)
        chunk = "Mini Redis keeps reads fast while MongoDB remains the source of truth. "
        docs = [
            {
                "post_id": index,
                "title": f"Sample Post {index}",
                "content": (f"Content for sample post {index}. " + chunk * max(1, content_size // len(chunk)))[:content_size],
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
