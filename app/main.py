from __future__ import annotations

import os

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.api.routes import router
from app.db.mongo import MongoRepository
from app.mini_redis.client import RemoteMiniRedisClient
from app.services.post_service import PostService


mongo_repo = MongoRepository()
mini_redis = RemoteMiniRedisClient(base_url=os.getenv("MINI_REDIS_URL", "tcp://localhost:6380"))
post_service = PostService(
    mongo_repo=mongo_repo,
    mini_redis=mini_redis,
)

app = FastAPI(title="Mini Redis API", version="1.0.0")
app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(router)


@app.on_event("shutdown")
def shutdown_event() -> None:
    mini_redis.close()


@app.get("/health")
def root() -> dict[str, str]:
    mini_redis_health = mini_redis.ping()
    return {
        "message": "Mini Redis API is running.",
        "mongo_backend": mongo_repo.health(),
        "mini_redis_backend": str(mini_redis_health.get("status", "unknown")),
    }
