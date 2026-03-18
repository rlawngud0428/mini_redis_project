from __future__ import annotations

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.api.routes import router
from app.db.mongo import MongoRepository
from app.mini_redis.core import MiniRedis
from app.mini_redis.persistence import MiniRedisPersistence
from app.services.post_service import PostService


mongo_repo = MongoRepository()
mini_redis = MiniRedis()
persistence = MiniRedisPersistence()
post_service = PostService(
    mongo_repo=mongo_repo,
    mini_redis=mini_redis,
    persistence=persistence,
)

app = FastAPI(title="Mini Redis API", version="1.0.0")
app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(router)


@app.get("/health")
def root() -> dict[str, str]:
    return {
        "message": "Mini Redis API is running.",
        "mongo_backend": mongo_repo.health(),
    }
