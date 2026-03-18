from __future__ import annotations

import time
from typing import Any

from app.db.mongo import MongoRepository
from app.mini_redis.core import MiniRedis
from app.mini_redis.persistence import MiniRedisPersistence


class PostService:
    POSTS_CACHE_KEY = "posts:all"
    POST_CACHE_KEY_PREFIX = "post:"
    VIEW_KEY_PREFIX = "views:"
    RANKING_KEY = "rankings:posts"

    def __init__(
        self,
        mongo_repo: MongoRepository,
        mini_redis: MiniRedis,
        persistence: MiniRedisPersistence,
        cache_ttl_seconds: int = 30,
    ) -> None:
        self.mongo_repo = mongo_repo
        self.mini_redis = mini_redis
        self.persistence = persistence
        self.cache_ttl_seconds = cache_ttl_seconds

    def seed_posts(self, count: int) -> dict[str, Any]:
        result = self.mongo_repo.seed_posts(count=count)
        self.mini_redis.delete(self.POSTS_CACHE_KEY)
        self.mini_redis.delete(self.RANKING_KEY)
        debug_state = self.mini_redis.debug_state()
        for key in debug_state["keys"]:
            if key.startswith(self.POST_CACHE_KEY_PREFIX) or key.startswith(self.VIEW_KEY_PREFIX):
                self.mini_redis.delete(key)
        return result

    def get_posts(self) -> dict[str, Any]:
        started = time.perf_counter()
        cached_posts = self.mini_redis.get(self.POSTS_CACHE_KEY)
        if cached_posts is not None:
            elapsed_ms = (time.perf_counter() - started) * 1000
            return {
                "posts": cached_posts,
                "data_source": "mini_redis",
                "elapsed_ms": round(elapsed_ms, 3),
            }
        posts = self.mongo_repo.list_posts()
        self.mini_redis.set(self.POSTS_CACHE_KEY, posts)
        self.mini_redis.expire(self.POSTS_CACHE_KEY, self.cache_ttl_seconds)
        elapsed_ms = (time.perf_counter() - started) * 1000
        return {
            "posts": posts,
            "data_source": "mongo",
            "elapsed_ms": round(elapsed_ms, 3),
        }

    def get_post_detail(self, post_id: int) -> dict[str, Any]:
        cache_key = f"{self.POST_CACHE_KEY_PREFIX}{post_id}"
        started = time.perf_counter()
        post = self.mini_redis.get(cache_key)
        if post is not None:
            data_source = "mini_redis"
        else:
            post = self.mongo_repo.get_post(post_id)
            if post is None:
                raise KeyError(f"Post {post_id} not found.")
            self.mini_redis.set(cache_key, post)
            self.mini_redis.expire(cache_key, self.cache_ttl_seconds)
            data_source = "mongo"
        views = self.mini_redis.incr(f"{self.VIEW_KEY_PREFIX}{post_id}")
        ranking_score = self.mini_redis.zincrby(self.RANKING_KEY, 1, str(post_id))
        elapsed_ms = (time.perf_counter() - started) * 1000
        return {
            "post": post,
            "views": views,
            "ranking_score": ranking_score,
            "data_source": data_source,
            "elapsed_ms": round(elapsed_ms, 3),
        }

    def get_rankings(self, top_n: int) -> dict[str, Any]:
        rankings = self.mini_redis.zrange(self.RANKING_KEY, top_n, desc=True)
        enriched: list[dict[str, Any]] = []
        for item in rankings:
            post_id = int(item.member)
            post = self.mini_redis.get(f"{self.POST_CACHE_KEY_PREFIX}{post_id}") or self.mongo_repo.get_post(post_id)
            enriched.append(
                {
                    "post_id": post_id,
                    "score": item.score,
                    "title": post["title"] if post else None,
                }
            )
        return {"rankings": enriched}

    def compare_performance(self) -> dict[str, Any]:
        mongo_start = time.perf_counter()
        mongo_posts = self.mongo_repo.list_posts()
        mongo_ms = (time.perf_counter() - mongo_start) * 1000

        self.mini_redis.set(self.POSTS_CACHE_KEY, mongo_posts)
        self.mini_redis.expire(self.POSTS_CACHE_KEY, self.cache_ttl_seconds)

        cache_start = time.perf_counter()
        cache_posts = self.mini_redis.get(self.POSTS_CACHE_KEY) or []
        cache_ms = (time.perf_counter() - cache_start) * 1000

        speedup = None
        if cache_ms > 0:
            speedup = round(mongo_ms / cache_ms, 3)

        return {
            "mongo": {"elapsed_ms": round(mongo_ms, 3), "count": len(mongo_posts)},
            "mini_redis": {"elapsed_ms": round(cache_ms, 3), "count": len(cache_posts)},
            "speedup_ratio": speedup,
        }

    def invalidate_cache(self, key: str) -> dict[str, Any]:
        deleted = self.mini_redis.delete(key)
        return {"deleted": deleted, "key": key}

    def save_snapshot(self) -> dict[str, Any]:
        return self.persistence.save(self.mini_redis)

    def load_snapshot(self) -> dict[str, Any]:
        return self.persistence.load(self.mini_redis)

    def debug_mini_redis(self) -> dict[str, Any]:
        return self.mini_redis.debug_state()
