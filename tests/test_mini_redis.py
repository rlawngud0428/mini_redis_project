from __future__ import annotations

import os
import threading
import time

from fastapi.testclient import TestClient

os.environ["USE_MOCK_MONGO"] = "true"

from app.db.mongo import MongoRepository
from app.main import app
from app.mini_redis.core import MiniRedis
from app.services.post_service import PostService


def test_set_get_delete_incr() -> None:
    redis = MiniRedis()
    assert redis.set("name", "mini-redis") is True
    assert redis.get("name") == "mini-redis"
    assert redis.incr("counter") == 1
    assert redis.incr("counter", 2) == 3
    assert redis.delete("name") is True
    assert redis.get("name") is None


def test_ttl_expiration() -> None:
    redis = MiniRedis()
    redis.set("temp", "value")
    assert redis.expire("temp", 1) is True
    assert redis.ttl("temp") in {0, 1}
    time.sleep(1.2)
    assert redis.get("temp") is None
    assert redis.ttl("temp") == -2


def test_sorted_set() -> None:
    redis = MiniRedis()
    redis.zincrby("rankings", 3, "post-1")
    redis.zincrby("rankings", 4, "post-2")
    redis.zincrby("rankings", 2, "post-1")
    result = redis.zrange("rankings", 2, desc=True)
    assert result[0].member == "post-1"
    assert result[0].score == 5.0
    assert result[1].member == "post-2"
    assert result[1].score == 4.0


def test_concurrent_incr_is_atomic() -> None:
    redis = MiniRedis()

    def worker() -> None:
        for _ in range(500):
            redis.incr("counter")

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert redis.get("counter") == 3000


def test_write_behind_flush() -> None:
    redis = MiniRedis()
    redis.record_view("views:1", "rankings:posts", "1", 3)
    redis.record_view("views:2", "rankings:posts", "2", 2)
    redis.record_view("views:1", "rankings:posts", "1", 1)

    pending = redis.pending_write_stats()
    assert pending["pending_post_count"] == 2
    assert pending["pending_view_count"] == 6

    flushed = redis.flush_pending_views()
    assert flushed == {"1": 4, "2": 2}
    assert redis.pending_write_stats()["pending_view_count"] == 0


def test_api_endpoints_work() -> None:
    mongo_repo = MongoRepository(force_mock=True)
    mini_redis = MiniRedis()
    service = PostService(
        mongo_repo=mongo_repo,
        mini_redis=mini_redis,
        cache_ttl_seconds=5,
    )

    from app.api.routes import get_post_service

    app.dependency_overrides[get_post_service] = lambda: service
    client = TestClient(app)

    seed_response = client.post("/seed", json={"count": 3, "content_size": 256})
    assert seed_response.status_code == 200
    assert seed_response.json()["data"]["inserted_count"] == 3

    first_posts = client.get("/posts")
    assert first_posts.status_code == 200
    assert first_posts.json()["meta"]["data_source"] == "mongo"

    second_posts = client.get("/posts")
    assert second_posts.status_code == 200
    assert second_posts.json()["meta"]["data_source"] == "mini_redis"

    direct_posts = client.get("/posts", params={"cache_mode": "db_only"})
    assert direct_posts.status_code == 200
    assert direct_posts.json()["meta"]["data_source"] == "mongo_direct"

    detail_response = client.get("/posts/1")
    assert detail_response.status_code == 200
    assert detail_response.json()["meta"]["views"] == 1

    direct_detail_response = client.get("/posts/2", params={"cache_mode": "db_only"})
    assert direct_detail_response.status_code == 200
    assert direct_detail_response.json()["meta"]["data_source"] == "mongo_direct"
    assert direct_detail_response.json()["meta"]["views"] == 1

    cache_hit_response = client.post("/posts/1/view-hit", params={"cache_mode": "cache"})
    assert cache_hit_response.status_code == 200
    assert cache_hit_response.json()["data"]["post_id"] == 1
    assert mongo_repo.get_post(1)["view_count"] == 0

    db_hit_response = client.post("/posts/2/view-hit", params={"cache_mode": "db_only"})
    assert db_hit_response.status_code == 200
    assert db_hit_response.json()["meta"]["data_source"] == "mongo_direct"
    assert mongo_repo.get_post(2)["view_count"] == 1

    flush_response = client.post("/mini-redis/flush")
    assert flush_response.status_code == 200
    assert flush_response.json()["data"]["applied_posts"] >= 1
    assert mongo_repo.get_post(1)["view_count"] == 2

    ranking_response = client.get("/rankings", params={"top_n": 3})
    assert ranking_response.status_code == 200
    assert ranking_response.json()["data"][0]["post_id"] == 1

    mongo_ranking_response = client.get("/rankings", params={"top_n": 3, "source": "mongo"})
    assert mongo_ranking_response.status_code == 200
    assert mongo_ranking_response.json()["meta"]["source"] == "mongo"

    delete_response = client.delete("/cache/posts:all")
    assert delete_response.status_code == 200
    assert delete_response.json()["data"]["deleted"] is True

    mini_redis_health_response = client.get("/mini-redis/health")
    assert mini_redis_health_response.status_code == 200

    missing_response = client.get("/posts/999")
    assert missing_response.status_code == 404

    app.dependency_overrides.clear()
