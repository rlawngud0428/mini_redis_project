from __future__ import annotations

import tempfile
import threading
import time

from fastapi.testclient import TestClient

from app.db.mongo import MongoRepository
from app.main import app
from app.mini_redis.core import MiniRedis
from app.mini_redis.persistence import MiniRedisPersistence
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


def test_persistence_save_and_load() -> None:
    redis = MiniRedis()
    redis.set("persist:key", {"value": 1})
    redis.zincrby("rankings", 7, "42")

    with tempfile.NamedTemporaryFile(suffix=".json") as snapshot:
        persistence = MiniRedisPersistence(snapshot.name)
        save_result = persistence.save(redis)
        assert save_result["saved"] is True

        restored = MiniRedis()
        load_result = persistence.load(restored)
        assert load_result["loaded"] is True
        assert restored.get("persist:key") == {"value": 1}
        assert restored.zrange("rankings", 1, desc=True)[0].member == "42"


def test_api_endpoints_work() -> None:
    mongo_repo = MongoRepository(force_mock=True)
    mini_redis = MiniRedis()
    with tempfile.NamedTemporaryFile(suffix=".json") as snapshot:
        persistence = MiniRedisPersistence(snapshot.name)
        service = PostService(
            mongo_repo=mongo_repo,
            mini_redis=mini_redis,
            persistence=persistence,
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

        pure_detail_response = client.get("/posts/1/pure", params={"cache_mode": "db_only"})
        assert pure_detail_response.status_code == 200
        assert pure_detail_response.json()["meta"]["pure_read"] is True

        direct_detail_response = client.get("/posts/2", params={"cache_mode": "db_only"})
        assert direct_detail_response.status_code == 200
        assert direct_detail_response.json()["meta"]["data_source"] == "mongo_direct"
        assert direct_detail_response.json()["meta"]["views"] == 1

        ranking_response = client.get("/rankings", params={"top_n": 3})
        assert ranking_response.status_code == 200
        assert ranking_response.json()["data"][0]["post_id"] == 1

        mongo_ranking_response = client.get("/rankings", params={"top_n": 3, "source": "mongo"})
        assert mongo_ranking_response.status_code == 200
        assert mongo_ranking_response.json()["meta"]["source"] == "mongo"

        compare_response = client.get("/compare/performance")
        assert compare_response.status_code == 200
        assert "mongo" in compare_response.json()["data"]
        assert "mini_redis" in compare_response.json()["data"]

        delete_response = client.delete("/cache/posts:all")
        assert delete_response.status_code == 200
        assert delete_response.json()["data"]["deleted"] is True

        save_response = client.post("/mini-redis/save")
        assert save_response.status_code == 200
        assert save_response.json()["data"]["saved"] is True

        load_response = client.post("/mini-redis/load")
        assert load_response.status_code == 200
        assert load_response.json()["data"]["loaded"] is True

        debug_response = client.get("/mini-redis/debug")
        assert debug_response.status_code == 200
        assert "keys" in debug_response.json()["data"]

        missing_response = client.get("/posts/999")
        assert missing_response.status_code == 404

        app.dependency_overrides.clear()
