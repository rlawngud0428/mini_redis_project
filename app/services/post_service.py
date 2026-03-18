from __future__ import annotations

import json
import random
import shutil
import subprocess
import time
from pathlib import Path
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
        self.last_traffic_test_result: dict[str, Any] | None = None
        self.last_multi_traffic_test_result: dict[str, Any] | None = None

    def seed_posts(self, count: int, content_size: int = 128) -> dict[str, Any]:
        result = self.mongo_repo.seed_posts(count=count, content_size=content_size)
        self.mini_redis.delete(self.POSTS_CACHE_KEY)
        self.mini_redis.delete(self.RANKING_KEY)
        debug_state = self.mini_redis.debug_state()
        for key in debug_state["keys"]:
            if key.startswith(self.POST_CACHE_KEY_PREFIX) or key.startswith(self.VIEW_KEY_PREFIX):
                self.mini_redis.delete(key)
        return result

    def get_posts(self) -> dict[str, Any]:
        return self.get_posts_by_mode(cache_mode="cache")

    def get_posts_by_mode(self, cache_mode: str = "cache") -> dict[str, Any]:
        started = time.perf_counter()
        if cache_mode == "db_only":
            posts = self.mongo_repo.list_posts()
            data_source = "mongo_direct"
        else:
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
            data_source = "mongo"
        elapsed_ms = (time.perf_counter() - started) * 1000
        return {
            "posts": posts,
            "data_source": data_source,
            "elapsed_ms": round(elapsed_ms, 3),
        }

    def get_post_detail(self, post_id: int) -> dict[str, Any]:
        return self.get_post_detail_by_mode(post_id, cache_mode="cache", pure_read=False)

    def record_view_hit_by_mode(self, post_id: int, cache_mode: str = "cache") -> dict[str, Any]:
        started = time.perf_counter()
        post = self.mongo_repo.get_post(post_id)
        if post is None:
            raise KeyError(f"Post {post_id} not found.")
        if cache_mode == "db_only":
            mongo_post = self.mongo_repo.increment_post_views(post_id, 1)
            if mongo_post is None:
                raise KeyError(f"Post {post_id} not found.")
            views = int(mongo_post.get("view_count", 0))
            ranking_score = float(views)
            data_source = "mongo_direct"
        else:
            views = self.mini_redis.incr(f"{self.VIEW_KEY_PREFIX}{post_id}")
            ranking_score = self.mini_redis.zincrby(self.RANKING_KEY, 1, str(post_id))
            data_source = "mini_redis"
        elapsed_ms = (time.perf_counter() - started) * 1000
        return {
            "post_id": post_id,
            "title": post["title"],
            "views": views,
            "ranking_score": ranking_score,
            "data_source": data_source,
            "elapsed_ms": round(elapsed_ms, 3),
        }

    def get_post_detail_by_mode(self, post_id: int, cache_mode: str = "cache", pure_read: bool = False) -> dict[str, Any]:
        cache_key = f"{self.POST_CACHE_KEY_PREFIX}{post_id}"
        started = time.perf_counter()
        mongo_post = None
        if cache_mode == "db_only":
            post = self.mongo_repo.get_post(post_id)
            if post is None:
                raise KeyError(f"Post {post_id} not found.")
            data_source = "mongo_direct"
        else:
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
        if pure_read:
            views = self.mini_redis.get(f"{self.VIEW_KEY_PREFIX}{post_id}") or 0
            ranking_score = next(
                (item.score for item in self.mini_redis.zrange(self.RANKING_KEY, 1000, desc=True) if item.member == str(post_id)),
                0.0,
            )
        elif cache_mode == "db_only":
            mongo_post = self.mongo_repo.increment_post_views(post_id, 1)
            if mongo_post is None:
                raise KeyError(f"Post {post_id} not found.")
            post = mongo_post
            views = int(mongo_post.get("view_count", 0))
            ranking_score = float(views)
        else:
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

    def get_mongo_rankings(self, top_n: int) -> dict[str, Any]:
        docs = self.mongo_repo.top_posts_by_views(limit=top_n)
        return {
            "rankings": [
                {
                    "post_id": int(doc["post_id"]),
                    "score": float(doc.get("view_count", 0)),
                    "title": doc.get("title"),
                }
                for doc in docs
            ]
        }

    def get_available_post_ids(self, limit: int | None = None) -> list[int]:
        posts = self.mongo_repo.list_posts()
        post_ids = [int(post["post_id"]) for post in posts]
        if limit is not None and limit > 0:
            return post_ids[:limit]
        return post_ids

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

    def run_view_traffic_test(
        self,
        base_url: str,
        post_id: int,
        concurrency: int,
        repeat_per_worker: int,
        cache_mode: str = "cache",
        endpoint_kind: str = "detail",
        pure_read: bool = False,
    ) -> dict[str, Any]:
        node_path = shutil.which("node")
        if not node_path:
            result = {
                "success": False,
                "reason": "node_not_found",
                "message": "Node.js is required to run the traffic tester.",
            }
            self.last_traffic_test_result = result
            return result

        scenario_dir = Path("scripts/scenarios")
        scenario_dir.mkdir(parents=True, exist_ok=True)
        scenario_path = scenario_dir / "view_burst.json"
        normalized_base_url = base_url.rstrip("/") + "/"
        method = "GET"
        if endpoint_kind == "list":
            path = f"/posts?cache_mode={cache_mode}"
        elif endpoint_kind == "view_hit":
            method = "POST"
            path = f"/posts/{post_id}/view-hit?cache_mode={cache_mode}"
        elif pure_read:
            path = f"/posts/{post_id}/pure?cache_mode={cache_mode}"
        else:
            path = f"/posts/{post_id}?cache_mode={cache_mode}"
        scenario = {
            "baseUrl": normalized_base_url,
            "concurrency": concurrency,
            "repeatPerWorker": repeat_per_worker,
            "requestTimeoutMs": 10000,
            "steps": [
                {
                    "name": f"view-{endpoint_kind}-{cache_mode}",
                    "method": method,
                    "path": path,
                    "expectedStatus": [200],
                }
            ],
        }
        scenario_path.write_text(json.dumps(scenario, ensure_ascii=False, indent=2), encoding="utf-8")

        runner_path = Path(r"C:\Users\kjh\.codex\skills\traffic-tester\scripts\http_burst.js")
        command = [node_path, str(runner_path), "--scenario", str(scenario_path)]
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=120,
            check=False,
        )
        stdout = completed.stdout.strip()
        stderr = completed.stderr.strip()

        if completed.returncode != 0:
            result = {
                "success": False,
                "reason": "runner_failed",
                "message": stderr or stdout or "Traffic tester failed.",
                "command": command,
                "scenario_path": str(scenario_path),
            }
            self.last_traffic_test_result = result
            return result

        parsed = json.loads(stdout)
        result = {
            "success": True,
            "scenario_path": str(scenario_path),
            "command": command,
            "summary": parsed,
            "cache_mode": cache_mode,
            "endpoint_kind": endpoint_kind,
            "pure_read": pure_read,
        }
        self.last_traffic_test_result = result
        return result

    def compare_view_traffic_test(
        self,
        base_url: str,
        post_id: int,
        concurrency: int,
        repeat_per_worker: int,
        endpoint_kind: str = "detail",
        pure_read: bool = False,
    ) -> dict[str, Any]:
        cached = self.run_view_traffic_test(
            base_url=base_url,
            post_id=post_id,
            concurrency=concurrency,
            repeat_per_worker=repeat_per_worker,
            cache_mode="cache",
            endpoint_kind=endpoint_kind,
            pure_read=pure_read,
        )
        direct = self.run_view_traffic_test(
            base_url=base_url,
            post_id=post_id,
            concurrency=concurrency,
            repeat_per_worker=repeat_per_worker,
            cache_mode="db_only",
            endpoint_kind=endpoint_kind,
            pure_read=pure_read,
        )
        summary = {
            "success": cached.get("success") and direct.get("success"),
            "cache_run": cached,
            "db_direct_run": direct,
            "endpoint_kind": endpoint_kind,
            "pure_read": pure_read,
        }
        if cached.get("success") and direct.get("success"):
            cache_avg = cached["summary"].get("avgLatencyMs")
            db_avg = direct["summary"].get("avgLatencyMs")
            cache_p95 = cached["summary"].get("p95LatencyMs")
            db_p95 = direct["summary"].get("p95LatencyMs")
            summary["comparison"] = {
                "avg_latency_gap_ms": None if cache_avg is None or db_avg is None else round(db_avg - cache_avg, 2),
                "p95_latency_gap_ms": None if cache_p95 is None or db_p95 is None else round(db_p95 - cache_p95, 2),
                "db_vs_cache_avg_ratio": None if not cache_avg else round(db_avg / cache_avg, 3),
            }
        self.last_traffic_test_result = summary
        return summary

    def run_multi_post_traffic_test(
        self,
        base_url: str,
        post_ids: list[int] | None,
        user_count: int,
        concurrency: int,
        repeat_per_worker: int,
        randomize_posts: bool = False,
        random_step_count: int = 10,
        use_db_posts: bool = False,
        db_post_limit: int = 10,
        cache_mode: str = "cache",
        pure_read: bool = False,
        endpoint_kind: str = "detail",
    ) -> dict[str, Any]:
        node_path = shutil.which("node")
        if not node_path:
            result = {
                "success": False,
                "reason": "node_not_found",
                "message": "Node.js is required to run the traffic tester.",
            }
            self.last_multi_traffic_test_result = result
            return result

        source_post_ids = self.get_available_post_ids(db_post_limit) if use_db_posts else (post_ids or [])
        sanitized_post_ids = [post_id for post_id in source_post_ids if post_id > 0]
        if not sanitized_post_ids:
            result = {
                "success": False,
                "reason": "invalid_post_ids",
                "message": "At least one valid post id is required. Seed posts first or provide manual post ids.",
            }
            self.last_multi_traffic_test_result = result
            return result

        scenario_dir = Path("scripts/scenarios")
        scenario_dir.mkdir(parents=True, exist_ok=True)
        scenario_path = scenario_dir / "multi_view_burst.json"
        normalized_base_url = base_url.rstrip("/") + "/"
        users = [{"name": f"user-{index}"} for index in range(1, user_count + 1)]
        if randomize_posts:
            step_post_ids = [random.choice(sanitized_post_ids) for _ in range(max(1, random_step_count))]
        else:
            step_post_ids = sanitized_post_ids
        if endpoint_kind == "list":
            steps = [
                {
                    "name": f"list-posts-{cache_mode}-{index}",
                    "method": "GET",
                    "path": f"/posts?cache_mode={cache_mode}",
                    "expectedStatus": [200],
                }
                for index in range(1, max(2, len(step_post_ids) + 1))
            ]
        elif endpoint_kind == "view_hit":
            steps = [
                {
                    "name": f"view-hit-{post_id}-{cache_mode}-{index}",
                    "method": "POST",
                    "path": f"/posts/{post_id}/view-hit?cache_mode={cache_mode}",
                    "expectedStatus": [200],
                }
                for index, post_id in enumerate(step_post_ids, start=1)
            ]
        else:
            path_builder = (
                lambda current_post_id: f"/posts/{current_post_id}/pure?cache_mode={cache_mode}"
                if pure_read
                else f"/posts/{current_post_id}?cache_mode={cache_mode}"
            )
            steps = [
                {
                    "name": f"view-post-{post_id}-{cache_mode}-{index}",
                    "method": "GET",
                    "path": path_builder(post_id),
                    "expectedStatus": [200],
                }
                for index, post_id in enumerate(step_post_ids, start=1)
            ]
        scenario = {
            "baseUrl": normalized_base_url,
            "concurrency": concurrency,
            "repeatPerWorker": repeat_per_worker,
            "requestTimeoutMs": 10000,
            "users": users,
            "steps": steps,
        }
        scenario_path.write_text(json.dumps(scenario, ensure_ascii=False, indent=2), encoding="utf-8")

        runner_path = Path(r"C:\Users\kjh\.codex\skills\traffic-tester\scripts\http_burst.js")
        command = [node_path, str(runner_path), "--scenario", str(scenario_path)]
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=120,
            check=False,
        )
        stdout = completed.stdout.strip()
        stderr = completed.stderr.strip()

        if completed.returncode != 0:
            result = {
                "success": False,
                "reason": "runner_failed",
                "message": stderr or stdout or "Traffic tester failed.",
                "command": command,
                "scenario_path": str(scenario_path),
            }
            self.last_multi_traffic_test_result = result
            return result

        parsed = json.loads(stdout)
        result = {
            "success": True,
            "scenario_path": str(scenario_path),
            "command": command,
            "summary": parsed,
            "post_ids": sanitized_post_ids,
            "user_count": user_count,
            "randomize_posts": randomize_posts,
            "step_post_ids": step_post_ids,
            "use_db_posts": use_db_posts,
            "db_post_limit": db_post_limit,
            "cache_mode": cache_mode,
            "endpoint_kind": endpoint_kind,
            "pure_read": pure_read,
        }
        self.last_multi_traffic_test_result = result
        return result

    def compare_multi_post_traffic_test(
        self,
        base_url: str,
        post_ids: list[int] | None,
        user_count: int,
        concurrency: int,
        repeat_per_worker: int,
        randomize_posts: bool = False,
        random_step_count: int = 10,
        use_db_posts: bool = False,
        db_post_limit: int = 10,
        pure_read: bool = False,
        endpoint_kind: str = "detail",
    ) -> dict[str, Any]:
        cached = self.run_multi_post_traffic_test(
            base_url=base_url,
            post_ids=post_ids,
            user_count=user_count,
            concurrency=concurrency,
            repeat_per_worker=repeat_per_worker,
            randomize_posts=randomize_posts,
            random_step_count=random_step_count,
            use_db_posts=use_db_posts,
            db_post_limit=db_post_limit,
            cache_mode="cache",
            pure_read=pure_read,
            endpoint_kind=endpoint_kind,
        )
        direct = self.run_multi_post_traffic_test(
            base_url=base_url,
            post_ids=post_ids,
            user_count=user_count,
            concurrency=concurrency,
            repeat_per_worker=repeat_per_worker,
            randomize_posts=randomize_posts,
            random_step_count=random_step_count,
            use_db_posts=use_db_posts,
            db_post_limit=db_post_limit,
            cache_mode="db_only",
            pure_read=pure_read,
            endpoint_kind=endpoint_kind,
        )
        summary = {
            "success": cached.get("success") and direct.get("success"),
            "cache_run": cached,
            "db_direct_run": direct,
            "endpoint_kind": endpoint_kind,
            "pure_read": pure_read,
        }
        if cached.get("success") and direct.get("success"):
            cache_avg = cached["summary"].get("avgLatencyMs")
            db_avg = direct["summary"].get("avgLatencyMs")
            cache_p95 = cached["summary"].get("p95LatencyMs")
            db_p95 = direct["summary"].get("p95LatencyMs")
            summary["comparison"] = {
                "avg_latency_gap_ms": None if cache_avg is None or db_avg is None else round(db_avg - cache_avg, 2),
                "p95_latency_gap_ms": None if cache_p95 is None or db_p95 is None else round(db_p95 - cache_p95, 2),
                "db_vs_cache_avg_ratio": None if not cache_avg else round(db_avg / cache_avg, 3),
            }
        self.last_multi_traffic_test_result = summary
        return summary
