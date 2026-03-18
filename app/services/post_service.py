from __future__ import annotations

import json
import random
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

from app.db.mongo import MongoRepository
from app.mini_redis.client import RemoteMiniRedisClient
from app.mini_redis.core import MiniRedis


class PostService:
    POSTS_CACHE_KEY = "posts:all"
    POST_CACHE_KEY_PREFIX = "post:"
    VIEW_KEY_PREFIX = "views:"
    RANKING_KEY = "rankings:posts"
    HTTP_BURST_RUNNER = Path("scripts/http_burst.js")

    def __init__(
        self,
        mongo_repo: MongoRepository,
        mini_redis: MiniRedis | RemoteMiniRedisClient,
        cache_ttl_seconds: int = 30,
    ) -> None:
        self.mongo_repo = mongo_repo
        self.mini_redis = mini_redis
        self.cache_ttl_seconds = cache_ttl_seconds
        self.last_traffic_test_result: dict[str, Any] | None = None
        self.last_multi_traffic_test_result: dict[str, Any] | None = None

    @staticmethod
    def _ranking_item_member(item: Any) -> str:
        if hasattr(item, "member"):
            return str(item.member)
        return str(item["member"])

    @staticmethod
    def _ranking_item_score(item: Any) -> float:
        if hasattr(item, "score"):
            return float(item.score)
        return float(item["score"])

    def _can_pipeline(self) -> bool:
        return isinstance(self.mini_redis, RemoteMiniRedisClient)

    def seed_posts(self, count: int, content_size: int = 128) -> dict[str, Any]:
        result = self.mongo_repo.seed_posts(count=count, content_size=content_size)
        self.mini_redis.delete(self.POSTS_CACHE_KEY)
        self.mini_redis.delete(self.RANKING_KEY)
        for post_id in self.get_available_post_ids():
            self.mini_redis.delete(f"{self.POST_CACHE_KEY_PREFIX}{post_id}")
            self.mini_redis.delete(f"{self.VIEW_KEY_PREFIX}{post_id}")
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
            if self._can_pipeline():
                self.mini_redis.pipeline(
                    [
                        {"command": "set", "key": self.POSTS_CACHE_KEY, "value": posts},
                        {"command": "expire", "key": self.POSTS_CACHE_KEY, "seconds": self.cache_ttl_seconds},
                    ]
                )
            else:
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
        return self.get_post_detail_by_mode(post_id, cache_mode="cache")

    def record_view_hit_by_mode(self, post_id: int, cache_mode: str = "cache") -> dict[str, Any]:
        started = time.perf_counter()
        if cache_mode == "db_only":
            mongo_post = self.mongo_repo.increment_post_views_slow_path(post_id, 1)
            if mongo_post is None:
                raise KeyError(f"Post {post_id} not found.")
            views = int(mongo_post.get("view_count", 0))
            ranking_score = float(views)
            data_source = "mongo_direct"
        else:
            record = self.mini_redis.record_view(
                view_key=f"{self.VIEW_KEY_PREFIX}{post_id}",
                ranking_key=self.RANKING_KEY,
                member=str(post_id),
                amount=1,
            )
            views = int(record["views"])
            ranking_score = float(record["ranking_score"])
            data_source = "mini_redis"
        elapsed_ms = (time.perf_counter() - started) * 1000
        return {
            "post_id": post_id,
            "views": views,
            "ranking_score": ranking_score,
            "data_source": data_source,
            "elapsed_ms": round(elapsed_ms, 3),
        }

    def get_post_detail_by_mode(self, post_id: int, cache_mode: str = "cache") -> dict[str, Any]:
        cache_key = f"{self.POST_CACHE_KEY_PREFIX}{post_id}"
        started = time.perf_counter()
        if cache_mode == "db_only":
            post = self.mongo_repo.increment_post_views_slow_path(post_id, 1)
            if post is None:
                raise KeyError(f"Post {post_id} not found.")
            views = int(post.get("view_count", 0))
            ranking_score = float(views)
            data_source = "mongo_direct"
        else:
            record: dict[str, Any] | None = None
            if self._can_pipeline():
                post, record = self.mini_redis.pipeline(
                    [
                        {"command": "get", "key": cache_key},
                        {
                            "command": "record_view",
                            "view_key": f"{self.VIEW_KEY_PREFIX}{post_id}",
                            "ranking_key": self.RANKING_KEY,
                            "member": str(post_id),
                            "amount": 1,
                        },
                    ]
                )
                if post is not None:
                    data_source = "mini_redis"
                    views = int(record["views"])
                    ranking_score = float(record["ranking_score"])
                    elapsed_ms = (time.perf_counter() - started) * 1000
                    return {
                        "post": post,
                        "views": views,
                        "ranking_score": ranking_score,
                        "data_source": data_source,
                        "elapsed_ms": round(elapsed_ms, 3),
                    }

            post = self.mini_redis.get(cache_key)
            if post is not None:
                data_source = "mini_redis"
            else:
                post = self.mongo_repo.get_post(post_id)
                if post is None:
                    raise KeyError(f"Post {post_id} not found.")
                if self._can_pipeline():
                    self.mini_redis.pipeline(
                        [
                            {"command": "set", "key": cache_key, "value": post},
                            {"command": "expire", "key": cache_key, "seconds": self.cache_ttl_seconds},
                        ]
                    )
                else:
                    self.mini_redis.set(cache_key, post)
                    self.mini_redis.expire(cache_key, self.cache_ttl_seconds)
                data_source = "mongo"
            if record is None:
                record = self.mini_redis.record_view(
                    view_key=f"{self.VIEW_KEY_PREFIX}{post_id}",
                    ranking_key=self.RANKING_KEY,
                    member=str(post_id),
                    amount=1,
                )
            views = int(record["views"])
            ranking_score = float(record["ranking_score"])
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
            post_id = int(self._ranking_item_member(item))
            post = self.mini_redis.get(f"{self.POST_CACHE_KEY_PREFIX}{post_id}") or self.mongo_repo.get_post(post_id)
            enriched.append(
                {
                    "post_id": post_id,
                    "score": self._ranking_item_score(item),
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

    def invalidate_cache(self, key: str) -> dict[str, Any]:
        deleted = self.mini_redis.delete(key)
        return {"deleted": deleted, "key": key}

    def set_cache_value(self, key: str, value: Any, ttl_seconds: int | None = None) -> dict[str, Any]:
        stored = self.mini_redis.set(key, value)
        ttl_applied = False
        if ttl_seconds is not None:
            ttl_applied = self.mini_redis.expire(key, ttl_seconds)
        return {
            "key": key,
            "stored": bool(stored),
            "ttl_applied": bool(ttl_applied),
            "ttl_seconds": ttl_seconds,
        }

    def get_cache_value(self, key: str) -> dict[str, Any]:
        value = self.mini_redis.get(key)
        ttl = self.mini_redis.ttl(key)
        return {
            "key": key,
            "value": value,
            "ttl_seconds": ttl,
            "found": value is not None,
        }

    def expire_cache_key(self, key: str, ttl_seconds: int) -> dict[str, Any]:
        updated = self.mini_redis.expire(key, ttl_seconds)
        ttl = self.mini_redis.ttl(key)
        return {
            "key": key,
            "updated": bool(updated),
            "ttl_seconds": ttl,
        }

    def get_cache_ttl(self, key: str) -> dict[str, Any]:
        ttl = self.mini_redis.ttl(key)
        return {
            "key": key,
            "ttl_seconds": ttl,
        }

    def get_pending_write_stats(self) -> dict[str, int]:
        return self.mini_redis.pending_write_stats()

    def flush_pending_views_to_mongo(self) -> dict[str, Any]:
        raw_deltas = self.mini_redis.flush_pending_views()
        view_deltas = {
            int(post_id): int(amount)
            for post_id, amount in raw_deltas.items()
            if int(amount) > 0
        }
        mongo_result = self.mongo_repo.apply_view_deltas(view_deltas)
        self.mini_redis.delete(self.POSTS_CACHE_KEY)
        for post_id in view_deltas:
            self.mini_redis.delete(f"{self.POST_CACHE_KEY_PREFIX}{post_id}")
        return {
            "mode": "write_behind_flush",
            "applied_posts": mongo_result["applied_posts"],
            "applied_views": mongo_result["applied_views"],
            "matched_count": mongo_result["matched_count"],
            "modified_count": mongo_result["modified_count"],
        }

    def _finalize_write_behind_run(self, cache_mode: str) -> dict[str, Any] | None:
        if cache_mode != "cache":
            return None
        pending_before = self.get_pending_write_stats()
        flush_result = self.flush_pending_views_to_mongo()
        pending_after = self.get_pending_write_stats()
        return {
            "pending_before_flush": pending_before,
            "flush_result": flush_result,
            "pending_after_flush": pending_after,
        }

    def run_view_traffic_test(
        self,
        base_url: str,
        post_id: int,
        concurrency: int,
        repeat_per_worker: int,
        cache_mode: str = "cache",
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
        method = "POST"
        path = f"/posts/{post_id}/view-hit?cache_mode={cache_mode}"
        scenario = {
            "baseUrl": normalized_base_url,
            "concurrency": concurrency,
            "repeatPerWorker": repeat_per_worker,
            "requestTimeoutMs": 10000,
            "steps": [
                {
                    "name": f"view-hit-{cache_mode}",
                    "method": method,
                    "path": path,
                    "expectedStatus": [200],
                }
            ],
        }
        scenario_path.write_text(json.dumps(scenario, ensure_ascii=False, indent=2), encoding="utf-8")

        runner_path = self.HTTP_BURST_RUNNER
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
        }
        write_behind = self._finalize_write_behind_run(cache_mode)
        if write_behind is not None:
            result["write_behind"] = write_behind
        self.last_traffic_test_result = result
        return result

    def compare_view_traffic_test(
        self,
        base_url: str,
        post_id: int,
        concurrency: int,
        repeat_per_worker: int,
    ) -> dict[str, Any]:
        cached = self.run_view_traffic_test(
            base_url=base_url,
            post_id=post_id,
            concurrency=concurrency,
            repeat_per_worker=repeat_per_worker,
            cache_mode="cache",
        )
        direct = self.run_view_traffic_test(
            base_url=base_url,
            post_id=post_id,
            concurrency=concurrency,
            repeat_per_worker=repeat_per_worker,
            cache_mode="db_only",
        )
        summary = {
            "success": cached.get("success") and direct.get("success"),
            "cache_run": cached,
            "db_direct_run": direct,
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
        steps = [
            {
                "name": f"view-hit-{post_id}-{cache_mode}-{index}",
                "method": "POST",
                "path": f"/posts/{post_id}/view-hit?cache_mode={cache_mode}",
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

        runner_path = self.HTTP_BURST_RUNNER
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
        }
        write_behind = self._finalize_write_behind_run(cache_mode)
        if write_behind is not None:
            result["write_behind"] = write_behind
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
        )
        summary = {
            "success": cached.get("success") and direct.get("success"),
            "cache_run": cached,
            "db_direct_run": direct,
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
