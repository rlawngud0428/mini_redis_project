from __future__ import annotations

import argparse
import json
import os
import shlex
from typing import Any

from app.db.mongo import MongoRepository
from app.mini_redis.client import RemoteMiniRedisClient
from app.services.post_service import PostService


def _env_bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).lower() == "true"


def _build_service(args: argparse.Namespace) -> tuple[PostService, RemoteMiniRedisClient]:
    mongo_repo = MongoRepository(
        mongo_uri=args.mongo_uri,
        force_mock=args.use_mock_mongo,
    )
    mini_redis = RemoteMiniRedisClient(
        base_url=args.mini_redis_url,
    )
    return PostService(mongo_repo=mongo_repo, mini_redis=mini_redis), mini_redis


def _build_mini_redis_client(args: argparse.Namespace) -> RemoteMiniRedisClient:
    return RemoteMiniRedisClient(base_url=args.mini_redis_url)


def _print_json(data: Any) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2))


def _comma_separated_ints(value: str) -> list[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def _parse_value(value: str) -> Any:
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _shell_help() -> dict[str, Any]:
    return {
        "commands": [
            "PING",
            "SET <key> <value> [ttl_seconds]",
            "GET <key>",
            "KEYS",
            "DUMPALL",
            "DEL <key>",
            "INCR <key> [amount]",
            "EXPIRE <key> <seconds>",
            "TTL <key>",
            "ZINCRBY <key> <score> <member>",
            "ZRANGE <key> <top_n> [desc]",
            "HELP",
            "EXIT",
            "QUIT",
        ],
        "notes": [
            "JSON value is supported for SET. Example: SET user:1 {\"name\":\"kim\"}",
            "SET applies TTL 30 seconds by default unless you pass a custom ttl.",
            "Use desc=true to view rankings in descending order.",
        ],
    }


def _run_shell_command(mini_redis: RemoteMiniRedisClient, line: str) -> tuple[bool, Any]:
    parts = shlex.split(line)
    if not parts:
        return True, None

    command = parts[0].lower()
    try:
        if command in {"exit", "quit"}:
            return False, {"message": "Bye."}
        if command == "help":
            return True, _shell_help()
        if command == "ping":
            return True, mini_redis.ping()
        if command == "set":
            if len(parts) not in {3, 4}:
                raise ValueError("Usage: SET <key> <value> [ttl_seconds]")
            key = parts[1]
            ttl_seconds = int(parts[3]) if len(parts) == 4 else 30
            value = _parse_value(parts[2])
            return True, {
                "ok": mini_redis.set(key, value),
                "ttl_applied": mini_redis.expire(key, ttl_seconds),
                "ttl_seconds": ttl_seconds,
            }
        if command == "get":
            if len(parts) != 2:
                raise ValueError("Usage: GET <key>")
            key = parts[1]
            return True, {"key": key, "value": mini_redis.get(key), "ttl_seconds": mini_redis.ttl(key)}
        if command == "keys":
            if len(parts) != 1:
                raise ValueError("Usage: KEYS")
            return True, {"keys": mini_redis.keys()}
        if command == "dumpall":
            if len(parts) != 1:
                raise ValueError("Usage: DUMPALL")
            return True, mini_redis.dumpall()
        if command in {"del", "delete"}:
            if len(parts) != 2:
                raise ValueError("Usage: DEL <key>")
            return True, {"deleted": mini_redis.delete(parts[1])}
        if command == "incr":
            if len(parts) not in {2, 3}:
                raise ValueError("Usage: INCR <key> [amount]")
            amount = int(parts[2]) if len(parts) == 3 else 1
            return True, {"value": mini_redis.incr(parts[1], amount=amount)}
        if command == "expire":
            if len(parts) != 3:
                raise ValueError("Usage: EXPIRE <key> <seconds>")
            return True, {"updated": mini_redis.expire(parts[1], int(parts[2]))}
        if command == "ttl":
            if len(parts) != 2:
                raise ValueError("Usage: TTL <key>")
            return True, {"ttl_seconds": mini_redis.ttl(parts[1])}
        if command == "zincrby":
            if len(parts) != 4:
                raise ValueError("Usage: ZINCRBY <key> <score> <member>")
            return True, {"score": mini_redis.zincrby(parts[1], float(parts[2]), parts[3])}
        if command == "zrange":
            if len(parts) not in {3, 4}:
                raise ValueError("Usage: ZRANGE <key> <top_n> [desc]")
            desc = len(parts) == 4 and parts[3].lower() in {"1", "true", "desc", "yes"}
            return True, {"items": mini_redis.zrange(parts[1], int(parts[2]), desc=desc)}
        raise ValueError(f"Unknown command: {parts[0]}. Type HELP for available commands.")
    except Exception as error:  # noqa: BLE001
        return True, {"error": str(error)}


def _interactive_shell(mini_redis: RemoteMiniRedisClient) -> int:
    print("Connected to Mini Redis shell. Type HELP for commands, EXIT to quit.")
    while True:
        try:
            line = input("mini-redis> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        should_continue, result = _run_shell_command(mini_redis, line)
        if result is not None:
            _print_json(result)
        if not should_continue:
            break
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Mini Redis demo CLI")
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27017"))
    parser.add_argument("--mini-redis-url", default=os.getenv("MINI_REDIS_URL", "tcp://localhost:6380"))
    parser.add_argument("--base-url", default=os.getenv("APP_BASE_URL", "http://127.0.0.1:8000"))
    parser.add_argument(
        "--use-mock-mongo",
        action=argparse.BooleanOptionalAction,
        default=_env_bool("USE_MOCK_MONGO", False),
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    seed = subparsers.add_parser("seed", help="Seed sample posts")
    seed.add_argument("--count", type=int, default=10)
    seed.add_argument("--content-size", type=int, default=128)

    posts = subparsers.add_parser("posts", help="List posts")
    posts.add_argument("--cache-mode", choices=["cache", "db_only"], default="cache")

    post = subparsers.add_parser("post", help="Fetch a single post detail")
    post.add_argument("post_id", type=int)
    post.add_argument("--cache-mode", choices=["cache", "db_only"], default="cache")

    view_hit = subparsers.add_parser("view-hit", help="Record a view hit")
    view_hit.add_argument("post_id", type=int)
    view_hit.add_argument("--cache-mode", choices=["cache", "db_only"], default="cache")

    rankings = subparsers.add_parser("rankings", help="Show rankings")
    rankings.add_argument("--top-n", type=int, default=5)
    rankings.add_argument("--source", choices=["mini_redis", "mongo"], default="mini_redis")

    subparsers.add_parser("pending", help="Show pending write-behind stats")
    subparsers.add_parser("flush", help="Flush pending Mini Redis views to MongoDB")

    cache_delete = subparsers.add_parser("cache-delete", help="Delete a cache key")
    cache_delete.add_argument("key")

    cache_set = subparsers.add_parser("cache-set", help="Set a cache key and optional TTL")
    cache_set.add_argument("key")
    cache_set.add_argument("value", type=_parse_value)
    cache_set.add_argument("--ttl", type=int, default=30)

    cache_get = subparsers.add_parser("cache-get", help="Get a cache key and TTL")
    cache_get.add_argument("key")

    cache_expire = subparsers.add_parser("cache-expire", help="Apply TTL to an existing cache key")
    cache_expire.add_argument("key")
    cache_expire.add_argument("ttl", type=int)

    cache_ttl = subparsers.add_parser("cache-ttl", help="Check remaining TTL for a cache key")
    cache_ttl.add_argument("key")

    subparsers.add_parser("keys", help="List all Mini Redis keys")
    subparsers.add_parser("dumpall", help="Dump all Mini Redis values and internal sections")

    subparsers.add_parser("shell", help="Open interactive Mini Redis shell")

    health = subparsers.add_parser("health", help="Show service health")
    health.add_argument("--top-n", type=int, default=5)

    traffic = subparsers.add_parser("traffic-test", help="Run single-post traffic test")
    traffic.add_argument("--post-id", type=int, required=True)
    traffic.add_argument("--concurrency", type=int, default=20)
    traffic.add_argument("--repeat-per-worker", type=int, default=5)
    traffic.add_argument("--cache-mode", choices=["cache", "db_only", "compare"], default="compare")

    multi = subparsers.add_parser("multi-traffic-test", help="Run multi-post traffic test")
    multi.add_argument("--post-ids", type=_comma_separated_ints, default=None)
    multi.add_argument("--user-count", type=int, default=5)
    multi.add_argument("--concurrency", type=int, default=20)
    multi.add_argument("--repeat-per-worker", type=int, default=3)
    multi.add_argument("--randomize-posts", action="store_true")
    multi.add_argument("--random-step-count", type=int, default=10)
    multi.add_argument("--use-db-posts", action="store_true")
    multi.add_argument("--db-post-limit", type=int, default=10)
    multi.add_argument("--cache-mode", choices=["cache", "db_only", "compare"], default="compare")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command in {"shell", "keys", "dumpall"}:
        mini_redis = _build_mini_redis_client(args)
        try:
            if args.command == "shell":
                return _interactive_shell(mini_redis)
            if args.command == "keys":
                _print_json({"keys": mini_redis.keys()})
                return 0
            _print_json(mini_redis.dumpall())
            return 0
        finally:
            mini_redis.close()

    service, mini_redis = _build_service(args)

    try:
        if args.command == "seed":
            result = service.seed_posts(count=args.count, content_size=args.content_size)
        elif args.command == "posts":
            result = service.get_posts_by_mode(cache_mode=args.cache_mode)
        elif args.command == "post":
            result = service.get_post_detail_by_mode(post_id=args.post_id, cache_mode=args.cache_mode)
        elif args.command == "view-hit":
            result = service.record_view_hit_by_mode(post_id=args.post_id, cache_mode=args.cache_mode)
        elif args.command == "rankings":
            if args.source == "mongo":
                result = service.get_mongo_rankings(top_n=args.top_n)
            else:
                result = service.get_rankings(top_n=args.top_n)
        elif args.command == "pending":
            result = service.get_pending_write_stats()
        elif args.command == "flush":
            result = service.flush_pending_views_to_mongo()
        elif args.command == "cache-delete":
            result = service.invalidate_cache(args.key)
        elif args.command == "cache-set":
            result = service.set_cache_value(key=args.key, value=args.value, ttl_seconds=args.ttl)
        elif args.command == "cache-get":
            result = service.get_cache_value(args.key)
        elif args.command == "cache-expire":
            result = service.expire_cache_key(key=args.key, ttl_seconds=args.ttl)
        elif args.command == "cache-ttl":
            result = service.get_cache_ttl(args.key)
        elif args.command == "health":
            result = {
                "mongo_backend": service.mongo_repo.health(),
                "mini_redis_backend": mini_redis.ping().get("status", "unknown"),
                "pending_write_stats": service.get_pending_write_stats(),
                "rankings_preview": service.get_rankings(top_n=args.top_n),
            }
        elif args.command == "traffic-test":
            if args.cache_mode == "compare":
                result = service.compare_view_traffic_test(
                    base_url=args.base_url,
                    post_id=args.post_id,
                    concurrency=args.concurrency,
                    repeat_per_worker=args.repeat_per_worker,
                )
            else:
                result = service.run_view_traffic_test(
                    base_url=args.base_url,
                    post_id=args.post_id,
                    concurrency=args.concurrency,
                    repeat_per_worker=args.repeat_per_worker,
                    cache_mode=args.cache_mode,
                )
        elif args.command == "multi-traffic-test":
            if args.cache_mode == "compare":
                result = service.compare_multi_post_traffic_test(
                    base_url=args.base_url,
                    post_ids=args.post_ids,
                    user_count=args.user_count,
                    concurrency=args.concurrency,
                    repeat_per_worker=args.repeat_per_worker,
                    randomize_posts=args.randomize_posts,
                    random_step_count=args.random_step_count,
                    use_db_posts=args.use_db_posts,
                    db_post_limit=args.db_post_limit,
                )
            else:
                result = service.run_multi_post_traffic_test(
                    base_url=args.base_url,
                    post_ids=args.post_ids,
                    user_count=args.user_count,
                    concurrency=args.concurrency,
                    repeat_per_worker=args.repeat_per_worker,
                    randomize_posts=args.randomize_posts,
                    random_step_count=args.random_step_count,
                    use_db_posts=args.use_db_posts,
                    db_post_limit=args.db_post_limit,
                    cache_mode=args.cache_mode,
                )
        else:
            parser.error(f"Unsupported command: {args.command}")
            return 2

        _print_json(result)
        return 0
    finally:
        mini_redis.close()


if __name__ == "__main__":
    raise SystemExit(main())
