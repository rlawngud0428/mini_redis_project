from __future__ import annotations

import json
import random

from app.db.mongo import MongoRepository
from app.mini_redis.core import MiniRedis
from app.mini_redis.persistence import MiniRedisPersistence
from app.services.post_service import PostService


def main() -> None:
    mongo_repo = MongoRepository()
    mini_redis = MiniRedis()
    persistence = MiniRedisPersistence()
    service = PostService(mongo_repo, mini_redis, persistence)

    print("1. 데이터 생성")
    print(json.dumps(service.seed_posts(5), ensure_ascii=False, indent=2))

    print("\n2. 조회 반복 -> 조회수 증가")
    for post_id in [1, 2, 1, 3, 1, 2, 4, 2, 2, random.choice([1, 2, 3, 4, 5])]:
        result = service.get_post_detail(post_id)
        print(
            json.dumps(
                {
                    "post_id": post_id,
                    "views": result["views"],
                    "data_source": result["data_source"],
                },
                ensure_ascii=False,
            )
        )

    print("\n3. 인기 게시물 순위 변화 확인")
    print(json.dumps(service.get_rankings(5), ensure_ascii=False, indent=2))

    print("\n4. 성능 비교 실행")
    print(json.dumps(service.compare_performance(), ensure_ascii=False, indent=2))

    print("\n5. persistence 저장")
    print(json.dumps(service.save_snapshot(), ensure_ascii=False, indent=2))

    print("\n6. 복구 확인")
    mini_redis.delete(service.POSTS_CACHE_KEY)
    mini_redis.delete(service.RANKING_KEY)
    print(json.dumps({"before_load": service.debug_mini_redis()}, ensure_ascii=False, indent=2))
    print(json.dumps(service.load_snapshot(), ensure_ascii=False, indent=2))
    print(json.dumps({"after_load": service.debug_mini_redis()}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
