# Mini Redis Project

Mini Redis는 Python으로 직접 구현한 경량 Key-Value 저장소이며, MongoDB를 원본 데이터 저장소로 두고 FastAPI 기반 API 서버에서 캐시, 조회수, 랭킹 시스템으로 활용합니다.

## 핵심 메시지

- MongoDB = 원본 저장소
- Mini Redis = 빠른 캐시 계층
- 직접 만든 Mini Redis로 TTL, 조회수, 랭킹, persistence, 성능 비교를 구현

## 프로젝트 구조

```text
app/
 ├── main.py
 ├── api/
 │    └── routes.py
 ├── services/
 │    └── post_service.py
 ├── db/
 │    └── mongo.py
 ├── mini_redis/
 │    ├── core.py
 │    └── persistence.py
 ├── models/
 │    └── schemas.py

tests/
 └── test_mini_redis.py

scripts/
 └── demo_simulation.py
```

## 설치

```bash
pip install -r requirements.txt
```

## 실행

로컬 MongoDB가 있으면 자동으로 사용하고, 연결이 불가능하면 `mongomock`으로 폴백합니다.

```bash
uvicorn app.main:app --reload
```

화면 확인:

- 대시보드: `GET /`
- 헬스체크 JSON: `GET /health`
- Swagger UI: `GET /docs`

## 화면 기능

`/` 대시보드는 Jinja2 템플릿으로 렌더링되며 다음을 바로 시연할 수 있습니다.

- 샘플 게시물 생성
- 게시물 목록 확인
- 게시물 상세 조회 + 조회수 증가
- 인기 랭킹 확인
- MongoDB vs Mini Redis 성능 비교
- 특정 캐시 키 삭제
- 스냅샷 저장 / 복구
- Mini Redis 내부 상태 디버그 확인

## API 목록

- `POST /seed`
- `GET /posts`
- `GET /posts/{id}`
- `GET /rankings?top_n=5`
- `GET /compare/performance`
- `DELETE /cache/{key}`
- `POST /mini-redis/save`
- `POST /mini-redis/load`
- `GET /mini-redis/debug`

## 예시 요청

### 1. 샘플 데이터 생성

```bash
curl -X POST http://127.0.0.1:8000/seed -H "Content-Type: application/json" -d "{\"count\": 5}"
```

### 2. 게시물 목록 조회

```bash
curl http://127.0.0.1:8000/posts
```

응답에는 `data_source`와 `elapsed_ms`가 포함됩니다.

### 3. 게시물 상세 조회

```bash
curl http://127.0.0.1:8000/posts/1
```

조회 시 다음이 함께 일어납니다.

- 상세 데이터 캐싱
- 조회수 `INCR`
- 랭킹 `ZINCRBY`

## 시연 스크립트

```bash
python scripts/demo_simulation.py
```

자동 수행 항목:

1. 데이터 생성
2. 반복 조회로 조회수 증가
3. 인기 게시물 랭킹 확인
4. MongoDB vs Mini Redis 성능 비교
5. Mini Redis 스냅샷 저장
6. 스냅샷 복구 확인

## 테스트

```bash
pytest -q
```

## 설계 설명

### 1. 해시 테이블 기반 설계 원리

Mini Redis의 기본 저장소는 Python `dict`입니다. 평균적으로 O(1)에 가까운 `SET`, `GET`, `DELETE`, `INCR`이 가능하므로 캐시 계층에 적합합니다. 내부적으로는 다음 구조를 사용합니다.

- `store`: 일반 Key-Value 저장
- `expire_map`: 키별 만료 시각 저장
- `sorted_sets`: 멤버별 점수를 갖는 랭킹 저장
- `lock`: 동시성 제어

### 2. 동시성 처리 방식 설명

멀티 요청 환경에서 race condition을 막기 위해 `threading.RLock`으로 임계 구역을 보호합니다. `INCR`, `DELETE`, `EXPIRE`, `ZINCRBY` 같은 상태 변경 연산은 모두 락 안에서 실행되므로 원자성이 보장됩니다. 예를 들어 여러 요청이 동시에 조회수를 증가시켜도 카운터 유실 없이 누적됩니다.

### 3. TTL 처리 방식 설명

TTL은 `expire_map[key] = 만료시각` 형태로 관리합니다. 현재 구현은 lazy deletion 방식입니다. `GET`, `TTL`, `ZINCRBY` 같은 접근 시점마다 만료 여부를 확인하고, 만료되었으면 자동 삭제합니다. 별도 백그라운드 스레드 없이도 요구사항의 자동 삭제를 만족하면서 구조를 단순하게 유지할 수 있습니다.

### 4. 캐시 vs DB 성능 차이 원리

MongoDB 조회는 네트워크 I/O, 디스크 접근, 쿼리 처리 비용이 포함될 수 있습니다. 반면 Mini Redis 조회는 애플리케이션 메모리 안의 `dict`에서 직접 찾기 때문에 훨씬 가볍습니다. 그래서 동일한 게시물 목록을 여러 번 읽을 때 첫 요청은 MongoDB, 이후 요청은 Mini Redis 캐시가 담당하여 처리 시간이 줄어듭니다. `/compare/performance`는 `time.perf_counter()`로 이 차이를 수치로 보여줍니다.

### 5. persistence 방식 설명

현재는 RDB 스타일에 가까운 스냅샷 저장 방식을 사용합니다. `POST /mini-redis/save`를 호출하면 현재 `store`, `expire_map`, `sorted_sets`를 JSON 파일로 저장하고, `POST /mini-redis/load`로 다시 메모리에 복구합니다. 서버 재시작 뒤에도 캐시 상태와 랭킹 상태를 일부 복원할 수 있어 장애 대응 시연에 적합합니다.

## 참고

- 실제 Redis 전체 기능을 복제한 것은 아니고, 핵심 기능만 구현한 Mini Redis입니다.
- MongoDB는 원본 데이터 저장소 역할을 하고, Mini Redis는 빠른 캐시/조회수/랭킹 계층 역할을 합니다.
