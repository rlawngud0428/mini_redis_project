from __future__ import annotations

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.models.schemas import APIResponse, SeedRequest
from app.services.post_service import PostService


router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


def get_post_service() -> PostService:
    from app.main import post_service

    return post_service


def build_dashboard_context(service: PostService, selected_post_id: int | None = None, flash: str | None = None) -> dict:
    posts_result = service.get_posts()
    rankings_result = service.get_rankings(5)
    performance_result = service.compare_performance()
    debug_result = service.debug_mini_redis()
    selected_post = None
    if selected_post_id is not None:
        try:
            selected_post = service.get_post_detail(selected_post_id)
        except KeyError:
            flash = f"Post {selected_post_id} not found."
    return {
        "posts": posts_result["posts"],
        "posts_meta": {
            "data_source": posts_result["data_source"],
            "elapsed_ms": posts_result["elapsed_ms"],
        },
        "selected_post": selected_post,
        "rankings": rankings_result["rankings"],
        "performance": performance_result,
        "debug": debug_result,
        "traffic_test": service.last_traffic_test_result,
        "multi_traffic_test": service.last_multi_traffic_test_result,
        "flash": flash,
        "mongo_backend": service.mongo_repo.health(),
    }


@router.get("/", response_class=HTMLResponse)
def dashboard(request: Request, service: PostService = Depends(get_post_service)) -> HTMLResponse:
    context = build_dashboard_context(service)
    return templates.TemplateResponse(
        name="dashboard.html",
        context={
            "request": request,
            **context,
        },
    )


@router.post("/dashboard/seed")
def dashboard_seed(
    count: int = Form(...),
    content_size: int = Form(...),
    service: PostService = Depends(get_post_service),
) -> RedirectResponse:
    service.seed_posts(count, content_size=content_size)
    return RedirectResponse(url="/?flash=seeded", status_code=status.HTTP_303_SEE_OTHER)


@router.get("/dashboard/post/{post_id}", response_class=HTMLResponse)
def dashboard_post_detail(
    post_id: int,
    request: Request,
    service: PostService = Depends(get_post_service),
) -> HTMLResponse:
    context = build_dashboard_context(service, selected_post_id=post_id)
    return templates.TemplateResponse(
        name="dashboard.html",
        context={
            "request": request,
            **context,
        },
    )


@router.post("/dashboard/cache/delete")
def dashboard_delete_cache(
    key: str = Form(...),
    service: PostService = Depends(get_post_service),
) -> RedirectResponse:
    service.invalidate_cache(key)
    return RedirectResponse(url="/?flash=cache_deleted", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/dashboard/save")
def dashboard_save(service: PostService = Depends(get_post_service)) -> RedirectResponse:
    service.save_snapshot()
    return RedirectResponse(url="/?flash=snapshot_saved", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/dashboard/load")
def dashboard_load(service: PostService = Depends(get_post_service)) -> RedirectResponse:
    service.load_snapshot()
    return RedirectResponse(url="/?flash=snapshot_loaded", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/dashboard/traffic-test")
def dashboard_traffic_test(
    request: Request,
    post_id: int = Form(...),
    concurrency: int = Form(...),
    repeat_per_worker: int = Form(...),
    cache_mode: str = Form(default="compare"),
    endpoint_kind: str = Form(default="list"),
    pure_read: str | None = Form(default="on"),
    service: PostService = Depends(get_post_service),
) -> RedirectResponse:
    if cache_mode == "compare":
        service.compare_view_traffic_test(
            base_url=str(request.base_url),
            post_id=post_id,
            concurrency=concurrency,
            repeat_per_worker=repeat_per_worker,
            endpoint_kind=endpoint_kind,
            pure_read=pure_read == "on",
        )
    else:
        service.run_view_traffic_test(
            base_url=str(request.base_url),
            post_id=post_id,
            concurrency=concurrency,
            repeat_per_worker=repeat_per_worker,
            cache_mode=cache_mode,
            endpoint_kind=endpoint_kind,
            pure_read=pure_read == "on",
        )
    return RedirectResponse(url="/?flash=traffic_test_done", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/dashboard/multi-traffic-test")
def dashboard_multi_traffic_test(
    request: Request,
    post_ids: str = Form(default=""),
    user_count: int = Form(...),
    concurrency: int = Form(...),
    repeat_per_worker: int = Form(...),
    randomize_posts: str | None = Form(default=None),
    random_step_count: int = Form(default=10),
    use_db_posts: str | None = Form(default=None),
    db_post_limit: int = Form(default=10),
    cache_mode: str = Form(default="compare"),
    endpoint_kind: str = Form(default="list"),
    pure_read: str | None = Form(default="on"),
    service: PostService = Depends(get_post_service),
) -> RedirectResponse:
    parsed_post_ids = [
        int(value.strip())
        for value in post_ids.split(",")
        if value.strip().isdigit()
    ]
    if cache_mode == "compare":
        service.compare_multi_post_traffic_test(
            base_url=str(request.base_url),
            post_ids=parsed_post_ids,
            user_count=user_count,
            concurrency=concurrency,
            repeat_per_worker=repeat_per_worker,
            randomize_posts=randomize_posts == "on",
            random_step_count=random_step_count,
            use_db_posts=use_db_posts == "on",
            db_post_limit=db_post_limit,
            pure_read=pure_read == "on",
            endpoint_kind=endpoint_kind,
        )
    else:
        service.run_multi_post_traffic_test(
            base_url=str(request.base_url),
            post_ids=parsed_post_ids,
            user_count=user_count,
            concurrency=concurrency,
            repeat_per_worker=repeat_per_worker,
            randomize_posts=randomize_posts == "on",
            random_step_count=random_step_count,
            use_db_posts=use_db_posts == "on",
            db_post_limit=db_post_limit,
            cache_mode=cache_mode,
            pure_read=pure_read == "on",
            endpoint_kind=endpoint_kind,
        )
    return RedirectResponse(url="/?flash=multi_traffic_test_done", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/seed", response_model=APIResponse)
def seed_posts(
    request: SeedRequest,
    service: PostService = Depends(get_post_service),
) -> APIResponse:
    result = service.seed_posts(request.count, content_size=request.content_size)
    return APIResponse(message="Sample posts seeded.", data=result)


@router.get("/posts", response_model=APIResponse)
def get_posts(
    cache_mode: str = Query(default="cache", pattern="^(cache|db_only)$"),
    service: PostService = Depends(get_post_service),
) -> APIResponse:
    result = service.get_posts_by_mode(cache_mode=cache_mode)
    return APIResponse(
        message="Posts fetched successfully.",
        data=result["posts"],
        meta={
            "data_source": result["data_source"],
            "elapsed_ms": result["elapsed_ms"],
            "cache_mode": cache_mode,
        },
    )


@router.get("/posts/{post_id}", response_model=APIResponse)
def get_post_detail(
    post_id: int,
    cache_mode: str = Query(default="cache", pattern="^(cache|db_only)$"),
    service: PostService = Depends(get_post_service),
) -> APIResponse:
    try:
        result = service.get_post_detail_by_mode(post_id, cache_mode=cache_mode, pure_read=False)
    except KeyError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    return APIResponse(
        message="Post fetched successfully.",
        data=result["post"],
        meta={
            "views": result["views"],
            "ranking_score": result["ranking_score"],
            "data_source": result["data_source"],
            "elapsed_ms": result["elapsed_ms"],
            "cache_mode": cache_mode,
        },
    )


@router.get("/posts/{post_id}/pure", response_model=APIResponse)
def get_post_detail_pure(
    post_id: int,
    cache_mode: str = Query(default="cache", pattern="^(cache|db_only)$"),
    service: PostService = Depends(get_post_service),
) -> APIResponse:
    try:
        result = service.get_post_detail_by_mode(post_id, cache_mode=cache_mode, pure_read=True)
    except KeyError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    return APIResponse(
        message="Post fetched successfully without side effects.",
        data=result["post"],
        meta={
            "views": result["views"],
            "ranking_score": result["ranking_score"],
            "data_source": result["data_source"],
            "elapsed_ms": result["elapsed_ms"],
            "cache_mode": cache_mode,
            "pure_read": True,
        },
    )


@router.get("/rankings", response_model=APIResponse)
def get_rankings(
    top_n: int = Query(default=5, ge=1, le=100),
    source: str = Query(default="mini_redis", pattern="^(mini_redis|mongo)$"),
    service: PostService = Depends(get_post_service),
) -> APIResponse:
    result = service.get_rankings(top_n) if source == "mini_redis" else service.get_mongo_rankings(top_n)
    return APIResponse(
        message="Rankings fetched successfully.",
        data=result["rankings"],
        meta={"source": source},
    )


@router.get("/compare/performance", response_model=APIResponse)
def compare_performance(service: PostService = Depends(get_post_service)) -> APIResponse:
    result = service.compare_performance()
    return APIResponse(message="Performance compared successfully.", data=result)


@router.delete("/cache/{key}", response_model=APIResponse)
def delete_cache(key: str, service: PostService = Depends(get_post_service)) -> APIResponse:
    result = service.invalidate_cache(key)
    return APIResponse(message="Cache invalidation finished.", data=result)


@router.post("/mini-redis/save", response_model=APIResponse)
def save_snapshot(service: PostService = Depends(get_post_service)) -> APIResponse:
    result = service.save_snapshot()
    return APIResponse(message="Mini Redis snapshot saved.", data=result)


@router.post("/mini-redis/load", response_model=APIResponse)
def load_snapshot(service: PostService = Depends(get_post_service)) -> APIResponse:
    result = service.load_snapshot()
    return APIResponse(message="Mini Redis snapshot loaded.", data=result)


@router.get("/mini-redis/debug", response_model=APIResponse)
def debug_mini_redis(service: PostService = Depends(get_post_service)) -> APIResponse:
    result = service.debug_mini_redis()
    return APIResponse(message="Mini Redis debug state fetched.", data=result)
