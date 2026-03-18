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
    service: PostService = Depends(get_post_service),
) -> RedirectResponse:
    service.seed_posts(count)
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


@router.post("/seed", response_model=APIResponse)
def seed_posts(
    request: SeedRequest,
    service: PostService = Depends(get_post_service),
) -> APIResponse:
    result = service.seed_posts(request.count)
    return APIResponse(message="Sample posts seeded.", data=result)


@router.get("/posts", response_model=APIResponse)
def get_posts(service: PostService = Depends(get_post_service)) -> APIResponse:
    result = service.get_posts()
    return APIResponse(
        message="Posts fetched successfully.",
        data=result["posts"],
        meta={
            "data_source": result["data_source"],
            "elapsed_ms": result["elapsed_ms"],
        },
    )


@router.get("/posts/{post_id}", response_model=APIResponse)
def get_post_detail(post_id: int, service: PostService = Depends(get_post_service)) -> APIResponse:
    try:
        result = service.get_post_detail(post_id)
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
        },
    )


@router.get("/rankings", response_model=APIResponse)
def get_rankings(
    top_n: int = Query(default=5, ge=1, le=100),
    service: PostService = Depends(get_post_service),
) -> APIResponse:
    result = service.get_rankings(top_n)
    return APIResponse(message="Rankings fetched successfully.", data=result["rankings"])


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
