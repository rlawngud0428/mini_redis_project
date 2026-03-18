from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class PostSchema(BaseModel):
    post_id: int
    title: str
    content: str
    created_at: str


class SeedRequest(BaseModel):
    count: int = Field(default=10, ge=1, le=1000)


class APIResponse(BaseModel):
    success: bool = True
    message: str
    data: Any | None = None
    meta: dict[str, Any] = Field(default_factory=dict)
