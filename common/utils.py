from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Iterable


def now() -> datetime:
    """
    统一获取当前时间，避免业务代码到处直接调用 datetime.now()。
    """
    return datetime.now()


def format_dt(value: datetime | None) -> str | None:
    """
    把时间对象格式化成接口常用字符串，空值则原样返回 None。
    """
    if value is None:
        return None
    return value.strftime("%Y-%m-%d %H:%M:%S")


def to_json_text(value: Any) -> str:
    """
    把 Python 对象安全转成 JSON 字符串，便于写入 TEXT/JSON 字段。
    """
    if value is None:
        return "{}"
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, default=str)


def from_json_text(value: str | None, default: Any = None) -> Any:
    """
    把 JSON 字符串反序列化回来，失败时返回默认值，避免业务直接炸掉。
    """
    if not value:
        return default
    try:
        return json.loads(value)
    except Exception:
        return default


def paginate(items: list[Any], page: int, page_size: int) -> dict[str, Any]:
    """
    对内存中的列表做简单分页，适合轻量级场景或已在数据库外完成筛选的数据。
    """
    total = len(items)
    start = (page - 1) * page_size
    end = start + page_size
    return {
        "page": page,
        "pageSize": page_size,
        "total": total,
        "items": items[start:end],
    }


def ensure_list(value: Any) -> list[Any]:
    """
    保证返回值一定是 list，方便后面统一按列表处理。
    """
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return list(value)


def normalize_site_path(site_path: Iterable[str]) -> list[str]:
    """Normalize an ordered WMS site list without changing its execution order."""
    return [str(site).strip() for site in site_path if site is not None and str(site).strip()]


def build_route(
    site_path: Iterable[str] | str,
    start_site: str | None = None,
    via_sites: Iterable[str] | None = None,
) -> dict[str, Any]:
    """
    Build an executable route from WMS target sites and the selected robot position.

    ``build_route("A", "B", ["C"])`` remains supported for old callers. New callers
    should pass ``build_route(["B", "C"], start_site="A")``.
    """
    if isinstance(site_path, str):
        if start_site is None:
            raise ValueError("legacy route building requires a destination site")
        requested_route = normalize_site_path([*(via_sites or []), start_site])
        resolved_start = site_path.strip()
    else:
        requested_route = normalize_site_path(site_path)
        resolved_start = start_site.strip() if start_site else None

    route = ([resolved_start] if resolved_start else []) + requested_route
    segments = []
    if resolved_start:
        for index in range(len(route) - 1):
            segments.append(
                {
                    "stepIndex": index + 1,
                    "from": route[index],
                    "to": route[index + 1],
                }
            )

    return {
        "requestedRoute": requested_route,
        "route": route,
        "segments": segments,
    }
