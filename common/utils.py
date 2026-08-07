from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Iterable

from app.map.graph import MapGraph


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


def robot_location_json(x: Any = None, y: Any = None) -> str:
    """
    把机器人的实时坐标统一写成 JSON 文本，供状态历史流水保存。

    :param x: 机器人当前地图 X 坐标，可以为空。
    :param y: 机器人当前地图 Y 坐标，可以为空。
    :return: 形如 ``{"x": 12.3, "y": 5.6}`` 的合法 JSON 字符串。
    """
    return to_json_text(
        {
            "x": float(x) if x is not None else None,
            "y": float(y) if y is not None else None,
        }
    )


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
    """规范化一个有序的 WMS 库位列表，但不改变其执行顺序."""
    return [str(site).strip() for site in site_path if site is not None and str(site).strip()]


def build_route(
    site_path: Iterable[str],
    start_site: str | None = None,
    map_data: Any | None = None,
    start_pose: dict[str, float] | None = None,
    entry_node: str | None = None,
) -> dict[str, Any]:
    """
    根据 WMS 的有序目标库位列表和机器人当前位置生成可执行路径。

    提交任务时还没有选定机器人，只有目标库位，因此此时不传 start_site；
    调度选车后再传入机器人所在节点或坐标接入节点，生成完整路线和分段信息。

    当机器人只有坐标时，``start_pose`` 保存真实坐标，``entry_node`` 保存坐标
    接入的地图节点，并额外生成一段 ``coordinateApproach``，避免把接入节点
    误认为机器人已经到达。

    :param site_path: 按执行顺序排列的目标地图节点编码。
    :param start_site: 机器人当前节点或坐标接入节点编码。
    :param map_data: 可选的 MapGraph，用于计算地图最短路径。
    :param start_pose: 机器人当前真实坐标，仅坐标接入时传入。
    :param entry_node: 坐标接入节点编码，默认使用 start_site。
    :return: 包含目标路径、地图节点路线和分段信息的字典。
    """
    requested_route = normalize_site_path(site_path)
    resolved_start = start_site.strip() if start_site else None
    resolved_entry = entry_node.strip() if entry_node else resolved_start

    if not resolved_start:
        return {
            "sitePath": requested_route,
            "route": requested_route,
            "segments": [],
        }

    waypoints = [resolved_start, *requested_route]
    route: list[str] = []
    segments = []
    if start_pose and resolved_entry:
        segments.append(
            {
                "stepIndex": 1,
                "from": resolved_entry,
                "to": resolved_entry,
                "segmentType": "coordinateApproach",
                "startPose": start_pose,
            }
        )
    for leg_from, leg_to in zip(waypoints, waypoints[1:]):
        leg_route = plan_map_segment(leg_from, leg_to, map_data)
        if not leg_route:
            continue
        if not route:
            route.extend(leg_route)
        elif route[-1] == leg_route[0]:
            route.extend(leg_route[1:])
        else:
            route.extend(leg_route)
        for index in range(len(leg_route) - 1):
            segments.append(
                {
                    "stepIndex": len(segments) + 1,
                    "from": leg_route[index],
                    "to": leg_route[index + 1],
                }
            )

    return {
        "sitePath": requested_route,
        "route": route,
        "segments": segments,
        "entryNode": resolved_entry,
        "startPose": start_pose,
    }


def plan_map_segment(
    from_site: str,
    to_site: str,
    map_data: Any | None = None,
) -> list[str]:
    """使用地图拓扑规划两个节点之间的有向最短路径。"""
    if from_site == to_site:
        if isinstance(map_data, MapGraph):
            map_data.require_nodes([from_site])
        return [from_site]
    if isinstance(map_data, MapGraph):
        return map_data.shortest_path(from_site, to_site)
    return [from_site, to_site]
