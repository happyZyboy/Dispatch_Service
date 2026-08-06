from __future__ import annotations

from typing import Any

from common.enums.dispatch_status import DispatchStatus


class OrtoolsSolver:
    def choose_robot(
        self,
        robots: list[dict[str, Any]],
        required_uuid: str | None = None,
        target_site: str | None = None,
        site_positions: dict[str, tuple[int | float | None, int | float | None]] | None = None,
        route_costs: dict[str, float | None] | None = None,
    ) -> dict[str, Any] | None:
        """
        从候选列表中选择指定机器人，或选择距离首个目标点最近的可调度机器人。

        优先使用地图拓扑路径成本；没有地图成本时保留行列坐标作为兼容回退。
        """
        def is_eligible(robot: dict[str, Any]) -> bool:
            route_available = required_uuid or route_costs is None or route_costs.get(robot.get("uuid")) is not None
            return bool(
                route_available
                and robot.get("del_", robot.get("del", 0)) == 0
                and robot.get("enable_status", 0) == 1
                and robot.get("dispatch_status") == DispatchStatus.IDLE
                and robot.get("has_unresolved_alarm", 0) == 0
                and float(robot.get("battery_level") or 0)
                >= float(robot.get("battery_threshold") or 0)
                and bool(robot.get("current_site_id") or robot.get("current_location"))
            )

        def distance_to_target(robot: dict[str, Any]) -> float | None:
            if route_costs is not None:
                value = route_costs.get(robot.get("uuid"))
                return float(value) if value is not None else None
            if not target_site:
                return None
            current_site = robot.get("current_site_id")
            if current_site == target_site:
                return 0
            if not site_positions or current_site not in site_positions or target_site not in site_positions:
                return None
            current_row, current_column = site_positions[current_site]
            target_row, target_column = site_positions[target_site]
            if None in (current_row, current_column, target_row, target_column):
                return None
            return abs(float(current_row) - float(target_row)) + abs(float(current_column) - float(target_column))

        if required_uuid:
            for robot in robots:
                if robot.get("uuid") == required_uuid and is_eligible(robot):
                    return robot
            return None

        candidates = [robot for robot in robots if is_eligible(robot)]
        candidates.sort(
            key=lambda item: (
                distance_to_target(item) is None,
                distance_to_target(item) if distance_to_target(item) is not None else float("inf"),
                -float(item.get("battery_level") or 0),
            )
        )
        return candidates[0] if candidates else None
