from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from common.enums.dispatch_status import DispatchStatus
from common.utils import to_json_text
from core.conf import settings
from database.models import AlarmRecord, RobotCurrentState, RobotItem, RobotStatusRecord, WindTaskDef


async def seed_database(session: AsyncSession) -> None:
    """
    在对应数据表为空时写入初版任务模板、机器人和报警种子数据。
    """
    # 只在空库时补默认数据，避免覆盖手工维护内容。
    if await session.scalar(select(func.count()).select_from(WindTaskDef)) == 0:
        _seed_task_defs(session)
    if await session.scalar(select(func.count()).select_from(RobotItem)) == 0:
        await _seed_robots(session)
    if await session.scalar(select(func.count()).select_from(AlarmRecord)) == 0:
        _seed_alarms(session)
    await session.commit()


def _seed_task_defs(session: AsyncSession) -> None:
    """
    创建一份默认任务模板，提供初版任务提交和调度流程使用。
    """
    # 默认模板只保留最小可跑链路，后面再扩展复杂 block 树。
    default_detail = {
        "inputParams": [
            {"name": "sitePath", "type": "JSONArray", "label": "按顺序执行的地图节点路径", "required": True, "defaultValue": []},
            {"name": "vehicle", "type": "String", "label": "指定车辆", "required": False, "defaultValue": ""},
            {"name": "priority", "type": "Integer", "label": "优先级", "required": False, "defaultValue": 5},
        ],
        "outputParams": [],
        "rootBlock": {
            "id": -1,
            "name": "-1",
            "blockType": "RootBp",
            "inputParams": {},
            "children": {"default": []},
        },
    }
    session.add(
        WindTaskDef(
            label=settings.default_task_label,
            template_name="默认搬运模板",
            detail=to_json_text(default_detail),
            status=1,
            if_enable=1,
            version=1,
            remark="初版默认模板",
        )
    )


async def _seed_robots(session: AsyncSession) -> None:
    """
    创建默认机器人档案、机器人状态快照和初始状态流水。
    """
    # 默认种两台在线空闲车，方便联调和演示。
    robot_1 = RobotItem(uuid="AGV-001", robot_code="R001", robot_name="AGV-001", robot_type="AMR", enable_status=1, battery_threshold=20, current_map="default")
    robot_2 = RobotItem(uuid="AGV-002", robot_code="R002", robot_name="AGV-002", robot_type="AMR", enable_status=1, battery_threshold=20, current_map="default")
    session.add_all([robot_1, robot_2])
    await session.flush()
    session.add_all(
        [
            RobotCurrentState(
                robot_id=robot_1.id,
                uuid=robot_1.uuid,
                vehicle_name=robot_1.robot_name,
                current_status=1,
                dispatch_status=DispatchStatus.IDLE,
                current_site_id="LM1",
                battery_level=88,
                last_heartbeat_at=robot_1.added_on,
            ),
            RobotCurrentState(
                robot_id=robot_2.id,
                uuid=robot_2.uuid,
                vehicle_name=robot_2.robot_name,
                current_status=1,
                dispatch_status=DispatchStatus.IDLE,
                current_site_id="LM3",
                battery_level=76,
                last_heartbeat_at=robot_2.added_on,
            ),
            RobotStatusRecord(uuid=robot_1.uuid, vehicle_name=robot_1.robot_name, old_status=0, new_status=1, location="LM1"),
            RobotStatusRecord(uuid=robot_2.uuid, vehicle_name=robot_2.robot_name, old_status=0, new_status=1, location="LM3"),
        ]
    )


def _seed_alarms(session: AsyncSession) -> None:
    """
    创建一条默认报警记录，方便联调报警查询和恢复接口。
    """
    session.add(
        AlarmRecord(
            alarms_code="ALM-AGV-BAT-001",
            alarms_desc="电量偏低示例记录",
            level="WARNING",
            vehicle_id="AGV-001",
            type_=0,
        )
    )
