import unittest
import json

from app.domain import _build_root_block, build_operation_plans, task_requested_sites
from common.utils import build_route, robot_location_json
from database.models import WindTaskRecord
from plugin.ortools.solver import OrtoolsSolver


def _task() -> WindTaskRecord:
    return WindTaskRecord(
        id=100,
        input_params='{"sitePath":["SITE-1","SITE-2","SITE-3"]}',
        path='{"sitePath":["SITE-1","SITE-2","SITE-3"],"route":["SITE-1","SITE-2","SITE-3"]}',
        variables="{}",
        agv_id="AGV-001",
    )


class SequentialRouteTest(unittest.TestCase):
    def test_robot_location_is_valid_json_with_numeric_coordinates(self):
        location = json.loads(robot_location_json("12.3", "5.6"))

        self.assertEqual(location, {"x": 12.3, "y": 5.6})

    def test_build_route_adds_robot_position_before_wms_targets(self):
        route = build_route(["SITE-1", "SITE-2"], start_site="ROBOT-SITE")

        self.assertEqual(route["route"], ["ROBOT-SITE", "SITE-1", "SITE-2"])
        self.assertEqual(
            route["segments"],
            [
                {"stepIndex": 1, "from": "ROBOT-SITE", "to": "SITE-1"},
                {"stepIndex": 2, "from": "SITE-1", "to": "SITE-2"},
            ],
        )


    def test_root_blocks_are_scoped_to_one_target_phase(self):
        task = _task()
        root = _build_root_block(task, root_step_index=1, target_index=0, from_site="ROBOT-SITE", to_site="SITE-1")
        plans = build_operation_plans(
            task,
            root,
            [
                {"from": "ROBOT-SITE", "to": "SITE-MIDDLE"},
                {"from": "SITE-MIDDLE", "to": "SITE-1"},
            ],
        )

        self.assertEqual(task_requested_sites(task), ["SITE-1", "SITE-2", "SITE-3"])
        self.assertEqual(root.block_name, "RootBp")
        self.assertEqual(len(plans), 2)
        self.assertEqual(plans[0]["parentBlockId"], root.block_id)
        self.assertEqual(plans[0]["inputParams"]["to"], "SITE-MIDDLE")
        self.assertEqual(plans[1]["inputParams"]["to"], "SITE-1")
        self.assertIsNone(plans[0]["inputParams"]["scriptName"])

        plans_with_script = build_operation_plans(
            task,
            root,
            [
                {"from": "ROBOT-SITE", "to": "SITE-MIDDLE"},
                {"from": "SITE-MIDDLE", "to": "SITE-1"},
            ],
            script_name="binTask",
        )
        self.assertIsNone(plans_with_script[0]["inputParams"]["scriptName"])
        self.assertEqual(plans_with_script[1]["inputParams"]["scriptName"], "binTask")


    def test_solver_prefers_nearest_eligible_robot_before_battery(self):
        robots = [
            {
                "uuid": "FAR-HIGH-BATTERY",
                "del": 0,
                "enable_status": 1,
                "dispatch_status": 1,
                "battery_level": 99,
                "battery_threshold": 20,
                "current_site_id": "SITE-FAR",
            },
            {
                "uuid": "NEAR-LOWER-BATTERY",
                "del": 0,
                "enable_status": 1,
                "dispatch_status": 1,
                "battery_level": 60,
                "battery_threshold": 20,
                "current_site_id": "SITE-NEAR",
            },
        ]
        selected = OrtoolsSolver().choose_robot(
            robots,
            target_site="SITE-TARGET",
            site_positions={
                "SITE-FAR": (10, 10),
                "SITE-NEAR": (2, 2),
                "SITE-TARGET": (2, 3),
            },
        )

        self.assertIsNotNone(selected)
        self.assertEqual(selected["uuid"], "NEAR-LOWER-BATTERY")
