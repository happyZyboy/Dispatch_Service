import unittest
import json

from common.utils import build_route, robot_location_json
from plugin.ortools.solver import OrtoolsSolver


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
