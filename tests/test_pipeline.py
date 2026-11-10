import tempfile
import unittest
from pathlib import Path

from waste_lakehouse.pipeline import aggregate, run, transform


VALID = {
    "event_id": "one",
    "sensor_id": "bin-1",
    "event_time": "2026-10-01T08:00:00+00:00",
    "fill_percent": "90",
    "temperature_c": "30",
    "latitude": "17.3",
    "longitude": "78.4",
}


class PipelineTests(unittest.TestCase):
    def test_quarantines_out_of_range_fill(self):
        invalid = {**VALID, "fill_percent": "120"}
        accepted, rejected = transform([invalid])
        self.assertEqual(accepted, [])
        self.assertIn("fill_percent must be between 0 and 100", rejected[0]["errors"])

    def test_quarantines_duplicate_event(self):
        accepted, rejected = transform([VALID, VALID])
        self.assertEqual(len(accepted), 1)
        self.assertEqual(rejected[0]["errors"], ["duplicate event_id"])

    def test_minimizes_precise_location_by_default(self):
        accepted, _ = transform([VALID])
        self.assertEqual(accepted[0].latitude, 17.3)
        self.assertEqual(accepted[0].longitude, 78.4)

        precise = {**VALID, "latitude": "17.385012", "longitude": "78.486712"}
        accepted, _ = transform([precise])
        self.assertEqual(accepted[0].latitude, 17.39)
        self.assertEqual(accepted[0].longitude, 78.49)

    def test_blocks_and_does_not_persist_unexpected_sensitive_field(self):
        accepted, rejected = transform([{**VALID, "operator_email": "private@example.com"}])
        self.assertEqual(accepted, [])
        self.assertEqual(rejected[0]["errors"], ["unexpected field blocked: operator_email"])
        self.assertNotIn("operator_email", rejected[0]["row"])

    def test_builds_daily_metrics(self):
        second = {**VALID, "event_id": "two", "fill_percent": "50"}
        events, _ = transform([VALID, second])
        metric = aggregate(events)[0]
        self.assertEqual(metric["average_fill_percent"], 70.0)
        self.assertEqual(metric["collection_alerts"], 1)

    def test_end_to_end_writes_all_layers(self):
        source = Path(__file__).parents[1] / "data" / "sample_sensor_events.csv"
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            summary = run(source, output)
            self.assertEqual(summary, {"raw": 6, "accepted": 4, "rejected": 2, "gold": 3})
            self.assertTrue((output / "gold" / "daily_metrics.csv").exists())


if __name__ == "__main__":
    unittest.main()

