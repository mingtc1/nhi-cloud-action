import json
import unittest
from unittest.mock import Mock

from publish_sync_events import event_from_report


class PublishSyncEventsTests(unittest.TestCase):
    def test_report_uses_only_compact_outcome_fields(self):
        path = Mock()
        path.exists.return_value = True
        path.read_text.return_value = json.dumps({
            "status": "deferred_quota", "total_records": 13759,
            "changed": 120, "reason": "D1 budget guard",
            "usage": {"private_detail": "must not be sent"},
        })
        event = event_from_report("nhi", path, "123-1")
        self.assertEqual(event["status"], "deferred_quota")
        self.assertEqual(event["changed"], 120)
        self.assertNotIn("usage", event)

    def test_missing_report_is_visible_as_failure(self):
        path = Mock()
        path.exists.return_value = False
        event = event_from_report("tfda_ingredients", path, "123-1")
        self.assertEqual(event["status"], "failure")


if __name__ == "__main__":
    unittest.main()
