import json
import sys
import unittest
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from spark_streaming import (  # noqa: E402
    create_activity_dataframe,
    create_profile_dataframe,
)


class StreamingValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("SparkStreamingValidationTests")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def make_json_dataframe(self, records):
        return self.spark.createDataFrame(
            [(json.dumps(record) if not isinstance(record, str) else record,)
             for record in records],
            ["value"],
        )

    def test_profile_schema_accepts_valid_uuid_and_rejects_bad_records(self):
        valid_profile = {
            "profile_id": "3b7c1168-d92b-4834-88e3-9b50701fe07b",
            "full_name": "Test User",
            "gender": "female",
            "location": "1 Main Street",
            "city": "Test City",
            "country": "Test Country",
            "postcode": "12345",
            "latitude": 10.5,
            "longitude": 20.25,
            "email": "test@example.com",
        }
        missing_id = dict(valid_profile, profile_id=None)
        invalid_coordinates = dict(valid_profile, latitude=91)
        records = [
            valid_profile,
            missing_id,
            invalid_coordinates,
            "not-json",
            "null",
        ]

        result = (
            create_profile_dataframe(self.make_json_dataframe(records))
            .select("_is_valid")
            .collect()
        )

        self.assertEqual([row["_is_valid"] for row in result], [
            True, False, False, False, False,
        ])

    def test_activity_schema_accepts_valid_event_and_rejects_bad_records(self):
        valid_event = {
            "event_id": "a9b5b4d2-8b10-4f20-9b4c-44a7a5be9a11",
            "user_id": "user_001",
            "event_type": "purchase",
            "event_time": "2026-11-03T10:00:00+00:00",
            "device": "mobile",
            "product_id": "product_001",
        }
        missing_product = dict(valid_event, product_id=None)
        invalid_device = dict(valid_event, device="unknown")
        invalid_timestamp = dict(valid_event, event_time="not-a-timestamp")
        records = [
            valid_event,
            missing_product,
            invalid_device,
            invalid_timestamp,
            "not-json",
            "null",
        ]

        result = (
            create_activity_dataframe(self.make_json_dataframe(records))
            .select("_is_valid", "event_time")
            .collect()
        )

        self.assertEqual([row["_is_valid"] for row in result], [
            True, False, False, False, False, False,
        ])
        self.assertIsNotNone(result[0]["event_time"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
