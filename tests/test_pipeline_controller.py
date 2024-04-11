import logging
import pytest

from datetime import date, datetime
from helpers.query_helper import REDSHIFT_DROP_QUERY, REDSHIFT_RECOVERABLE_QUERY
from lib.pipeline_controller import PipelineController
from tests.test_helpers import TestHelpers


_TEST_KNOWN_DATA_DICT = {
    ("lib a", 1, datetime(2023, 12, 1, 9, 0, 0)): (10, 11),
    ("lib c", 3, datetime(2023, 12, 1, 9, 30, 0)): (200, 201),
}
_TEST_RECOVERABLE_SITE_DATES = [
    ("lib a", date(2023, 12, 1)),
    ("lib b", date(2023, 12, 1)),
    ("lib c", date(2023, 12, 1)),
    ("lib a", date(2023, 12, 2)),
]
_TEST_ENCODED_RECORDS = [b"encoded1", b"encoded2", b"encoded3"]


def _build_test_api_data(increment_date_str, is_recovery_data):
    return [
        {
            "shoppertrak_site_id": "lib a",
            "orbit": 1,
            "increment_start": increment_date_str + " 09:00:00",
            "enters": 10,
            "exits": 11,
            "is_healthy_orbit": True,
            "is_recovery_data": is_recovery_data,
            "poll_date": "2024-01-01",
        },
        {
            "shoppertrak_site_id": "lib a",
            "orbit": 2,  # different orbit
            "increment_start": increment_date_str + " 09:00:00",
            "enters": 14,
            "exits": 15,
            "is_healthy_orbit": True,
            "is_recovery_data": is_recovery_data,
            "poll_date": "2024-01-01",
        },
        {
            "shoppertrak_site_id": "lib a",
            "orbit": 1,
            "increment_start": increment_date_str + " 09:15:00",  # different time
            "enters": 16,
            "exits": 17,
            "is_healthy_orbit": True,
            "is_recovery_data": is_recovery_data,
            "poll_date": "2024-01-01",
        },
        {
            "shoppertrak_site_id": "lib b",  # different site
            "orbit": 1,
            "increment_start": increment_date_str + " 09:00:00",
            "enters": 12,
            "exits": 13,
            "is_healthy_orbit": True,
            "is_recovery_data": is_recovery_data,
            "poll_date": "2024-01-01",
        },
        {
            "shoppertrak_site_id": "lib c",
            "orbit": 3,
            "increment_start": increment_date_str + " 09:30:00",
            "enters": 20,
            "exits": 21,
            "is_healthy_orbit": is_recovery_data,  # unhealthy if not recovery
            "is_recovery_data": is_recovery_data,
            "poll_date": "2024-01-01",
        },
    ]


@pytest.mark.freeze_time("2024-01-01 23:00:00-05:00")
class TestPipelineController:

    @pytest.fixture(autouse=True)
    def teardown_class(cls):
        TestHelpers.set_env_vars()
        yield
        TestHelpers.clear_env_vars()

    @pytest.fixture
    def test_instance(self, mocker):
        mocker.patch("lib.pipeline_controller.AvroEncoder")
        mocker.patch("lib.pipeline_controller.KinesisClient")
        mocker.patch("lib.pipeline_controller.RedshiftClient")
        mocker.patch("lib.pipeline_controller.S3Client")
        mocker.patch("lib.pipeline_controller.ShopperTrakApiClient")
        return PipelineController()

    @pytest.fixture
    def mock_logger(self, mocker):
        mocker.patch("lib.pipeline_controller.create_log")

    def test_run(self, test_instance, mock_logger, mocker):
        mocked_all_sites_method = mocker.patch(
            "lib.pipeline_controller.PipelineController.process_all_sites_data"
        )
        mocked_broken_orbits_method = mocker.patch(
            "lib.pipeline_controller.PipelineController.process_broken_orbits"
        )
        test_instance.s3_client.fetch_cache.return_value = {
            "last_poll_date": "2023-12-29"
        }

        test_instance.run()

        mocked_all_sites_method.assert_called_once_with(date(2023, 12, 31), 0)
        test_instance.s3_client.close.assert_called_once()
        mocked_broken_orbits_method.assert_called_once_with(
            date(2023, 12, 2), date(2023, 12, 30)
        )
        test_instance.kinesis_client.close.assert_called_once()

    def test_process_all_sites_data_single_run(
        self, test_instance, mock_logger, mocker
    ):
        TEST_API_DATA = _build_test_api_data("2023-12-31", False)
        mock_xml_root = mocker.MagicMock()

        test_instance.s3_client.fetch_cache.side_effect = [
            {"last_poll_date": "2023-12-30"},
            {"last_poll_date": "2023-12-31"},
        ]
        test_instance.shoppertrak_api_client.query.return_value = mock_xml_root
        test_instance.shoppertrak_api_client.parse_response.return_value = TEST_API_DATA
        test_instance.avro_encoder.encode_batch.return_value = _TEST_ENCODED_RECORDS

        test_instance.process_all_sites_data(date(2023, 12, 31), 0)

        assert test_instance.s3_client.fetch_cache.call_count == 2
        test_instance.shoppertrak_api_client.query.assert_called_once_with(
            "allsites", "20231231"
        )
        test_instance.shoppertrak_api_client.parse_response.assert_called_once_with(
            mock_xml_root, "20231231"
        )
        test_instance.avro_encoder.encode_batch.assert_called_once_with(TEST_API_DATA)
        test_instance.kinesis_client.send_records.assert_called_once_with(
            _TEST_ENCODED_RECORDS
        )
        test_instance.s3_client.set_cache.assert_called_once_with(
            {"last_poll_date": "2023-12-31"}
        )

    def test_process_all_sites_data_multi_run(self, test_instance, mock_logger, mocker):
        test_instance.s3_client.fetch_cache.side_effect = [
            {"last_poll_date": "2023-12-28"},
            {"last_poll_date": "2023-12-29"},
            {"last_poll_date": "2023-12-30"},
            {"last_poll_date": "2023-12-31"},
        ]

        test_instance.process_all_sites_data(date(2023, 12, 31), 0)

        assert test_instance.s3_client.fetch_cache.call_count == 4
        test_instance.shoppertrak_api_client.query.assert_has_calls(
            [
                mocker.call("allsites", "20231229"),
                mocker.call("allsites", "20231230"),
                mocker.call("allsites", "20231231"),
            ]
        )
        test_instance.s3_client.set_cache.assert_has_calls(
            [
                mocker.call({"last_poll_date": "2023-12-29"}),
                mocker.call({"last_poll_date": "2023-12-30"}),
                mocker.call({"last_poll_date": "2023-12-31"}),
            ]
        )

    def test_process_broken_orbits(self, test_instance, mock_logger, mocker):
        mocked_create_table_query = mocker.patch(
            "lib.pipeline_controller.build_redshift_create_table_query",
            return_value="CREATE TABLE",
        )
        mocked_build_known_query = mocker.patch(
            "lib.pipeline_controller.build_redshift_known_query",
            return_value="KNOWN TABLE",
        )
        mocked_recover_data_method = mocker.patch(
            "lib.pipeline_controller.PipelineController._recover_data"
        )
        test_instance.redshift_client.execute_query.side_effect = [
            _TEST_RECOVERABLE_SITE_DATES,
            [k + v for k, v in _TEST_KNOWN_DATA_DICT.items()],
        ]

        test_instance.process_broken_orbits(date(2023, 12, 1), date(2023, 12, 30))

        test_instance.redshift_client.connect.assert_called_once()
        test_instance.redshift_client.execute_transaction.assert_has_calls(
            [
                mocker.call([("CREATE TABLE", None)]),
                mocker.call([(REDSHIFT_DROP_QUERY, None)]),
            ]
        )
        test_instance.redshift_client.execute_query.assert_has_calls(
            [mocker.call(REDSHIFT_RECOVERABLE_QUERY), mocker.call("KNOWN TABLE")]
        )
        test_instance.redshift_client.close_connection.assert_called_once()
        mocked_create_table_query.assert_called_once_with(
            "location_visits_test_redshift_name", date(2023, 12, 1), date(2023, 12, 30)
        )
        mocked_build_known_query.assert_called_once_with(
            "location_visits_test_redshift_name"
        )
        mocked_recover_data_method.assert_called_once_with(
            _TEST_RECOVERABLE_SITE_DATES,
            _TEST_KNOWN_DATA_DICT,
        )

    def test_recover_data(self, test_instance, mock_logger, mocker):
        TEST_API_DATA = _build_test_api_data("2023-12-01", True)
        test_instance.shoppertrak_api_client.query.return_value = True
        test_instance.shoppertrak_api_client.parse_response.side_effect = [
            TEST_API_DATA[:3],
            TEST_API_DATA[3:4],
            TEST_API_DATA[4:],
            [],
        ]
        mocked_check_recovered_data_method = mocker.patch(
            "lib.pipeline_controller.PipelineController._check_recovered_data"
        )

        test_instance._recover_data(
            [
                ("lib a", date(2023, 12, 1)),
                ("lib b", date(2023, 12, 1)),
                ("lib c", date(2023, 12, 1)),
                ("lib a", date(2023, 12, 2)),
            ],
            _TEST_KNOWN_DATA_DICT,
        )

        test_instance.shoppertrak_api_client.query.assert_has_calls(
            [
                mocker.call("site/lib a", "20231201"),
                mocker.call("site/lib b", "20231201"),
                mocker.call("site/lib c", "20231201"),
                mocker.call("site/lib a", "20231202"),
            ]
        )
        assert test_instance.shoppertrak_api_client.parse_response.call_count == 4
        mocked_check_recovered_data_method.assert_has_calls(
            [
                mocker.call(TEST_API_DATA[:3], _TEST_KNOWN_DATA_DICT),
                mocker.call(TEST_API_DATA[3:4], _TEST_KNOWN_DATA_DICT),
                mocker.call(TEST_API_DATA[4:], _TEST_KNOWN_DATA_DICT),
                mocker.call([], _TEST_KNOWN_DATA_DICT),
            ]
        )

    def test_check_recovered_data(self, test_instance, caplog):
        TEST_API_DATA = _build_test_api_data("2023-12-01", True)
        test_instance.avro_encoder.encode_batch.return_value = _TEST_ENCODED_RECORDS

        with caplog.at_level(logging.WARNING):
            test_instance._check_recovered_data(TEST_API_DATA, _TEST_KNOWN_DATA_DICT)

        assert (
            "Different healthy data found in API and Redshift: "
            "('lib c', 3, FakeDatetime(2023, 12, 1, 9, 30)) mapped to (20, 21) in the "
            "API and (200, 201) in Redshift"
        ) in caplog.text
        assert "lib a" not in caplog.text
        assert "lib b" not in caplog.text
        test_instance.avro_encoder.encode_batch.assert_called_once_with(
            TEST_API_DATA[1:4]
        )
        test_instance.kinesis_client.send_records.assert_called_once_with(
            _TEST_ENCODED_RECORDS
        )
