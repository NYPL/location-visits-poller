import logging
import pytest
import xml.etree.ElementTree as ET

from datetime import date, datetime, time
from helpers.query_helper import REDSHIFT_DROP_QUERY, REDSHIFT_RECOVERABLE_QUERY
from lib.pipeline_controller import PipelineController
from lib.shoppertrak_api_client import APIStatus


_TEST_LOCATION_HOURS_DICT = {("aa", "Sunday"): (time(9), time(17))}
_TEST_KNOWN_DATA_DICT = {
    ("aa", 1, datetime(2023, 12, 1, 9, 0, 0)): (99, True, 10, 11),
    ("aa", 2, datetime(2023, 12, 1, 9, 0, 0)): (98, False, 0, 0),
    ("aa", 1, datetime(2023, 12, 1, 9, 15, 0)): (97, False, 0, 0),
    ("cc", 3, datetime(2023, 12, 1, 9, 30, 0)): (96, True, 200, 201),
}
_TEST_RECOVERABLE_SITE_DATES = (
    ["aa", date(2023, 12, 1)],
    ["bb", date(2023, 12, 1)],
    ["cc", date(2023, 12, 1)],
    ["aa", date(2023, 12, 2)],
    ["bb", date(2023, 12, 3)],
)
_TEST_ENCODED_RECORDS = [b"encoded1", b"encoded2", b"encoded3"]
_TEST_XML_ROOT = ET.fromstring('<?xml version="1.0"?><element></element>')


def _build_test_api_data(increment_date_str, is_all_healthy_data):
    return [
        {
            "shoppertrak_site_id": "aa",
            "orbit": 1,
            "increment_start": increment_date_str + " 09:00:00",
            "enters": 10,
            "exits": 11,
            "is_healthy_data": True,
            "is_missing_data": False,
            "is_fresh": True,
            "poll_date": "2024-01-01",
        },
        {
            "shoppertrak_site_id": "aa",
            "orbit": 2,  # different orbit
            "increment_start": increment_date_str + " 09:00:00",
            "enters": 14,
            "exits": 15,
            "is_healthy_data": True,
            "is_missing_data": False,
            "is_fresh": True,
            "poll_date": "2024-01-01",
        },
        {
            "shoppertrak_site_id": "aa",
            "orbit": 1,
            "increment_start": increment_date_str + " 09:15:00",  # different time
            "enters": 16,
            "exits": 17,
            "is_healthy_data": True,
            "is_missing_data": False,
            "is_fresh": True,
            "poll_date": "2024-01-01",
        },
        {
            "shoppertrak_site_id": "bb",  # different site
            "orbit": 1,
            "increment_start": increment_date_str + " 09:00:00",
            "enters": 12,
            "exits": 13,
            "is_healthy_data": True,
            "is_missing_data": False,
            "is_fresh": True,
            "poll_date": "2024-01-01",
        },
        {
            "shoppertrak_site_id": "cc",
            "orbit": 3,
            "increment_start": increment_date_str + " 09:30:00",
            "enters": 0,
            "exits": 0,
            "is_healthy_data": is_all_healthy_data,
            "is_missing_data": not is_all_healthy_data,
            "is_fresh": True,
            "poll_date": "2024-01-01",
        },
    ]


class TestPipelineController:

    @pytest.fixture
    def test_instance(self, mocker):
        mocker.patch("lib.pipeline_controller.AvroEncoder")
        mocker.patch("lib.pipeline_controller.KinesisClient")
        mocker.patch("lib.pipeline_controller.RedshiftClient")
        mocker.patch(
            "lib.pipeline_controller.S3Client",
            side_effect=[mocker.MagicMock(), mocker.MagicMock()],
        )
        mocker.patch("lib.pipeline_controller.ShopperTrakApiClient")
        test_instance = PipelineController()
        test_instance.all_site_ids = {"aa", "bb", "cc", "dd", "ee"}
        test_instance.shoppertrak_api_client = mocker.MagicMock()
        return test_instance

    @pytest.fixture
    def mock_logger(self, mocker):
        mocker.patch("lib.pipeline_controller.create_log")

    def test_run(self, mock_logger, mocker):
        mocker.patch("lib.pipeline_controller.AvroEncoder")
        mocker.patch("lib.pipeline_controller.KinesisClient")
        mocker.patch("lib.pipeline_controller.RedshiftClient")

        mocked_location_hours_method = mocker.patch(
            "lib.pipeline_controller.PipelineController.get_location_hours_dict",
            return_value=_TEST_LOCATION_HOURS_DICT,
        )
        mocked_all_sites_method = mocker.patch(
            "lib.pipeline_controller.PipelineController.process_all_sites_data"
        )
        mocked_broken_orbits_method = mocker.patch(
            "lib.pipeline_controller.PipelineController.process_broken_orbits"
        )

        mock_all_sites_s3_client = mocker.MagicMock()
        mock_all_sites_s3_client.fetch_cache.return_value = ["aa", "bb"]
        mock_s3_client = mocker.MagicMock()
        mock_s3_client.fetch_cache.return_value = {"last_poll_date": "2023-12-29"}
        mock_s3_constructor = mocker.patch(
            "lib.pipeline_controller.S3Client",
            side_effect=[mock_all_sites_s3_client, mock_s3_client],
        )

        test_instance = PipelineController()
        assert test_instance.shoppertrak_api_client.location_hours_dict == dict()
        assert test_instance.all_site_ids == {"aa", "bb"}
        mock_s3_constructor.assert_has_calls(
            [
                mocker.call("test_all_sites_s3_bucket", "test_all_sites_s3_resource"),
                mocker.call("test_s3_bucket", "test_s3_resource"),
            ]
        )
        mock_all_sites_s3_client.fetch_cache.assert_called_once()
        mock_all_sites_s3_client.close.assert_called_once()

        test_instance.run()

        mocked_location_hours_method.assert_called_once()
        assert (
            test_instance.shoppertrak_api_client.location_hours_dict
            == _TEST_LOCATION_HOURS_DICT
        )
        mocked_all_sites_method.assert_called_once_with(date(2023, 12, 31), 0)
        mock_s3_client.close.assert_called_once()
        mocked_broken_orbits_method.assert_called_once_with(
            date(2023, 12, 2), date(2023, 12, 30)
        )
        test_instance.kinesis_client.close.assert_called_once()

    def test_get_location_hours_dict(self, test_instance, mock_logger, mocker):
        mocked_hours_query = mocker.patch(
            "lib.pipeline_controller.build_redshift_hours_query",
            return_value="HOURS",
        )
        test_instance.redshift_client.execute_query.return_value = [
            k + v for k, v in _TEST_LOCATION_HOURS_DICT.items()
        ]

        assert test_instance.get_location_hours_dict() == _TEST_LOCATION_HOURS_DICT

        test_instance.redshift_client.connect.assert_called_once()
        test_instance.redshift_client.execute_query.assert_called_once_with("HOURS")
        test_instance.redshift_client.close_connection.assert_called_once()
        mocked_hours_query.assert_called_once_with(
            "location_hours_v2_test_redshift_name"
        )

    def test_process_all_sites_data_single_run(
        self, test_instance, mock_logger, mocker
    ):
        TEST_API_DATA = _build_test_api_data("2023-12-31", False)

        test_instance.s3_client.fetch_cache.side_effect = [
            {"last_poll_date": "2023-12-30"},
            {"last_poll_date": "2023-12-31"},
        ]
        test_instance.shoppertrak_api_client.query.return_value = _TEST_XML_ROOT
        test_instance.shoppertrak_api_client.parse_response.return_value = TEST_API_DATA
        test_instance.avro_encoder.encode_batch.return_value = _TEST_ENCODED_RECORDS

        test_instance.process_all_sites_data(date(2023, 12, 31), 0)

        assert test_instance.s3_client.fetch_cache.call_count == 2
        test_instance.shoppertrak_api_client.query.assert_called_once_with(
            "allsites", date(2023, 12, 31)
        )
        test_instance.shoppertrak_api_client.parse_response.assert_called_once_with(
            _TEST_XML_ROOT, date(2023, 12, 31)
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
        test_instance.shoppertrak_api_client.query.return_value = _TEST_XML_ROOT

        test_instance.process_all_sites_data(date(2023, 12, 31), 0)

        assert test_instance.s3_client.fetch_cache.call_count == 4
        test_instance.shoppertrak_api_client.query.assert_has_calls(
            [
                mocker.call("allsites", date(2023, 12, 29)),
                mocker.call("allsites", date(2023, 12, 30)),
                mocker.call("allsites", date(2023, 12, 31)),
            ]
        )
        test_instance.s3_client.set_cache.assert_has_calls(
            [
                mocker.call({"last_poll_date": "2023-12-29"}),
                mocker.call({"last_poll_date": "2023-12-30"}),
                mocker.call({"last_poll_date": "2023-12-31"}),
            ]
        )

    def test_process_all_sites_error(self, test_instance, mock_logger, mocker, caplog):
        test_instance.s3_client.fetch_cache.return_value = {
            "last_poll_date": "2023-12-30"
        }
        test_instance.shoppertrak_api_client.query.return_value = APIStatus.ERROR

        with caplog.at_level(logging.WARNING):
            test_instance.process_all_sites_data(date(2023, 12, 31), 0)

        assert "Failed to retrieve all sites visits data" in caplog.text
        test_instance.s3_client.fetch_cache.assert_called_once()
        test_instance.shoppertrak_api_client.query.assert_called_once_with(
            "allsites", date(2023, 12, 31)
        )
        test_instance.shoppertrak_api_client.parse_response.assert_not_called()
        test_instance.avro_encoder.encode_batch.assert_not_called()
        test_instance.kinesis_client.send_records.assert_not_called()
        test_instance.s3_client.set_cache.assert_not_called()

    def test_process_broken_orbits_no_missing_sites(
        self, test_instance, mock_logger, mocker
    ):
        mocked_closures_query = mocker.patch(
            "lib.pipeline_controller.build_redshift_closures_query",
            return_value="CLOSURES",
        )
        mocked_found_sites_query = mocker.patch(
            "lib.pipeline_controller.build_redshift_found_sites_query",
            return_value="FOUND SITES",
        )
        mocked_create_table_query = mocker.patch(
            "lib.pipeline_controller.build_redshift_create_table_query",
            return_value="CREATE TABLE",
        )
        mocked_build_known_query = mocker.patch(
            "lib.pipeline_controller.build_redshift_known_query",
            return_value="KNOWN",
        )
        mocked_recover_data_method = mocker.patch(
            "lib.pipeline_controller.PipelineController._recover_data"
        )
        test_instance.redshift_client.execute_query.side_effect = [
            [],
            (
                ["aa", date(2023, 12, 1)],
                ["bb", date(2023, 12, 1)],
                ["cc", date(2023, 12, 1)],
                ["dd", date(2023, 12, 1)],
                ["ee", date(2023, 12, 1)],
                ["aa", date(2023, 12, 2)],
                ["bb", date(2023, 12, 2)],
                ["cc", date(2023, 12, 2)],
                ["dd", date(2023, 12, 2)],
                ["ee", date(2023, 12, 2)],
            ),
            _TEST_RECOVERABLE_SITE_DATES,
            [k + v for k, v in _TEST_KNOWN_DATA_DICT.items()],
        ]

        test_instance.process_broken_orbits(date(2023, 12, 1), date(2023, 12, 3))

        test_instance.redshift_client.connect.assert_called_once()
        test_instance.redshift_client.execute_transaction.assert_has_calls(
            [
                mocker.call([("CREATE TABLE", None)]),
                mocker.call([(REDSHIFT_DROP_QUERY, None)]),
            ]
        )
        test_instance.redshift_client.execute_query.assert_has_calls(
            [
                mocker.call("CLOSURES"),
                mocker.call("FOUND SITES"),
                mocker.call(REDSHIFT_RECOVERABLE_QUERY),
                mocker.call("KNOWN"),
            ]
        )
        test_instance.redshift_client.close_connection.assert_called_once()
        mocked_closures_query.assert_called_once_with(
            "location_closures_v2_test_redshift_name", date(2023, 12, 1)
        )
        mocked_found_sites_query.assert_called_once_with(
            "location_visits_test_redshift_name", date(2023, 12, 1), date(2023, 12, 3)
        )
        mocked_create_table_query.assert_called_once_with(
            "location_visits_test_redshift_name", date(2023, 12, 1), date(2023, 12, 3)
        )
        mocked_build_known_query.assert_called_once_with(
            "location_visits_test_redshift_name"
        )
        mocked_recover_data_method.assert_called_once_with(
            [tuple(row) for row in _TEST_RECOVERABLE_SITE_DATES],
            _TEST_KNOWN_DATA_DICT,
        )

    def test_process_broken_orbits_missing_sites(
        self, test_instance, mock_logger, mocker
    ):
        mocked_recover_data_method = mocker.patch(
            "lib.pipeline_controller.PipelineController._recover_data"
        )
        test_instance.redshift_client.execute_query.side_effect = [
            (
                ["cc", date(2023, 12, 1)],
                ["ee", date(2023, 12, 2)],
                [None, date(2023, 12, 3)],
            ),
            (
                ["aa", date(2023, 12, 1)],
                ["bb", date(2023, 12, 1)],
                ["cc", date(2023, 12, 1)],
                ["dd", date(2023, 12, 1)],
                ["aa", date(2023, 12, 2)],
                ["bb", date(2023, 12, 2)],
                ["cc", date(2023, 12, 2)],
                ["bb", date(2023, 12, 3)],
            ),
            _TEST_RECOVERABLE_SITE_DATES,
            [k + v for k, v in _TEST_KNOWN_DATA_DICT.items()],
        ]

        test_instance.process_broken_orbits(date(2023, 12, 1), date(2023, 12, 3))

        test_instance.redshift_client.connect.assert_called_once()
        test_instance.redshift_client.close_connection.assert_called_once()
        mocked_recover_data_method.assert_has_calls(
            [
                mocker.call(
                    [("ee", date(2023, 12, 1)), ("dd", date(2023, 12, 2))],
                    dict(),
                    is_recovery_mode=False,
                ),
                mocker.call(
                    [
                        ("aa", date(2023, 12, 1)),
                        ("bb", date(2023, 12, 1)),
                        ("aa", date(2023, 12, 2)),
                    ],
                    _TEST_KNOWN_DATA_DICT,
                ),
            ]
        )

    def test_recover_data(self, test_instance, mock_logger, mocker):
        TEST_API_DATA = _build_test_api_data("2023-12-01", True)

        test_instance.shoppertrak_api_client.query.return_value = _TEST_XML_ROOT
        test_instance.shoppertrak_api_client.parse_response.side_effect = [
            TEST_API_DATA[:3],
            TEST_API_DATA[3:4],
            TEST_API_DATA[4:],
            [],
        ]
        mocked_process_recovered_data_method = mocker.patch(
            "lib.pipeline_controller.PipelineController._process_recovered_data"
        )

        test_instance._recover_data(
            [
                ("aa", date(2023, 12, 1)),
                ("bb", date(2023, 12, 1)),
                ("cc", date(2023, 12, 1)),
                ("aa", date(2023, 12, 2)),
            ],
            _TEST_KNOWN_DATA_DICT,
        )

        test_instance.shoppertrak_api_client.query.assert_has_calls(
            [
                mocker.call("site/aa", date(2023, 12, 1)),
                mocker.call("site/bb", date(2023, 12, 1)),
                mocker.call("site/cc", date(2023, 12, 1)),
                mocker.call("site/aa", date(2023, 12, 2)),
            ]
        )
        test_instance.shoppertrak_api_client.parse_response.assert_has_calls(
            [
                mocker.call(_TEST_XML_ROOT, date(2023, 12, 1), is_recovery_mode=True),
                mocker.call(_TEST_XML_ROOT, date(2023, 12, 1), is_recovery_mode=True),
                mocker.call(_TEST_XML_ROOT, date(2023, 12, 1), is_recovery_mode=True),
                mocker.call(_TEST_XML_ROOT, date(2023, 12, 2), is_recovery_mode=True),
            ]
        )
        mocked_process_recovered_data_method.assert_has_calls(
            [
                mocker.call(TEST_API_DATA[:3], _TEST_KNOWN_DATA_DICT),
                mocker.call(TEST_API_DATA[3:4], _TEST_KNOWN_DATA_DICT),
                mocker.call(TEST_API_DATA[4:], _TEST_KNOWN_DATA_DICT),
                mocker.call([], _TEST_KNOWN_DATA_DICT),
            ]
        )

    def test_recover_data_with_bad_poll_date(
        self, test_instance, mock_logger, mocker, caplog
    ):
        # mock bad poll date variable
        test_instance.bad_poll_dates = [date(2023, 12, 2)]
        TEST_API_DATA = _build_test_api_data("2023-12-01", True)

        test_instance.shoppertrak_api_client.query.side_effect = [
            _TEST_XML_ROOT,
            _TEST_XML_ROOT,
            _TEST_XML_ROOT,
            APIStatus.ERROR,
        ]
        test_instance.shoppertrak_api_client.parse_response.side_effect = [
            TEST_API_DATA[:3],
            TEST_API_DATA[3:4],
            TEST_API_DATA[4:],
        ]
        mocked_process_recovered_data_method = mocker.patch(
            "lib.pipeline_controller.PipelineController._process_recovered_data"
        )

        with caplog.at_level(logging.INFO):
            test_instance._recover_data(
                [
                    ("aa", date(2023, 12, 1)),
                    ("bb", date(2023, 12, 1)),
                    ("cc", date(2023, 12, 1)),
                    ("aa", date(2023, 12, 2)),
                ],
                _TEST_KNOWN_DATA_DICT,
            )

        # Verify that although ShopperTrak returned APIStatus.Error, we
        # only send INFO messages for known bad poll dates. We don't alert
        assert len(caplog.records) == 1
        assert caplog.records[0].levelname == "INFO"
        assert "Failed to retrieve site visits data for aa" in caplog.text

        test_instance.shoppertrak_api_client.query.assert_has_calls(
            [
                mocker.call("site/aa", date(2023, 12, 1)),
                mocker.call("site/bb", date(2023, 12, 1)),
                mocker.call("site/cc", date(2023, 12, 1)),
                mocker.call("site/aa", date(2023, 12, 2)),
            ]
        )
        test_instance.shoppertrak_api_client.parse_response.assert_has_calls(
            [
                mocker.call(_TEST_XML_ROOT, date(2023, 12, 1), is_recovery_mode=True),
                mocker.call(_TEST_XML_ROOT, date(2023, 12, 1), is_recovery_mode=True),
                mocker.call(_TEST_XML_ROOT, date(2023, 12, 1), is_recovery_mode=True),
            ]
        )
        mocked_process_recovered_data_method.assert_has_calls(
            [
                mocker.call(TEST_API_DATA[:3], _TEST_KNOWN_DATA_DICT),
                mocker.call(TEST_API_DATA[3:4], _TEST_KNOWN_DATA_DICT),
                mocker.call(TEST_API_DATA[4:], _TEST_KNOWN_DATA_DICT),
            ]
        )

    def test_recover_data_error(self, test_instance, mocker, caplog):
        test_instance.shoppertrak_api_client.query.side_effect = [
            _TEST_XML_ROOT,
            APIStatus.ERROR,
            _TEST_XML_ROOT,
        ]
        mocked_process_recovered_data_method = mocker.patch(
            "lib.pipeline_controller.PipelineController._process_recovered_data"
        )

        with caplog.at_level(logging.WARNING):
            test_instance._recover_data(
                [
                    ("aa", date(2023, 12, 1)),
                    ("bb", date(2023, 12, 1)),
                    ("aa", date(2023, 12, 2)),
                ],
                _TEST_KNOWN_DATA_DICT,
            )

        assert "Failed to retrieve site visits data for bb" in caplog.text
        test_instance.shoppertrak_api_client.parse_response.assert_has_calls(
            [
                mocker.call(_TEST_XML_ROOT, date(2023, 12, 1), is_recovery_mode=True),
                mocker.call(_TEST_XML_ROOT, date(2023, 12, 2), is_recovery_mode=True),
            ]
        )
        assert mocked_process_recovered_data_method.call_count == 2

    def test_process_recovered_data(self, test_instance, mocker, caplog):
        mocked_update_query = mocker.patch(
            "lib.pipeline_controller.build_redshift_update_query",
            return_value="UPDATE",
        )
        TEST_API_DATA = _build_test_api_data("2023-12-01", True)
        test_instance.avro_encoder.encode_batch.return_value = _TEST_ENCODED_RECORDS

        with caplog.at_level(logging.WARNING):
            test_instance._process_recovered_data(TEST_API_DATA, _TEST_KNOWN_DATA_DICT)

        assert (
            "Different healthy data found in API and Redshift: ('cc', 3, "
            "FakeDatetime(2023, 12, 1, 9, 30)) mapped to {'shoppertrak_site_id': 'cc', "
            "'orbit': 3, 'increment_start': '2023-12-01 09:30:00', 'enters': 0, "
            "'exits': 0, 'is_healthy_data': True, 'is_missing_data': False, "
            "'is_fresh': True, 'poll_date': '2024-01-01'} in the API and (96, True, "
            "200, 201) in Redshift"
        ) in caplog.text
        assert "aa" not in caplog.text
        assert "bb" not in caplog.text
        mocked_update_query.assert_called_once_with(
            "location_visits_test_redshift_name", "98,97"
        )
        test_instance.redshift_client.execute_transaction.assert_called_once_with(
            [("UPDATE", None)]
        )
        test_instance.avro_encoder.encode_batch.assert_called_once_with(
            TEST_API_DATA[1:4]
        )
        test_instance.kinesis_client.send_records.assert_called_once_with(
            _TEST_ENCODED_RECORDS
        )
