import pytest

from datetime import date
from lib.pipeline_controller import PipelineController
from tests.test_helpers import TestHelpers


@pytest.mark.freeze_time("2024-01-01 23:00:00-05:00")
class TestPipelineController:

    @pytest.fixture(autouse=True)
    def teardown_class(cls):
        TestHelpers.set_env_vars()
        yield
        TestHelpers.clear_env_vars()

    @pytest.fixture
    def test_instance(self, mocker):
        mocker.patch("lib.pipeline_controller.create_log")
        mocker.patch("lib.pipeline_controller.AvroEncoder")
        mocker.patch("lib.pipeline_controller.KinesisClient")
        mocker.patch("lib.pipeline_controller.S3Client")
        mocker.patch("lib.pipeline_controller.ShopperTrakApiClient")
        return PipelineController()

    def test_run(self, test_instance, mocker):
        mocked_all_sites_method = mocker.patch(
            "lib.pipeline_controller.PipelineController.process_all_sites_data"
        )
        test_instance.s3_client.fetch_cache.return_value = {
            "last_poll_date": "2023-12-29"
        }

        test_instance.run()

        mocked_all_sites_method.assert_called_once_with(date(2023, 12, 31), 0)
        test_instance.s3_client.close.assert_called_once()
        test_instance.kinesis_client.close.assert_called_once()

    def test_process_all_sites_data_single_run(self, test_instance, mocker):
        TEST_RESULTS = [{"key1": 1, "key2": "2"}, {"key1": 3, "key2": "4"}]
        TEST_ENCODED_RECORDS = [b"encoded1", b"encoded2"]
        mock_xml_root = mocker.MagicMock()

        test_instance.s3_client.fetch_cache.side_effect = [
            {"last_poll_date": "2023-12-30"},
            {"last_poll_date": "2023-12-31"},
        ]
        test_instance.shoppertrak_api_client.query.return_value = mock_xml_root
        test_instance.shoppertrak_api_client.parse_response.return_value = TEST_RESULTS
        test_instance.avro_encoder.encode_batch.return_value = TEST_ENCODED_RECORDS

        test_instance.process_all_sites_data(date(2023, 12, 31), 0)

        assert test_instance.s3_client.fetch_cache.call_count == 2
        test_instance.shoppertrak_api_client.query.assert_called_once_with(
            "allsites", "20231231"
        )
        test_instance.shoppertrak_api_client.parse_response.assert_called_once_with(
            mock_xml_root, "20231231"
        )
        test_instance.avro_encoder.encode_batch.assert_called_once_with(TEST_RESULTS)
        test_instance.kinesis_client.send_records.assert_called_once_with(
            TEST_ENCODED_RECORDS
        )
        test_instance.s3_client.set_cache.assert_called_once_with(
            {"last_poll_date": "2023-12-31"}
        )

    def test_process_all_sites_data_multi_run(self, test_instance, mocker):
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
