import logging
import os
import pytest
import xml.etree.ElementTree as ET

from copy import deepcopy
from lib import ShopperTrakApiClient, ShopperTrakApiClientError
from requests.exceptions import ConnectTimeout
from tests.test_helpers import TestHelpers

_PRETTY_API_RESPONSE = """
<?xml version="1.0" ?>
<sites>
	<site siteID="site1">
		<date dateValue="20231231">
			<entrance name="EP 01">
				<traffic code="01" exits="0" enters="0" startTime="000000"/>
				<traffic code="01" exits="1" enters="2" startTime="010000"/>
				<traffic code="01" exits="3" enters="4" startTime="020000"/>
				<traffic code="01" exits="0" enters="0" startTime="030000"/>
			</entrance>
			<entrance name=" EP02 ">
				<traffic code="01" exits="5" enters="6" startTime="000000"/>
				<traffic code="01" exits="0" enters="0" startTime="010000"/>
				<traffic code="02" exits="0" enters="0" startTime="020000"/>
				<traffic code="02" exits="70" enters="80" startTime="030000"/>
			</entrance>
		</date>
	</site>
    <site siteID="site2">
		<date dateValue="20231231">
			<entrance name="EP 1">
				<traffic code="02" exits="0" enters="0" startTime="000000"/>
				<traffic code="02" exits="0" enters="0" startTime="010000"/>
				<traffic code="02" exits="0" enters="0" startTime="020000"/>
				<traffic code="02" exits="0" enters="0" startTime="030000"/>
			</entrance>
		</date>
	</site>
</sites>"""
_TEST_API_RESPONSE = _PRETTY_API_RESPONSE.replace("\n", "")
_TEST_API_RESPONSE = _TEST_API_RESPONSE.replace("\t", "")

_PARSED_RESULT = [
    {"shoppertrak_site_id": "site1", "orbit": 1,
     "increment_start": "2023-12-31T00:00:00", "enters": 0, "exits": 0,
     "is_healthy_orbit": True, "is_recovery_data": False, "poll_date": "2024-01-01"},
    {"shoppertrak_site_id": "site1", "orbit": 1,
     "increment_start": "2023-12-31T01:00:00", "enters": 2, "exits": 1,
     "is_healthy_orbit": True, "is_recovery_data": False, "poll_date": "2024-01-01"},
    {"shoppertrak_site_id": "site1", "orbit": 1,
     "increment_start": "2023-12-31T02:00:00", "enters": 4, "exits": 3,
     "is_healthy_orbit": True, "is_recovery_data": False, "poll_date": "2024-01-01"},
    {"shoppertrak_site_id": "site1", "orbit": 1,
     "increment_start": "2023-12-31T03:00:00", "enters": 0, "exits": 0,
     "is_healthy_orbit": True, "is_recovery_data": False, "poll_date": "2024-01-01"},
    {"shoppertrak_site_id": "site1", "orbit": 2,
     "increment_start": "2023-12-31T00:00:00", "enters": 6, "exits": 5,
     "is_healthy_orbit": True, "is_recovery_data": False, "poll_date": "2024-01-01"},
    {"shoppertrak_site_id": "site1", "orbit": 2,
     "increment_start": "2023-12-31T01:00:00", "enters": 0, "exits": 0,
     "is_healthy_orbit": True, "is_recovery_data": False, "poll_date": "2024-01-01"},
    {"shoppertrak_site_id": "site1", "orbit": 2,
     "increment_start": "2023-12-31T02:00:00", "enters": 0, "exits": 0,
     "is_healthy_orbit": False, "is_recovery_data": False, "poll_date": "2024-01-01"},
    {"shoppertrak_site_id": "site1", "orbit": 2,
     "increment_start": "2023-12-31T03:00:00", "enters": 80, "exits": 70,
     "is_healthy_orbit": False, "is_recovery_data": False, "poll_date": "2024-01-01"},
    {"shoppertrak_site_id": "site2", "orbit": 1,
     "increment_start": "2023-12-31T00:00:00", "enters": 0, "exits": 0,
     "is_healthy_orbit": False, "is_recovery_data": False, "poll_date": "2024-01-01"},
    {"shoppertrak_site_id": "site2", "orbit": 1,
     "increment_start": "2023-12-31T01:00:00", "enters": 0, "exits": 0,
     "is_healthy_orbit": False, "is_recovery_data": False, "poll_date": "2024-01-01"},
    {"shoppertrak_site_id": "site2", "orbit": 1,
     "increment_start": "2023-12-31T02:00:00", "enters": 0, "exits": 0,
     "is_healthy_orbit": False, "is_recovery_data": False, "poll_date": "2024-01-01"},
    {"shoppertrak_site_id": "site2", "orbit": 1,
     "increment_start": "2023-12-31T03:00:00", "enters": 0, "exits": 0,
     "is_healthy_orbit": False, "is_recovery_data": False, "poll_date": "2024-01-01"},
]


@pytest.mark.freeze_time("2024-01-01 23:00:00-05:00")
class TestPipelineController:

    @pytest.fixture(autouse=True)
    def teardown_class(cls):
        TestHelpers.set_env_vars()
        yield
        TestHelpers.clear_env_vars()

    @pytest.fixture
    def test_instance(self):
        return ShopperTrakApiClient(
            os.environ["SHOPPERTRAK_USERNAME"], os.environ["SHOPPERTRAK_PASSWORD"]
        )

    def test_query(self, test_instance, requests_mock, mocker):
        requests_mock.get(
            "https://test_shoppertrak_url/test_endpoint"
            "?date=20231231&increment=15&total_property=N",
            text=_TEST_API_RESPONSE,
        )

        xml_root = mocker.MagicMock()
        mocked_check_response_method = mocker.patch(
            "lib.ShopperTrakApiClient._check_response", return_value=xml_root
        )

        assert test_instance.query("test_endpoint", "20231231") == xml_root
        mocked_check_response_method.assert_called_once_with(_TEST_API_RESPONSE)

    def test_query_request_exception(self, test_instance, requests_mock):
        requests_mock.get(
            "https://test_shoppertrak_url/test_endpoint", exc=ConnectTimeout
        )

        with pytest.raises(ShopperTrakApiClientError):
            test_instance.query("test_endpoint", "20231231")

    def test_query_retry_success(self, test_instance, requests_mock, mocker):
        mock_sleep = mocker.patch("time.sleep")
        requests_mock.get(
            "https://test_shoppertrak_url/test_endpoint"
            "?date=20231231&increment=15&total_property=N",
            [{"text": "error"}, {"text": "error2"}, {"text": _TEST_API_RESPONSE}],
        )

        xml_root = mocker.MagicMock()
        mocked_check_response_method = mocker.patch(
            "lib.ShopperTrakApiClient._check_response",
            side_effect=[None, None, xml_root],
        )

        assert test_instance.query("test_endpoint", "20231231") == xml_root
        mocked_check_response_method.assert_has_calls(
            [
                mocker.call("error"), mocker.call("error2"),
                mocker.call(_TEST_API_RESPONSE),
            ]
        )
        assert mock_sleep.call_count == 2

    def test_query_retry_fail(self, test_instance, requests_mock, mocker):
        mock_sleep = mocker.patch("time.sleep")
        requests_mock.get(
            "https://test_shoppertrak_url/test_endpoint"
            "?date=20231231&increment=15&total_property=N",
            text="error",
        )
        mocked_check_response_method = mocker.patch(
            "lib.ShopperTrakApiClient._check_response",
            side_effect=[None, None, None],
        )

        with pytest.raises(ShopperTrakApiClientError):
            test_instance.query("test_endpoint", "20231231")

        assert mocked_check_response_method.call_count == 3
        assert mock_sleep.call_count == 2

    def test_check_response(self, test_instance):
        assert test_instance._check_response(_TEST_API_RESPONSE) is not None

    def test_check_response_busy(self, test_instance):
        assert test_instance._check_response(
            '<?xml version="1.0" ?><message><error>E108</error>'
            '<description>Server is busy</description></message>'
        ) is None

    def test_check_response_unparsable(self, test_instance):
        with pytest.raises(ShopperTrakApiClientError):
            test_instance._check_response("bad xml")

    def test_check_response_xml_error(self, test_instance):
        with pytest.raises(ShopperTrakApiClientError):
            test_instance._check_response(
                '<?xml version="1.0" ?><message><error>E999</error>'
                '<description>Error!</description></message>')

    def test_check_response_no_traffic(self, test_instance):
        with pytest.raises(ShopperTrakApiClientError):
            test_instance._check_response(
                '<?xml version="1.0" ?><sites><site siteID="site1">'
                '<date dateValue="20231231"><entrance name="EP 01">'
                '</entrance></date></site></sites>')

    def test_parse_response(self, test_instance, caplog):
        with caplog.at_level(logging.WARNING):
            assert test_instance.parse_response(
                ET.fromstring(_TEST_API_RESPONSE), "20231231") == _PARSED_RESULT

        assert caplog.text == ""

    def test_parse_response_recovery_mode(self, test_instance, caplog):
        _TEST_RESULT = deepcopy(_PARSED_RESULT)[:6]
        for row in _TEST_RESULT:
            row["is_recovery_data"] = True

        with caplog.at_level(logging.WARNING):
            assert (
                test_instance.parse_response(
                    ET.fromstring(_TEST_API_RESPONSE), "20231231", True
                )
                == _TEST_RESULT
            )

        assert caplog.text == ""

    def test_parse_response_bad_date(self, test_instance):
        _MODIFIED_RESPONSE = _TEST_API_RESPONSE.replace("20231231", "20000101")

        with pytest.raises(ShopperTrakApiClientError):
            test_instance.parse_response(ET.fromstring(_MODIFIED_RESPONSE), "20231231")

    def test_parse_response_new_code(self, test_instance, caplog):
        _MODIFIED_RESPONSE = _TEST_API_RESPONSE.replace('code="02"', 'code="03"')

        with caplog.at_level(logging.WARNING):
            assert (
                test_instance.parse_response(
                    ET.fromstring(_MODIFIED_RESPONSE), "20231231"
                )
                == _PARSED_RESULT
            )

        assert "Unknown code: '03'. Setting is_real to False" in caplog.text

    def test_parse_response_blank_str(self, test_instance, caplog):
        _MODIFIED_RESPONSE = _TEST_API_RESPONSE.replace('code="02" ', "")

        with caplog.at_level(logging.WARNING):
            assert (
                test_instance.parse_response(
                    ET.fromstring(_MODIFIED_RESPONSE), "20231231"
                )
                == _PARSED_RESULT
            )

        assert "Found blank 'code'" in caplog.text

    def test_parse_response_bad_int(self, test_instance, caplog):
        _MODIFIED_RESPONSE = _TEST_API_RESPONSE.replace('enters="6"', 'enters="bad"')
        _MODIFIED_RESPONSE = _MODIFIED_RESPONSE.replace('exits="3" ', "")
        _TEST_RESULT = deepcopy(_PARSED_RESULT)
        _TEST_RESULT[4]["enters"] = None
        _TEST_RESULT[2]["exits"] = None

        with caplog.at_level(logging.WARNING):
            assert (
                test_instance.parse_response(
                    ET.fromstring(_MODIFIED_RESPONSE), "20231231"
                )
                == _TEST_RESULT
            )

        assert "Input string 'bad' cannot be cast to an int" in caplog.text
        assert "Found blank 'exits'" in caplog.text
