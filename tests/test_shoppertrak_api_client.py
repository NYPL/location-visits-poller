import logging
import os
import pytest
import xml.etree.ElementTree as ET

from copy import deepcopy
from datetime import date, time
from lib import APIStatus, ShopperTrakApiClient, ShopperTrakApiClientError
from requests.exceptions import ConnectTimeout


_PRETTY_API_RESPONSE = """
<?xml version="1.0" ?>
<sites>
	<site siteID="aa">
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
    <site siteID="bb - test sublocation">
		<date dateValue="20231231">
			<entrance name="EP 1">
				<traffic code="02" exits="99" enters="99" startTime="000000"/>
				<traffic code="02" exits="0" enters="0" startTime="010000"/>
				<traffic code="02" exits="0" enters="0" startTime="020000"/>
				<traffic code="02" exits="0" enters="0" startTime="030000"/>
			</entrance>
		</date>
	</site>
</sites>"""
_TEST_API_RESPONSE = _PRETTY_API_RESPONSE.replace("\n", "")
_TEST_API_RESPONSE = _TEST_API_RESPONSE.replace("\t", "")

_TEST_LOCATION_HOURS_DICT = {
    ("aa", "Sun"): (time(9), time(17)), ("aa", "Mon"): (time(10), time(18)),
    ("bb", "Sun"): (time(1), time(3)), ("bb", "Tue"): (time(11), time(19)),
    ("cc", "Sun"): (None, None), ("cc", "Wed"): (time(12, 30), time(20, 30)), 
}

_PARSED_RESULT = [
    {"shoppertrak_site_id": "aa", "orbit": 1, "increment_start": "2023-12-31 00:00:00",
     "enters": 0, "exits": 0, "is_healthy_data": True, "is_missing_data": False,
     "is_fresh": True, "poll_date": "2024-01-01"},
    {"shoppertrak_site_id": "aa", "orbit": 1, "increment_start": "2023-12-31 01:00:00",
     "enters": 2, "exits": 1, "is_healthy_data": True, "is_missing_data": False,
     "is_fresh": True, "poll_date": "2024-01-01"},
    {"shoppertrak_site_id": "aa", "orbit": 1, "increment_start": "2023-12-31 02:00:00",
     "enters": 4, "exits": 3, "is_healthy_data": True, "is_missing_data": False,
     "is_fresh": True, "poll_date": "2024-01-01"},
    {"shoppertrak_site_id": "aa", "orbit": 1, "increment_start": "2023-12-31 03:00:00",
     "enters": 0, "exits": 0, "is_healthy_data": True, "is_missing_data": False,
     "is_fresh": True, "poll_date": "2024-01-01"},
    {"shoppertrak_site_id": "aa", "orbit": 2, "increment_start": "2023-12-31 00:00:00",
     "enters": 6, "exits": 5, "is_healthy_data": True, "is_missing_data": False,
     "is_fresh": True, "poll_date": "2024-01-01"},
    {"shoppertrak_site_id": "aa", "orbit": 2, "increment_start": "2023-12-31 01:00:00",
     "enters": 0, "exits": 0, "is_healthy_data": True, "is_missing_data": False,
     "is_fresh": True, "poll_date": "2024-01-01"},
    {"shoppertrak_site_id": "aa", "orbit": 2, "increment_start": "2023-12-31 02:00:00",
     "enters": 0, "exits": 0, "is_healthy_data": False, "is_missing_data": False,
     "is_fresh": True, "poll_date": "2024-01-01"},
    {"shoppertrak_site_id": "aa", "orbit": 2, "increment_start": "2023-12-31 03:00:00",
     "enters": 80, "exits": 70, "is_healthy_data": False, "is_missing_data": False,
     "is_fresh": True, "poll_date": "2024-01-01"},
    {"shoppertrak_site_id": "bb - test sublocation", "orbit": 1,
     "increment_start": "2023-12-31 00:00:00", "enters": 99, "exits": 99,
     "is_healthy_data": False, "is_missing_data": False, "is_fresh": True,
     "poll_date": "2024-01-01"},
    {"shoppertrak_site_id": "bb - test sublocation", "orbit": 1,
     "increment_start": "2023-12-31 01:00:00", "enters": 0, "exits": 0,
     "is_healthy_data": False, "is_missing_data": True, "is_fresh": True,
     "poll_date": "2024-01-01"},
    {"shoppertrak_site_id": "bb - test sublocation", "orbit": 1,
     "increment_start": "2023-12-31 02:00:00", "enters": 0, "exits": 0,
     "is_healthy_data": False, "is_missing_data": True, "is_fresh": True,
     "poll_date": "2024-01-01"},
    {"shoppertrak_site_id": "bb - test sublocation", "orbit": 1,
     "increment_start": "2023-12-31 03:00:00", "enters": 0, "exits": 0,
     "is_healthy_data": False, "is_missing_data": False, "is_fresh": True,
     "poll_date": "2024-01-01"},
]


class TestPipelineController:

    @pytest.fixture
    def test_instance(self):
        return ShopperTrakApiClient(
            os.environ["SHOPPERTRAK_USERNAME"],
            os.environ["SHOPPERTRAK_PASSWORD"],
            _TEST_LOCATION_HOURS_DICT,
            [],
        )

    def test_query(self, test_instance, requests_mock, mocker):
        requests_mock.get(
            "https://test_shoppertrak_url/test%20-%20endpoint%3B%20one"
            "?date=20231231&increment=15&total_property=N",
            text=_TEST_API_RESPONSE,
        )

        xml_root = mocker.MagicMock()
        mocked_check_response_method = mocker.patch(
            "lib.ShopperTrakApiClient._check_response",
            return_value=(APIStatus.SUCCESS, xml_root),
        )

        assert test_instance.query("test - endpoint; one", date(2023, 12, 31)) == xml_root
        mocked_check_response_method.assert_called_once_with(_TEST_API_RESPONSE, date(2023, 12, 31))

    def test_query_request_exception(self, test_instance, requests_mock, mocker, caplog):
        requests_mock.get(
            "https://test_shoppertrak_url/test_endpoint", exc=ConnectTimeout
        )
        mocked_check_response_method = mocker.patch(
            "lib.ShopperTrakApiClient._check_response")

        with pytest.raises(ShopperTrakApiClientError):
            test_instance.query("test_endpoint", date(2023, 12, 31))
        
        assert ("Failed to retrieve response from "
                "https://test_shoppertrak_url/test_endpoint") in caplog.text
        mocked_check_response_method.assert_not_called()
    
    def test_query_non_fatal_error(self, test_instance, requests_mock, mocker):
        requests_mock.get(
            "https://test_shoppertrak_url/test_endpoint"
            "?date=20231231&increment=15&total_property=N",
            text="error",
        )
        mocker.patch("lib.ShopperTrakApiClient._check_response",
                     return_value=(APIStatus.ERROR, None))

        assert test_instance.query(
            "test_endpoint", date(2023, 12, 31)) == APIStatus.ERROR

    def test_query_retry_success(self, test_instance, requests_mock, mocker):
        test_date = date(2023, 12, 31)
        mock_sleep = mocker.patch("time.sleep")
        requests_mock.get(
            "https://test_shoppertrak_url/test_endpoint"
            "?date=20231231&increment=15&total_property=N",
            [{"text": "error"}, {"text": "error2"}, {"text": _TEST_API_RESPONSE}],
        )

        xml_root = mocker.MagicMock()
        mocked_check_response_method = mocker.patch(
            "lib.ShopperTrakApiClient._check_response",
            side_effect=[(APIStatus.RETRY, None), (APIStatus.RETRY, None),
                         (APIStatus.SUCCESS, xml_root)],
        )

        assert test_instance.query("test_endpoint", test_date) == xml_root
        mocked_check_response_method.assert_has_calls(
            [
                mocker.call("error", test_date), mocker.call("error2", test_date),
                mocker.call(_TEST_API_RESPONSE, test_date),
            ]
        )
        assert mock_sleep.call_count == 2

    def test_query_retry_fail(self, test_instance, requests_mock, mocker, caplog):
        mock_sleep = mocker.patch("time.sleep")
        requests_mock.get(
            "https://test_shoppertrak_url/test_endpoint"
            "?date=20231231&increment=15&total_property=N",
            text="error",
        )
        mocked_check_response_method = mocker.patch(
            "lib.ShopperTrakApiClient._check_response",
            return_value=(APIStatus.RETRY, None),
        )

        with pytest.raises(ShopperTrakApiClientError):
            test_instance.query("test_endpoint", date(2023, 12, 31))

        assert "Hit retry limit: sent 3 queries with no response" in caplog.text
        assert mocked_check_response_method.call_count == 3
        assert mock_sleep.call_count == 2

    def test_query_bad_status(self, test_instance, requests_mock, mocker, caplog):
        requests_mock.get(
            "https://test_shoppertrak_url/test_endpoint"
            "?date=20231231&increment=15&total_property=N",
            text="",
        )
        mocker.patch("lib.ShopperTrakApiClient._check_response",
                     return_value=(None, None))

        with pytest.raises(ShopperTrakApiClientError):
            test_instance.query("test_endpoint", date(2023, 12, 31))
        
        assert "Unknown API status: None" in caplog.text

    def test_check_response(self, test_instance, mocker):
        status, root = test_instance._check_response(_TEST_API_RESPONSE, mocker.MagicMock())
        assert status == APIStatus.SUCCESS
        assert type(root) == ET.Element

    def test_check_response_unparsable(self, test_instance, caplog):
        with caplog.at_level(logging.ERROR):
            status, root = test_instance._check_response("bad xml", date(2023, 12, 31))
        
        assert "Could not parse XML response bad xml" in caplog.text
        assert status == APIStatus.ERROR
        assert root is None

    def test_check_response_api_limit(self, test_instance, mocker, caplog):
        with pytest.raises(ShopperTrakApiClientError):
            test_instance._check_response(
                '<?xml version="1.0" ?><message><error>E107</error><description>'
                'Customer has exceeded the maximum number of requests allowed in a 24 '
                'hour period.</description></message>',
                mocker.MagicMock())
        assert "API limit exceeded" in caplog.text

    def test_check_response_down(self, test_instance, mocker, caplog):
        with caplog.at_level(logging.WARNING):
            status, root = test_instance._check_response(
                '<?xml version="1.0" ?><message><error>E000</error>'
                '<description>Server is down</description></message>',
                mocker.MagicMock())

        assert caplog.text == ""
        assert status == APIStatus.RETRY
        assert root is None

    def test_check_response_busy(self, test_instance, mocker, caplog):
        with caplog.at_level(logging.WARNING):
            status, root = test_instance._check_response(
                '<?xml version="1.0" ?><message><error>E108</error>'
                '<description>Server is busy</description></message>',
                mocker.MagicMock())

        assert caplog.text == ""
        assert status == APIStatus.RETRY
        assert root is None

    def test_check_response_xml_error(self, test_instance, caplog):
        with caplog.at_level(logging.ERROR):
            status, root = test_instance._check_response(
                '<?xml version="1.0" ?><message><error>E999</error>'
                '<description>Error!</description></message>',
                date(2023, 12, 31))

        assert "Error found in XML response:" in caplog.text
        assert status == APIStatus.ERROR
        assert root is None

    def test_check_response_no_traffic(self, test_instance, caplog):
        with caplog.at_level(logging.ERROR):
            status, root = test_instance._check_response(
                '<?xml version="1.0" ?><sites><site siteID="site1">'
                '<date dateValue="20231231"><entrance name="EP 01">'
                '</entrance></date></site></sites>',
                date(2023, 12, 31))
        
        assert "No traffic found in XML response:" in caplog.text
        assert status == APIStatus.ERROR
        assert root is None
    
    def test_check_response_bad_poller_date(self, test_instance, caplog):
        test_instance.bad_poll_dates = [date(2023, 12, 31)]
        with caplog.at_level(logging.INFO):
            status, root = test_instance._check_response(
                '<?xml version="1.0" ?><sites><site siteID="site1">'
                '<date dateValue="20231231"><entrance name="EP 01">'
                '</entrance></date></site></sites>',
                date(2023, 12, 31))
        
        assert status == APIStatus.ERROR
        assert root is None

        # Verify that although ShopperTrak returned APIStatus.Error, we
        # only send INFO messages for known bad poll dates. We don't alert
        assert len(caplog.records) == 1
        assert caplog.records[0].levelname == "INFO"
        assert "No traffic found in XML response:" in caplog.text

    def test_parse_response(self, test_instance, caplog):
        with caplog.at_level(logging.WARNING):
            assert test_instance.parse_response(
                ET.fromstring(_TEST_API_RESPONSE), date(2023, 12, 31)) == _PARSED_RESULT

        assert caplog.text == ""

    def test_parse_response_recovery_mode(self, test_instance, caplog):
        _TEST_RESULT = _PARSED_RESULT[:6]

        with caplog.at_level(logging.WARNING):
            assert test_instance.parse_response(
                ET.fromstring(_TEST_API_RESPONSE), date(2023, 12, 31), True
            ) == _TEST_RESULT

        assert caplog.text == ""
    
    def test_parse_response_closed_branch(self, test_instance, caplog):
        _TEST_RESULT = deepcopy(_PARSED_RESULT)
        _TEST_RESULT[9]["is_missing_data"] = False
        _TEST_RESULT[10]["is_missing_data"] = False

        _MODIFIED_LOCATION_HOURS_DICT = deepcopy(_TEST_LOCATION_HOURS_DICT)
        _MODIFIED_LOCATION_HOURS_DICT[("bb", "Sun")] = (None, None)
        test_instance.location_hours_dict = _MODIFIED_LOCATION_HOURS_DICT

        with caplog.at_level(logging.WARNING):
            assert test_instance.parse_response(
                ET.fromstring(_TEST_API_RESPONSE), date(2023, 12, 31)) == _TEST_RESULT

        assert caplog.text == ""

    def test_parse_response_bad_date(self, test_instance):
        _MODIFIED_RESPONSE = _TEST_API_RESPONSE.replace("20231231", "20000101")

        with pytest.raises(ShopperTrakApiClientError):
            test_instance.parse_response(ET.fromstring(_MODIFIED_RESPONSE), date(2023, 12, 31))

    def test_parse_response_new_code(self, test_instance, caplog):
        _MODIFIED_RESPONSE = _TEST_API_RESPONSE.replace('code="02"', 'code="03"')

        with caplog.at_level(logging.WARNING):
            assert test_instance.parse_response(
                ET.fromstring(_MODIFIED_RESPONSE), date(2023, 12, 31)) == _PARSED_RESULT

        assert "Unknown code: '03'. Setting is_healthy_data to False" in caplog.text

    def test_parse_response_duplicate_data(self, test_instance, caplog):
        _MODIFIED_RESPONSE = _TEST_API_RESPONSE.replace(
            'enters="4" startTime="020000"', 'enters="4" startTime="010000"'
        )
        _TEST_RESULT = deepcopy(_PARSED_RESULT)
        _TEST_RESULT[2]["increment_start"] = "2023-12-31 01:00:00"

        with caplog.at_level(logging.WARNING):
            assert test_instance.parse_response(
                ET.fromstring(_MODIFIED_RESPONSE), date(2023, 12, 31)) == _TEST_RESULT

        assert (
            "Received multiple results from the API for the same site/date/orbit/"
            "timestamp combination: {'shoppertrak_site_id': 'aa', 'orbit': 1, "
            "'increment_start': '2023-12-31 01:00:00', 'enters': 4, 'exits': 3, "
            "'is_healthy_data': True, 'is_missing_data': False, 'is_fresh': True, "
            "'poll_date': '2024-01-01'}"
        ) in caplog.text
    
    def test_parse_response_unknown_hours(self, test_instance, caplog):
        _TEST_RESULT = deepcopy(_PARSED_RESULT)
        _TEST_RESULT[9]["is_missing_data"] = True
        _TEST_RESULT[10]["is_missing_data"] = True
        _TEST_RESULT[11]["is_missing_data"] = True

        _MODIFIED_LOCATION_HOURS_DICT = deepcopy(_TEST_LOCATION_HOURS_DICT)
        del _MODIFIED_LOCATION_HOURS_DICT[("bb", "Sun")]
        test_instance.location_hours_dict = _MODIFIED_LOCATION_HOURS_DICT

        with caplog.at_level(logging.WARNING):
            assert test_instance.parse_response(
                ET.fromstring(_TEST_API_RESPONSE), date(2023, 12, 31)) == _TEST_RESULT

        assert (
            "Location hours not found for 'bb' on 'Sun'. Setting is_missing_data to "
            "True."
        ) in caplog.text

    def test_parse_response_blank_str(self, test_instance, caplog):
        _MODIFIED_RESPONSE = _TEST_API_RESPONSE.replace('code="02" ', "")

        with caplog.at_level(logging.WARNING):
            assert test_instance.parse_response(
                ET.fromstring(_MODIFIED_RESPONSE), date(2023, 12, 31)) == _PARSED_RESULT

        assert "Found blank 'code'" in caplog.text

    def test_parse_response_bad_int(self, test_instance, caplog):
        _MODIFIED_RESPONSE = _TEST_API_RESPONSE.replace('enters="6"', 'enters="bad"')
        _MODIFIED_RESPONSE = _MODIFIED_RESPONSE.replace('exits="3" ', "")
        _TEST_RESULT = deepcopy(_PARSED_RESULT)
        _TEST_RESULT[4]["enters"] = None
        _TEST_RESULT[2]["exits"] = None

        with caplog.at_level(logging.WARNING):
            assert test_instance.parse_response(
                ET.fromstring(_MODIFIED_RESPONSE), date(2023, 12, 31)) == _TEST_RESULT

        assert "Input string 'bad' cannot be cast to an int" in caplog.text
        assert "Found blank 'exits'" in caplog.text
