import os
import pytz
import requests
import time
import xml.etree.ElementTree as ET

from datetime import datetime
from enum import Enum
from helpers.util import log_based_on_poll_date
from nypl_py_utils.functions.log_helper import create_log
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException
from urllib.parse import quote

ALL_SITES_ENDPOINT = "allsites"
SINGLE_SITE_ENDPOINT = "site/"
_WEEKDAY_MAP = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}


class APIStatus(Enum):
    SUCCESS = 1  # The API successfully retrieved the data
    RETRY = 2  # The API is busy or down and should be retried later

    # The API returned a request-specific error. This status indicates that while this
    # request failed, others with different parameters may still succeed.
    ERROR = 3


class ShopperTrakApiClient:
    """Class for querying the ShopperTrak API for location visits data"""

    def __init__(self, username, password, location_hours_dict, bad_poll_dates):
        self.logger = create_log("shoppertrak_api_client")
        self.base_url = os.environ["SHOPPERTRAK_API_BASE_URL"]
        self.auth = HTTPBasicAuth(username, password)
        self.max_retries = int(os.environ["MAX_RETRIES"])
        self.today_str = datetime.now(pytz.timezone("US/Eastern")).date().isoformat()
        self.location_hours_dict = location_hours_dict
        self.bad_poll_dates = bad_poll_dates

    def query(self, endpoint, query_date, query_count=1):
        """
        Sends query to ShopperTrak API and either a) returns the result as an XML root
        if the query was successful, b) returns APIStatus.ERROR if the query failed but
        others should be attempted, or c) waits and tries again if the API was busy.
        """
        full_url = self.base_url + quote(endpoint)
        date_str = query_date.strftime("%Y%m%d")

        self.logger.info(f"Querying {endpoint} for {date_str} data")
        try:
            response = requests.get(
                full_url,
                params={"date": date_str, "increment": "15", "total_property": "N"},
                auth=self.auth,
            )
            response.raise_for_status()
        except RequestException as e:
            message = f"Failed to retrieve response from {full_url}: {e}"
            self.logger.error(message)
            raise ShopperTrakApiClientError(message) from None

        response_status, response_root = self._check_response(response.text, query_date)
        if response_status == APIStatus.SUCCESS:
            return response_root
        elif response_status == APIStatus.ERROR:
            return response_status
        elif response_status == APIStatus.RETRY:
            if query_count < self.max_retries:
                self.logger.info("Waiting 5 minutes and trying again")
                time.sleep(300)
                return self.query(endpoint, query_date, query_count + 1)
            else:
                self.logger.error(
                    f"Hit retry limit: sent {self.max_retries} queries with no response"
                )
                raise ShopperTrakApiClientError(
                    f"Hit retry limit: sent {self.max_retries} queries with no response"
                )
        else:
            self.logger.error(f"Unknown API status: {response_status}")
            raise ShopperTrakApiClientError(f"Unknown API status: {response_status}")

    def parse_response(self, xml_root, input_date, is_recovery_mode=False):
        """
        Takes API response as an XML root and returns a list of dictionaries containing
        result records. The XML root is expected to look as follows:

        <sites>
            <site siteID="lib a">
                <date dateValue="20240101">
                    <entrance name="EP 01">
                        <traffic code="01" exits="10" enters="11" startTime="000000"/>
                        <traffic code="02" exits="0" enters="0" startTime="001500"/>
                        ...
                    </entrance>
                    <entrance name="EP2">
                        <traffic .../>
                        ...
                    </entrance>
                </date>
            </site>
            <site siteID="lib b">
                ...
            </site>
        </sites>
        """
        rows = []
        for site_xml in xml_root.findall("site"):
            site_val = self._get_xml_str(site_xml, "siteID")
            for date_xml in site_xml.findall("date"):
                date_val = datetime.strptime(
                    self._get_xml_str(date_xml, "dateValue"), "%Y%m%d"
                ).date()
                weekday = _WEEKDAY_MAP[date_val.weekday()]
                if date_val != input_date:
                    self.logger.error(
                        f"Request date does not match response date.\nRequest date: "
                        f"{input_date}\nResponse date: {date_val}"
                    )
                    raise ShopperTrakApiClientError(
                        f"Request date does not match response date.\nRequest date: "
                        f"{input_date}\nResponse date: {date_val}"
                    )
                for entrance_xml in date_xml.findall("entrance"):
                    seen_timestamps = set()
                    entrance_val = self._get_xml_str(entrance_xml, "name")
                    if entrance_val:
                        entrance_val = self._cast_str_to_int(entrance_val.lstrip("EP"))
                    for traffic_xml in entrance_xml.findall("traffic"):
                        result_row = self._form_row(
                            traffic_xml,
                            site_val,
                            date_val,
                            entrance_val,
                            weekday,
                            is_recovery_mode,
                        )
                        if result_row["increment_start"] in seen_timestamps:
                            self.logger.warning(
                                f"Received multiple results from the API for the same "
                                f"site/date/orbit/timestamp combination: {result_row}"
                            )
                        if result_row["is_healthy_data"] or not is_recovery_mode:
                            rows.append(result_row)
                        seen_timestamps.add(result_row["increment_start"])
        return rows

    def _form_row(
        self, traffic_xml, site_val, date_val, entrance_val, weekday, is_recovery_mode
    ):
        """Forms one result row out of various XML elements and values"""
        start_time_val = datetime.strptime(
            self._get_xml_str(traffic_xml, "startTime"), "%H%M%S"
        ).time()
        start_dt_str = datetime.combine(date_val, start_time_val).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        enters = self._cast_str_to_int(self._get_xml_str(traffic_xml, "enters"))
        exits = self._cast_str_to_int(self._get_xml_str(traffic_xml, "exits"))

        status_code = self._get_xml_str(traffic_xml, "code")
        is_healthy_data = status_code == "01"
        if status_code != "01" and status_code != "02":
            self.logger.warning(
                f"Unknown code: '{status_code}'. Setting is_healthy_data to False."
            )

        # Determine if unhealthy data with 0 entrances/exits is missing or imputed by
        # checking if it occurs during a library's regular hours. This 1) assumes that
        # imputed data during a library's regular hours will always have at least one
        # entrance/exit, and 2) ignores irregular closures. However, these are
        # acceptable assumptions, as it's preferable to erroneously mark imputed
        # data as missing than to do the reverse.
        if is_healthy_data or is_recovery_mode or enters > 0 or exits > 0:
            is_missing_data = False
        else:
            branch_code = site_val.split(" ")[0]
            if (branch_code, weekday) in self.location_hours_dict:
                location_hours = self.location_hours_dict[(branch_code, weekday)]
                if location_hours[0] is None and location_hours[1] is None:
                    is_missing_data = False
                else:
                    is_missing_data = (
                        start_time_val >= location_hours[0]
                        and start_time_val < location_hours[1]
                    )
            else:
                self.logger.warning(
                    f"Location hours not found for '{branch_code}' on '{weekday}'. "
                    f"Setting is_missing_data to True."
                )
                is_missing_data = True

        return {
            "shoppertrak_site_id": site_val,
            "orbit": entrance_val,
            "increment_start": start_dt_str,
            "enters": enters,
            "exits": exits,
            "is_healthy_data": is_healthy_data,
            "is_missing_data": is_missing_data,
            "is_fresh": True,
            "poll_date": self.today_str,
        }

    def _check_response(self, response_text, query_date):
        """
        Checks response for errors. If none are found, returns the XML root. Otherwise,
        either throws an error or returns an APIStatus where appropriate.
        """
        is_bad_poll_date = bool(query_date in self.bad_poll_dates)

        try:
            root = ET.fromstring(response_text)
            error = root.find("error")
        except ET.ParseError as e:
            log_based_on_poll_date(
                self.logger,
                f"Could not parse XML response {response_text}: {e}",
                is_bad_poll_date,
            )
            return APIStatus.ERROR, None

        if error is not None and error.text is not None:
            # E107 is used when the daily API limit has been exceeded
            if error.text == "E107":
                self.logger.error("API limit exceeded")
                raise ShopperTrakApiClientError(f"API limit exceeded")
            # E000 is used when ShopperTrak is down and E108 is used when it's busy
            elif error.text == "E000" or error.text == "E108":
                self.logger.info("ShopperTrak is unavailable")
                return APIStatus.RETRY, None
            else:
                log_based_on_poll_date(
                    self.logger,
                    f"Error found in XML response: {response_text}",
                    is_bad_poll_date,
                )
                return APIStatus.ERROR, None
        elif len(root.findall(".//traffic")) == 0:
            log_based_on_poll_date(
                self.logger,
                f"No traffic found in XML response: {response_text}",
                is_bad_poll_date,
            )
            return APIStatus.ERROR, None
        else:
            return APIStatus.SUCCESS, root

    def _get_xml_str(self, xml, attribute):
        """
        Returns XML attribute as string and logs a warning if the attribute does not
        exist or is empty
        """
        attribute_str = (xml.get(attribute) or "").strip()
        if not attribute_str:
            self.logger.warning(f"Found blank '{attribute}'")
            return None
        else:
            return attribute_str

    def _cast_str_to_int(self, input_str):
        """Casts string to int and logs a warning if the string cannot be cast"""
        if not input_str:
            return None

        try:
            return int(input_str)
        except ValueError:
            self.logger.warning(f"Input string '{input_str}' cannot be cast to an int")
            return None


class ShopperTrakApiClientError(Exception):
    def __init__(self, message=None):
        self.message = message
