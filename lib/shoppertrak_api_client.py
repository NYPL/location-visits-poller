import os
import pytz
import requests
import time
import xml.etree.ElementTree as ET

from datetime import datetime
from nypl_py_utils.functions.log_helper import create_log
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException

ALL_SITES_ENDPOINT = "allsites"
SINGLE_SITE_ENDPOINT = "site/"
_WEEKDAY_MAP = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}


class ShopperTrakApiClient:
    """Class for querying the ShopperTrak API for location visits data"""

    def __init__(self, username, password, location_hours_dict):
        self.logger = create_log("shoppertrak_api_client")
        self.base_url = os.environ["SHOPPERTRAK_API_BASE_URL"]
        self.auth = HTTPBasicAuth(username, password)
        self.max_retries = int(os.environ["MAX_RETRIES"])
        self.today_str = datetime.now(pytz.timezone("US/Eastern")).date().isoformat()
        self.location_hours_dict = location_hours_dict

    def query(self, endpoint, query_date, query_count=1):
        """
        Sends query to ShopperTrak API and returns the result as an XML root if
        possible. If the API returns that it's busy, this method waits and recursively
        calls itself.
        """
        full_url = self.base_url + endpoint
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
            self.logger.error(f"Failed to retrieve response from {full_url}: {e}")
            raise ShopperTrakApiClientError(
                f"Failed to retrieve response from {full_url}: {e}"
            ) from None

        response_root = self._check_response(response.text)
        if response_root == "E108":
            if query_count < self.max_retries:
                self.logger.info("Waiting 5 minutes and trying again")
                time.sleep(300)
                return self.query(endpoint, query_date, query_count + 1)
            else:
                self.logger.error(
                    f"Reached max retries: sent {self.max_retries} queries with no "
                    f"response"
                )
                raise ShopperTrakApiClientError(
                    f"Reached max retries: sent {self.max_retries} queries with no "
                    f"response"
                )
        else:
            return response_root

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
        # checking if it occurs during a library's regular hours. This assumes imputed
        # data during a library's regular hours will always have at least one entrance/
        # exit, but this is acceptable as it's preferable to erroneously mark imputed
        # data as missing than the reverse.
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

    def _check_response(self, response_text):
        """
        If API response is XML, does not contain an error, and contains at least one
        traffic attribute, returns response as an XML root. Otherwise, throws an error.
        """
        try:
            root = ET.fromstring(response_text)
            error = root.find("error")
        except ET.ParseError as e:
            self.logger.error(f"Could not parse XML response {response_text}: {e}")
            raise ShopperTrakApiClientError(
                f"Could not parse XML response {response_text}: {e}"
            ) from None

        if error is not None and error.text is not None:
            # E104 is used when the given site ID matches multiple sites
            if error.text == "E104":
                self.logger.warning("E104: site ID has multiple matches")
                return "E104"
            # E107 is used when the daily API limit has been exceeded
            elif error.text == "E107":
                self.logger.info("E107: API limit exceeded")
                return "E107"
            # E108 is used when ShopperTrak is busy and they recommend trying again
            elif error.text == "E108":
                self.logger.info("E108: ShopperTrak is busy")
                return "E108"
            else:
                self.logger.error(f"Error found in XML response: {response_text}")
                raise ShopperTrakApiClientError(
                    f"Error found in XML response: {response_text}"
                )
        elif len(root.findall(".//traffic")) == 0:
            self.logger.error(f"No traffic found in XML response: {response_text}")
            raise ShopperTrakApiClientError(
                f"No traffic found in XML response: {response_text}"
            )
        else:
            return root

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
