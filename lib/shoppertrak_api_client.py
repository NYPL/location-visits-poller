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


class ShopperTrakApiClient:
    """Class for querying the ShopperTrak API for location visits data"""

    def __init__(self, username, password):
        self.logger = create_log("shoppertrak_api_client")
        self.base_url = os.environ["SHOPPERTRAK_API_BASE_URL"]
        self.auth = HTTPBasicAuth(username, password)
        self.max_retries = int(os.environ["MAX_RETRIES"])
        self.today_str = datetime.now(pytz.timezone("US/Eastern")).date().isoformat()

    def query(self, endpoint, date_str, query_count=1):
        full_url = self.base_url + endpoint

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
        if response_root is not None:
            return response_root
        elif query_count < self.max_retries:
            self.logger.info("Waiting 5 minutes and trying again")
            time.sleep(300)
            return self.query(endpoint, date_str, query_count + 1)
        else:
            self.logger.error(
                f"Reached max retries: sent {self.max_retries} queries with no response"
            )
            raise ShopperTrakApiClientError(
                f"Reached max retries: sent {self.max_retries} queries with no response"
            )

    def parse_response(self, xml_root, input_date_str, is_recovery_mode=False):
        rows = []
        for site_xml in xml_root.findall("site"):
            site_str = self._get_xml_str(site_xml, "siteID")
            for date_xml in site_xml.findall("date"):
                date_str = self._get_xml_str(date_xml, "dateValue")
                if date_str != input_date_str:
                    self.logger.error(
                        f"Request date does not match response date.\nRequest date: "
                        f"{input_date_str}\nResponse date: {date_str}"
                    )
                    raise ShopperTrakApiClientError(
                        f"Request date does not match response date.\nRequest date: "
                        f"{input_date_str}\nResponse date: {date_str}"
                    )
                for entrance_xml in date_xml.findall("entrance"):
                    entrance_val = self._get_xml_str(entrance_xml, "name")
                    if entrance_val:
                        entrance_val = self._cast_str_to_int(entrance_val.lstrip("EP"))
                    for traffic_xml in entrance_xml.findall("traffic"):
                        enters = self._cast_str_to_int(
                            self._get_xml_str(traffic_xml, "enters")
                        )
                        exits = self._cast_str_to_int(
                            self._get_xml_str(traffic_xml, "exits")
                        )

                        start_time_str = self._get_xml_str(traffic_xml, "startTime")
                        start_dt_str = datetime.strptime(
                            date_str + " " + start_time_str, "%Y%m%d %H%M%S"
                        ).isoformat()

                        code = self._get_xml_str(traffic_xml, "code")
                        is_healthy_orbit = code == "01"
                        if code != "01" and code != "02":
                            self.logger.warning(
                                f"Unknown code: '{code}'. Setting is_real to False"
                            )

                        if is_healthy_orbit or not is_recovery_mode:
                            rows.append(
                                {
                                    "shoppertrak_site_id": site_str,
                                    "orbit": entrance_val,
                                    "increment_start": start_dt_str,
                                    "enters": enters,
                                    "exits": exits,
                                    "is_healthy_orbit": is_healthy_orbit,
                                    "is_recovery_data": is_recovery_mode,
                                    "poll_date": self.today_str,
                                }
                            )
        return rows

    def _check_response(self, response_text):
        try:
            root = ET.fromstring(response_text)
            error = root.find("error")
        except Exception as e:
            self.logger.error(f"Could not parse XML response {response_text}: {e}")
            raise ShopperTrakApiClientError(
                f"Could not parse XML response {response_text}: {e}"
            ) from None

        if error is not None and error.text is not None:
            # E108 is used when ShopperTrak is busy and they recommend trying again
            if error.text == "E108":
                self.logger.info("E108: ShopperTrak is busy")
                return None
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
        attribute_str = (xml.get(attribute) or "").strip()
        if not attribute_str:
            self.logger.warning(f"Found blank '{attribute}'")
            return None
        else:
            return attribute_str

    def _cast_str_to_int(self, input_str):
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
