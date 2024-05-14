import os
import pytz
import xml.etree.ElementTree as ET

from datetime import datetime, timedelta
from helpers.query_helper import (
    build_redshift_create_table_query,
    build_redshift_hours_query,
    build_redshift_known_query,
    build_redshift_update_query,
    REDSHIFT_DROP_QUERY,
    REDSHIFT_RECOVERABLE_QUERY,
)
from lib import (
    ShopperTrakApiClient,
    ALL_SITES_ENDPOINT,
    SINGLE_SITE_ENDPOINT,
)
from nypl_py_utils.classes.avro_encoder import AvroEncoder
from nypl_py_utils.classes.kinesis_client import KinesisClient
from nypl_py_utils.classes.redshift_client import RedshiftClient
from nypl_py_utils.classes.s3_client import S3Client
from nypl_py_utils.functions.log_helper import create_log


class PipelineController:
    """Class for orchestrating pipeline runs"""

    def __init__(self):
        self.logger = create_log("pipeline_controller")
        self.shoppertrak_api_client = ShopperTrakApiClient(
            os.environ["SHOPPERTRAK_USERNAME"],
            os.environ["SHOPPERTRAK_PASSWORD"],
            dict(),
        )
        self.redshift_client = RedshiftClient(
            os.environ["REDSHIFT_DB_HOST"],
            os.environ["REDSHIFT_DB_NAME"],
            os.environ["REDSHIFT_DB_USER"],
            os.environ["REDSHIFT_DB_PASSWORD"],
        )
        self.avro_encoder = AvroEncoder(os.environ["LOCATION_VISITS_SCHEMA_URL"])

        self.yesterday = datetime.now(pytz.timezone("US/Eastern")).date() - timedelta(
            days=1
        )
        redshift_suffix = ""
        if os.environ["REDSHIFT_DB_NAME"] != "production":
            redshift_suffix = "_" + os.environ["REDSHIFT_DB_NAME"]
        self.redshift_visits_table = "location_visits" + redshift_suffix
        self.redshift_hours_table = "location_hours" + redshift_suffix
        self.redshift_branch_codes_table = "branch_codes_map" + redshift_suffix

        self.ignore_update = os.environ.get("IGNORE_UPDATE", False) == "True"
        self.ignore_cache = os.environ.get("IGNORE_CACHE", False) == "True"
        if not self.ignore_cache:
            self.s3_client = S3Client(
                os.environ["S3_BUCKET"], os.environ["S3_RESOURCE"]
            )

        self.ignore_kinesis = os.environ.get("IGNORE_KINESIS", False) == "True"
        if not self.ignore_kinesis:
            self.kinesis_client = KinesisClient(
                os.environ["KINESIS_STREAM_ARN"], int(os.environ["KINESIS_BATCH_SIZE"])
            )

    def run(self):
        """Main method for the class -- runs the pipeline"""
        self.logger.info("Getting regular branch hours from Redshift")
        self.shoppertrak_api_client.location_hours_dict = self.get_location_hours_dict()

        all_sites_start_date = self._get_poll_date(0) + timedelta(days=1)
        all_sites_end_date = (
            datetime.strptime(os.environ["END_DATE"], "%Y-%m-%d").date()
            if self.ignore_cache
            else self.yesterday
        )
        self.logger.info(
            f"Getting all sites data from {all_sites_start_date} through "
            f"{all_sites_end_date}"
        )
        self.process_all_sites_data(all_sites_end_date, 0)
        self.logger.info("Finished querying for all sites data")
        if not self.ignore_cache:
            self.s3_client.close()

        broken_start_date = self.yesterday - timedelta(days=29)
        self.logger.info(
            f"Attempting to recover previously unhealthy data from {broken_start_date} "
            f"up to {all_sites_start_date}"
        )
        self.process_broken_orbits(broken_start_date, all_sites_start_date)
        self.logger.info("Finished attempting to recover unhealthy data")
        if not self.ignore_kinesis:
            self.kinesis_client.close()

    def get_location_hours_dict(self):
        """
        Queries Redshift for each location's current regular hours and returns a map
        from (branch_code, weekday) to (regular_open, regular_close)
        """
        self.redshift_client.connect()
        raw_hours = self.redshift_client.execute_query(
            build_redshift_hours_query(
                self.redshift_hours_table, self.redshift_branch_codes_table
            )
        )
        self.redshift_client.close_connection()
        return {(row[0], row[1]): (row[2], row[3]) for row in raw_hours}

    def process_all_sites_data(self, end_date, batch_num):
        """Gets visits data from all available sites for the given day(s)"""
        last_poll_date = self._get_poll_date(batch_num)
        poll_date = last_poll_date + timedelta(days=1)
        if poll_date <= end_date:
            self.logger.info(f"Beginning batch {batch_num+1}: {poll_date.isoformat()}")
            all_sites_xml_root = self.shoppertrak_api_client.query(
                ALL_SITES_ENDPOINT, poll_date
            )
            if type(all_sites_xml_root) != ET.Element:
                self.logger.error(
                    "Error querying ShopperTrak API for all sites visits data"
                )
                raise PipelineControllerError(
                    "Error querying ShopperTrak API for all sites visits data"
                ) from None

            results = self.shoppertrak_api_client.parse_response(
                all_sites_xml_root, poll_date
            )
            encoded_records = self.avro_encoder.encode_batch(results)
            if not self.ignore_kinesis:
                self.kinesis_client.send_records(encoded_records)
            if not self.ignore_cache:
                self.s3_client.set_cache({"last_poll_date": poll_date.isoformat()})

            self.logger.info(f"Finished batch {batch_num+1}: {poll_date.isoformat()}")
            self.process_all_sites_data(end_date, batch_num + 1)

    def process_broken_orbits(self, start_date, end_date):
        """
        Re-queries individual sites with unhealthy data from the past 30 days (a limit
        set by the API) to see if any data has since been recovered
        """
        create_table_query = build_redshift_create_table_query(
            self.redshift_visits_table, start_date, end_date
        )
        self.redshift_client.connect()
        self.redshift_client.execute_transaction([(create_table_query, None)])
        recoverable_site_dates = self.redshift_client.execute_query(
            REDSHIFT_RECOVERABLE_QUERY
        )
        known_data = self.redshift_client.execute_query(
            build_redshift_known_query(self.redshift_visits_table)
        )
        self.redshift_client.execute_transaction([(REDSHIFT_DROP_QUERY, None)])

        # For all the site/date pairs with unhealthy data, form a dictionary of the
        # currently stored data for those sites on those dates where the key is (site
        # ID, orbit, timestamp) and the value is (Redshift ID, is_healthy_data, enters,
        # exits). This is to mark old rows as stale and to prevent sending duplicate
        # records when only some of the data for a site needs to be recovered on a
        # particular date (e.g. when only one of several orbits is broken, or when an
        # orbit goes down in the middle of the day).
        known_data_dict = dict()
        if known_data:
            known_data_dict = {
                (row[0], row[1], row[2]): (row[3], row[4], row[5], row[6])
                for row in known_data
            }
        self._recover_data(recoverable_site_dates, known_data_dict)
        self.redshift_client.close_connection()

    def _recover_data(self, site_dates, known_data_dict):
        """
        Individually query the ShopperTrak API for each site/date pair with any
        unhealthy data. Then check to see if the returned data is actually "recovered"
        data, as it may have never been unhealthy to begin with. If so, send to Kinesis.
        """
        for row in site_dates:
            site_xml_root = self.shoppertrak_api_client.query(
                SINGLE_SITE_ENDPOINT + row[0], row[1]
            )
            # If multiple sites match the same site ID (E104), continue to the next site
            # ID. If the API limit has been reached (E107), stop.
            if site_xml_root == "E104":
                continue
            elif site_xml_root == "E107":
                break
            else:
                site_results = self.shoppertrak_api_client.parse_response(
                    site_xml_root, row[1], is_recovery_mode=True
                )
                self._process_recovered_data(site_results, known_data_dict)

    def _process_recovered_data(self, recovered_data, known_data_dict):
        """
        Check that ShopperTrak "recovered" data was actually unhealthy to begin with
        and, if so, encode, send to Kinesis, and mark old Redshift rows as stale
        """
        results = []
        stale_ids = []
        for fresh_row in recovered_data:
            key = (
                fresh_row["shoppertrak_site_id"],
                fresh_row["orbit"],
                datetime.strptime(fresh_row["increment_start"], "%Y-%m-%d %H:%M:%S"),
            )
            if key not in known_data_dict:
                results.append(fresh_row)
            else:
                known_row = known_data_dict[key]
                if not known_row[1]:  # previously unhealthy data
                    results.append(fresh_row)
                    stale_ids.append(str(known_row[0]))
                elif (  # previously healthy data that doesn't match the new API data
                    fresh_row["enters"] != known_row[2]
                    or fresh_row["exits"] != known_row[3]
                ):
                    self.logger.warning(
                        f"Different healthy data found in API and Redshift: {key} "
                        f"mapped to {fresh_row} in the API and {known_row} in Redshift"
                    )

        # Mark old rows for successfully recovered data as stale
        if stale_ids:
            self.logger.info(f"Updating {len(stale_ids)} stale records")
            update_query = build_redshift_update_query(
                self.redshift_visits_table, ",".join(stale_ids)
            )
            if not self.ignore_update:
                self.redshift_client.execute_transaction([(update_query, None)])

        if results:
            encoded_records = self.avro_encoder.encode_batch(results)
            if not self.ignore_kinesis:
                self.kinesis_client.send_records(encoded_records)
        else:
            self.logger.info("No recovered data found")

    def _get_poll_date(self, batch_num):
        """Retrieves the last poll date from the S3 cache or the config"""
        if self.ignore_cache:
            poll_str = os.environ["LAST_POLL_DATE"]
            poll_date = datetime.strptime(poll_str, "%Y-%m-%d").date()
            return poll_date + timedelta(days=batch_num)
        else:
            poll_str = self.s3_client.fetch_cache()["last_poll_date"]
            return datetime.strptime(poll_str, "%Y-%m-%d").date()


class PipelineControllerError(Exception):
    def __init__(self, message=None):
        self.message = message
