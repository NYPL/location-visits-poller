import os
import pytz

from datetime import datetime, timedelta
from helpers.query_helper import (
    build_redshift_create_table_query,
    build_redshift_known_query,
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
        self.redshift_client = RedshiftClient(
            os.environ["REDSHIFT_DB_HOST"],
            os.environ["REDSHIFT_DB_NAME"],
            os.environ["REDSHIFT_DB_USER"],
            os.environ["REDSHIFT_DB_PASSWORD"],
        )
        self.shoppertrak_api_client = ShopperTrakApiClient(
            os.environ["SHOPPERTRAK_USERNAME"], os.environ["SHOPPERTRAK_PASSWORD"]
        )
        self.avro_encoder = AvroEncoder(os.environ["LOCATION_VISITS_SCHEMA_URL"])

        self.yesterday = datetime.now(pytz.timezone("US/Eastern")).date() - timedelta(
            days=1
        )
        self.redshift_table = "location_visits"
        if os.environ["REDSHIFT_DB_NAME"] != "production":
            self.redshift_table += "_" + os.environ["REDSHIFT_DB_NAME"]

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
            f"Attempting to recover previously missing data from {broken_start_date} "
            f"up to {all_sites_start_date}"
        )
        self.process_broken_orbits(broken_start_date, all_sites_start_date)
        self.logger.info("Finished attempting to recover missing data")
        if not self.ignore_kinesis:
            self.kinesis_client.close()

    def process_all_sites_data(self, end_date, batch_num):
        """Gets visits data from all available sites for the given day(s)"""
        last_poll_date = self._get_poll_date(batch_num)
        poll_date = last_poll_date + timedelta(days=1)
        if poll_date <= end_date:
            poll_date_str = poll_date.strftime("%Y%m%d")
            self.logger.info(f"Beginning batch {batch_num+1}: {poll_date_str}")

            all_sites_xml_root = self.shoppertrak_api_client.query(
                ALL_SITES_ENDPOINT, poll_date_str
            )
            results = self.shoppertrak_api_client.parse_response(
                all_sites_xml_root, poll_date_str
            )

            encoded_records = self.avro_encoder.encode_batch(results)
            if not self.ignore_kinesis:
                self.kinesis_client.send_records(encoded_records)
            if not self.ignore_cache:
                self.s3_client.set_cache({"last_poll_date": poll_date.isoformat()})

            self.logger.info(f"Finished batch {batch_num+1}: {poll_date_str}")
            self.process_all_sites_data(end_date, batch_num + 1)

    def process_broken_orbits(self, start_date, end_date):
        """
        Re-queries individual sites with missing data from the past 30 days (a limit set
        by the API) to see if any data has since been recovered
        """
        create_table_query = build_redshift_create_table_query(
            self.redshift_table, start_date, end_date
        )
        self.redshift_client.connect()
        self.redshift_client.execute_transaction([(create_table_query, None)])
        recoverable_site_dates = self.redshift_client.execute_query(
            REDSHIFT_RECOVERABLE_QUERY
        )
        known_data = self.redshift_client.execute_query(
            build_redshift_known_query(self.redshift_table)
        )
        self.redshift_client.execute_transaction([(REDSHIFT_DROP_QUERY, None)])
        self.redshift_client.close_connection()

        # For all the site/date pairs with missing data, form a dictionary of the
        # currently stored healthy data for those sites on those dates where the key is
        # (site ID, orbit, timestamp) and the value is (enters, exits). This is to
        # prevent sending duplicate records when only some of the data for a site needs
        # to be recovered on a particular date (e.g. when only one of several orbits is
        # broken, or when an orbit goes down in the middle of the day).
        known_data_dict = dict()
        if known_data:
            known_data_dict = {
                (row[0], row[1], row[2]): (row[3], row[4]) for row in known_data
            }
        self._recover_data(recoverable_site_dates, known_data_dict)

    def _recover_data(self, site_dates, known_data_dict):
        """
        Individually query the ShopperTrak API for each site/date pair with any missing
        data. Then check to see if the returned data is actually "recovered" data, as it
        may have never been missing to begin with. If so, send to Kinesis.
        """
        for row in site_dates:
            date_str = row[1].strftime("%Y%m%d")
            site_xml_root = self.shoppertrak_api_client.query(
                SINGLE_SITE_ENDPOINT + row[0], date_str
            )
            if site_xml_root:
                site_results = self.shoppertrak_api_client.parse_response(
                    site_xml_root, date_str, is_recovery_mode=True
                )
                self._check_recovered_data(site_results, known_data_dict)

    def _check_recovered_data(self, recovered_data, known_data_dict):
        """
        Check that ShopperTrak "recovered" data was actually missing to begin with and,
        if so, encode and send to Kinesis
        """
        results = []
        for row in recovered_data:
            fresh_key = (
                row["shoppertrak_site_id"],
                row["orbit"],
                datetime.strptime(row["increment_start"], "%Y-%m-%d %H:%M:%S"),
            )
            fresh_value = (row["enters"], row["exits"])
            if fresh_key in known_data_dict:
                # If the data was already healthy in Redshift, check that the new API
                # value matches what's in Redshift. Note that this assumes there is only
                # a single healthy row in Redshift for each key. Otherwise, only the
                # Redshift row with the most recent poll_date is checked.
                if known_data_dict[fresh_key] != fresh_value:
                    self.logger.warning(
                        f"Different healthy data found in API and Redshift: "
                        f"{fresh_key} mapped to {fresh_value} in the API and "
                        f"{known_data_dict[fresh_key]} in Redshift"
                    )
            else:
                results.append(row)

        if results:
            encoded_records = self.avro_encoder.encode_batch(results)
            if not self.ignore_kinesis:
                self.kinesis_client.send_records(encoded_records)

    def _get_poll_date(self, batch_num):
        """Retrieves the last poll date from the S3 cache or the config"""
        if self.ignore_cache:
            poll_str = os.environ["LAST_POLL_DATE"]
            poll_date = datetime.strptime(poll_str, "%Y-%m-%d").date()
            return poll_date + timedelta(days=batch_num)
        else:
            poll_str = self.s3_client.fetch_cache()["last_poll_date"]
            return datetime.strptime(poll_str, "%Y-%m-%d").date()
