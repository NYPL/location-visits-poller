import os
import pytz

from datetime import datetime, timedelta
from lib import ShopperTrakApiClient, ALL_SITES_ENDPOINT
from nypl_py_utils.classes.avro_encoder import AvroEncoder
from nypl_py_utils.classes.kinesis_client import KinesisClient
from nypl_py_utils.classes.s3_client import S3Client
from nypl_py_utils.functions.log_helper import create_log


class PipelineController:
    """Class for orchestrating pipeline runs"""

    def __init__(self):
        self.logger = create_log("pipeline_controller")
        self.shoppertrak_api_client = ShopperTrakApiClient(
            os.environ["SHOPPERTRAK_USERNAME"], os.environ["SHOPPERTRAK_PASSWORD"]
        )
        self.avro_encoder = AvroEncoder(os.environ["LOCATION_VISITS_SCHEMA_URL"])

        self.yesterday = datetime.now(pytz.timezone("US/Eastern")).date() - timedelta(
            days=1
        )
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
        if not self.ignore_cache:
            self.s3_client.close()
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

    def _get_poll_date(self, batch_num):
        """Retrieves the last poll date from the S3 cache or the config"""
        if self.ignore_cache:
            poll_str = os.environ["LAST_POLL_DATE"]
            poll_date = datetime.strptime(poll_str, "%Y-%m-%d").date()
            return poll_date + timedelta(days=batch_num)
        else:
            poll_str = self.s3_client.fetch_cache()["last_poll_date"]
            return datetime.strptime(poll_str, "%Y-%m-%d").date()
