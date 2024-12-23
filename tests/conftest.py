import os
import pytest

from freezegun import freeze_time

# Sets OS vars for entire set of tests
TEST_ENV_VARS = {
    "ENVIRONMENT": "test_environment",
    "AWS_REGION": "test_aws_region",
    "REDSHIFT_DB_NAME": "test_redshift_name",
    "REDSHIFT_DB_HOST": "test_redshift_host",
    "REDSHIFT_DB_USER": "test_redshift_user",
    "REDSHIFT_DB_PASSWORD": "test_redshift_password",
    "SHOPPERTRAK_API_BASE_URL": "https://test_shoppertrak_url/",
    "MAX_RETRIES": "3",
    "S3_BUCKET": "test_s3_bucket",
    "S3_RESOURCE": "test_s3_resource",
    "ALL_SITES_S3_BUCKET": "test_all_sites_s3_bucket",
    "ALL_SITES_S3_RESOURCE": "test_all_sites_s3_resource",
    "LOCATION_VISITS_SCHEMA_URL": "https://test_schema_url",
    "KINESIS_BATCH_SIZE": "5",
    "KINESIS_STREAM_ARN": "test_kinesis_stream",
    "SHOPPERTRAK_USERNAME": "test_shoppertrak_username",
    "SHOPPERTRAK_PASSWORD": "test_shoppertrak_password",
}


@pytest.fixture(scope="session", autouse=True)
def tests_setup_and_teardown():
    # Will be executed before the first test
    os.environ.update(TEST_ENV_VARS)
    freezer = freeze_time("2024-01-01 23:00:00-05:00")
    freezer.start()

    yield

    # Will execute after the final test
    freezer.stop()
    for os_config in TEST_ENV_VARS.keys():
        if os_config in os.environ:
            del os.environ[os_config]