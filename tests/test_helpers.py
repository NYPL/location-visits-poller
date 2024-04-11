import os


class TestHelpers:
    ENV_VARS = {
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
        "LOCATION_VISITS_SCHEMA_URL": "https://test_schema_url",
        "KINESIS_BATCH_SIZE": "5",
        "KINESIS_STREAM_ARN": "test_kinesis_stream",
        "SHOPPERTRAK_USERNAME": "test_shoppertrak_username",
        "SHOPPERTRAK_PASSWORD": "test_shoppertrak_password",
    }

    @classmethod
    def set_env_vars(cls):
        for key, value in cls.ENV_VARS.items():
            os.environ[key] = value

    @classmethod
    def clear_env_vars(cls):
        for key in cls.ENV_VARS.keys():
            if key in os.environ:
                del os.environ[key]
