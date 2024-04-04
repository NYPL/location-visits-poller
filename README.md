# LocationVisitsPoller

The LocationVisitsPoller periodically hits the ShopperTrak API for the number of incoming and outgoing visitors per location per day and writes the data to LocationVisits Kinesis streams for ingest into the [BIC](https://github.com/NYPL/BIC). Note that because there are onerous ShopperTrak API rate limits and there is no QA version of the API, the poller is only deployed to production and there is no QA version.

## Running locally
* Add your `AWS_PROFILE` to the config file for the environment you want to run
  * Alternatively, you can manually export it (e.g. `export AWS_PROFILE=<profile>`)
* Run `ENVIRONMENT=<env> python main.py`
  * `<env>` should be the config filename without the `.yaml` suffix. Note that running the poller with `production.yaml` will actually send records to the production Kinesis stream -- it is not meant to be used for development purposes.
  * `make run` will run the poller using the development environment
* Alternatively, to build and run a Docker container, run:
```
docker image build -t location-visits-poller:local .

docker container run -e ENVIRONMENT=<env> -e AWS_ACCESS_KEY_ID=<> -e AWS_SECRET_ACCESS_KEY=<> location-visits-poller:local
```

## Git workflow
This repo has only two branches: [`main`](https://github.com/NYPL/location-visits-poller/tree/main), which contains the latest and greatest commits and [`production`](https://github.com/NYPL/location-visits-poller/tree/production), which contains what's in our production environment.

### Workflow
- Cut a feature branch off of `main`
- Commit changes to your feature branch
- File a pull request against `main` and assign a reviewer
  - In order for the PR to be accepted, it must pass all unit tests, have no lint issues, and update the CHANGELOG (or contain the Skip-Changelog label in GitHub)
- After the PR is accepted, merge into `main`
- Merge `main` > `production`
- Deploy app to production and confirm it works

## Deployment
The poller is deployed as an AWS ECS service to a [prod](https://us-east-1.console.aws.amazon.com/ecs/home?region=us-east-1#/clusters/location-visits-poller-production/services) environment only. To upload a new version of this service, create a new release in GitHub off of the `production` branch and tag it `production-vX.X.X`. The GitHub Actions deploy-production workflow will then deploy the code to ECR and update the ECS service appropriately.

## Environment variables
Every variable not marked as optional below is required for the poller to run. There are additional optional variables that can be used for development purposes -- `devel.yaml` sets each of these and they are described below. Note that the `production.yaml` file is actually read by the deployed service, so do not change it unless you want to change how the service will behave in the wild -- it is not meant for local testing.

| Name        | Notes           |
| ------------- | ------------- |
| `AWS_REGION` | Always `us-east-1`. The AWS region used for the Redshift, S3, KMS, and Kinesis clients. |
| `SHOPPERTRAK_API_BASE_URL` | ShopperTrak API base URL to which the poller sends requests. This is not a full endpoint, as either the `site` or `allsites` endpoints may be used. |
| `MAX_RETRIES` | Number of times to try hitting the ShopperTrak API if it's busy before throwing an error |
| `S3_BUCKET` | S3 bucket for the cache. This can be empty when `IGNORE_CACHE` is `True`. |
| `S3_RESOURCE` | Name of the resource for the S3 cache. This can be empty when `IGNORE_CACHE` is `True`. |
| `LOCATION_VISITS_SCHEMA_URL` | Platform API endpoint from which to retrieve the LocationVisits Avro schema |
| `KINESIS_BATCH_SIZE` | How many records should be sent to Kinesis at once. Kinesis supports up to 500 records per batch. This can be empty when `IGNORE_KINESIS` is `True`. |
| `KINESIS_STREAM_ARN` | Encrypted ARN for the Kinesis stream the poller sends the encoded data to |
| `SHOPPERTRAK_USERNAME` | Encrypted ShopperTrak API username |
| `SHOPPERTRAK_PASSWORD` | Encrypted ShopperTrak API password |
| `LOG_LEVEL` (optional) | What level of logs should be output. Set to `info` by default. |
| `LAST_POLL_DATE` (optional) | If `IGNORE_CACHE` is `True`, the starting state. The first date to be queried will be the day *after* this date. If `IGNORE_CACHE` is `False`, this field is not read. |
| `LAST_END_DATE` (optional) | The most recent date to query for. If this is left blank, it will be yesterday. |
| `IGNORE_CACHE` (optional) | Whether fetching and setting the state from S3 should *not* be done. If this is `True`, the `LAST_POLL_DATE` will be used for the initial state. |
| `IGNORE_KINESIS` (optional) | Whether sending the encoded records to Kinesis should *not* be done |