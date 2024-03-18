# LocationVisitsPoller

The LocationVisitsPoller periodically hits the ShopperTrak API for the number of incoming and outgoing visitors per location per day and writes the data to LocationVisits Kinesis streams for ingest into the [BIC](https://github.com/NYPL/BIC).

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
This repo uses the [Main-QA-Production](https://github.com/NYPL/engineering-general/blob/main/standards/git-workflow.md#main-qa-production) git workflow.

[`main`](https://github.com/NYPL/location-visits-poller/tree/main) has the latest and greatest commits, [`qa`](https://github.com/NYPL/location-visits-poller/tree/qa) has what's in our QA environment, and [`production`](https://github.com/NYPL/location-visits-poller/tree/production) has what's in our production environment.

### Ideal Workflow
- Cut a feature branch off of `main`
- Commit changes to your feature branch
- File a pull request against `main` and assign a reviewer
  - In order for the PR to be accepted, it must pass all unit tests, have no lint issues, and update the CHANGELOG (or contain the Skip-Changelog label in GitHub)
- After the PR is accepted, merge into `main`
- Merge `main` > `qa`
- Deploy app to QA and confirm it works
- Merge `qa` > `production`
- Deploy app to production and confirm it works

## Deployment
The poller is deployed as an AWS ECS service to [qa](https://us-east-1.console.aws.amazon.com/ecs/home?region=us-east-1#/clusters/location-visits-poller-qa/services) and [prod](https://us-east-1.console.aws.amazon.com/ecs/home?region=us-east-1#/clusters/location-visits-poller-production/services) environments. To upload a new QA version of this service, create a new release in GitHub off of the `qa` branch and tag it `qa-vX.X.X`. The GitHub Actions deploy-qa workflow will then deploy the code to ECR and update the ECS service appropriately. To deploy to production, create the release from the `production` branch and tag it `production-vX.X.X`.

## Environment variables
Every variable not marked as optional below is required for the poller to run. There are additional optional variables that can be used for development purposes -- `devel.yaml` sets each of these and they are described below. Note that the `qa.yaml` and `production.yaml` files are actually read by the deployed service, so do not change these files unless you want to change how the service will behave in the wild -- these are not meant for local testing.