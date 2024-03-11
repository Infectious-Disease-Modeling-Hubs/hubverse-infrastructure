# Hubverse Data Transformations (Experimental)

Some prototype code that transforms an S3-based Hubverse model-output file:

- Adds three new columns: round_id, team, and model
- Writes a new copy of the file in parquet format

## Contents

1. `model_output.py`: defines a `ModelOutputHandler` class that reads and transforms a single Hubverse model-output file
2. `lambda_function.py`: a handler function for testing the above class in AWS Lambda

## Dev usage

### Installing for local development:

1. Clone this repository
2. `cd hubverse-transforms`
3. Create a virtual environment: `conda create --name hubverse-transforms python=3.12`
4. Activate the virtual environment: `conda activate hubverse-transforms`
5. Install the package in editable mode and include the dev dependencies: `pip install -e ".[dev]"`

### Running the test suite:

1. From the `hubverse-transforms` directory: `pytest`

### Using the ModelOutputHandler

To use this class from your local machine, you'll need to have Hubverse AWS credentials that allow writing to the hub's S3 bucket.

### Deploying to AWS Lambda:

The `create_package.sh` script packages the data transform code based on [instructions here](https://docs.aws.amazon.com/lambda/latest/dg/python-package.html).  

1. `cd hubverse-transforms`
2. `source create_package.sh`

This for testing only: if we decide to use AWS lambda, we'd do this via CI and not via a janky shell script.
The script uses AWS CLI commands to writes the package to S3 and updates the Lambda function, so you'll need to have
AWS CLI installed and configured with the necessary permissions.


### Deploying to AWS Lambda

Because the .zip is > 50 MB, it can't be uploaded directly via the AWM Lambda Console. It has to go to S3 first.

1. `cd hubverse-transforms`
2. `aws s3 cp hubverse_transforms.zip s3://hubverse-infrastructure-test/`
3. In the AWS Lambda Console, upload the .zip from the above S3 location

#### Lambda Limitations

- max size of deployment package (unzipped) = 250 MB
- max function run time = 15 minutes

## Infrastructure Note

This is all tested via on the files in `hubverse-cloud`.
AWS changes to get this working (mostly done via console to get up and running quickly):

- create second role for `hubverse-cloud` that is assumed by the Lambda function: `hubverse-cloud-transform`
- attach a trust policy to the above that allows lambda to assume the role (generated automatically by the console)
- create a new persmissions policy to allows writing logs to CloudWatch (this is generic and could be used for all hubs): `hubverse-cloud-write-bucket-policy`
- attach the new CloudWatch policy to `hubverse-cloud-transform`
- attach the hub's existing "write to the S3 bucket" policy to `hubverse-cloud-transform`