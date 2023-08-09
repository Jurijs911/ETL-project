# Northcoders ETL Data Engineering Project
## Data Ingestion and Warehousing Pipeline

A comprehensive data processing pipeline that utilises serverless AWS services to efficiently manage data flow from a source database to a data warehouse. The data pipeline uses Python-based extract, transform and load tools (ETLs) to ingest data from a the primary data source: A psuedo-production complex OLTP database provided by Northcoders.


- AWS EventBridge is used to schedule an Ingestion Lambda to fetch new and update data.
- The Ingestion Lambda stores data within AWS S3 buckets.
- On successful Ingestion Lambda operation, a Remodelling Lambda is triggered that processes and transforms the ingested data.
- On successful Remodelling Lambda operation, a Loading Lambda is trigged to input formatted data into relevant normalised database tables.
- The Remodelling Lambda processes data that has been read from an AWS S3 bucket, it then correctly formats the data before uploading it to another AWS S3 bucket, which is then 
- AWS Cloudwatch is used to monitor and log pipeline activity


# Prerequisites

- AWS Account
- Python 3.8 or higher
- AWS CLI configured with necessary permissions
- Github account to clone repository



# Installation

This project requires [Pytest](https://docs.pytest.org/en/7.4.x/) v7.2+ to run tests.

A Makefile is provided to run test and development commands.

To set up the dev environment and install all required dependencies, run the following in the terminal:

```sh
make create-environments
```

Unit testing can be carried out with the following command:

```sh
make unit-test
```

Security, test coverage, and PEP8 compliance checks can be carried out with the following command:

```sh
make run-checks
```

Alternatively, the following command will create the environemnt and run all tests and checks:

```sh
make all
```

All dependencies can be found in requirements.txt 


## Deployment

- Github actions is the CI/CD platform used to automate the test and deployment of this application.
- Minimum test coverage of 90% is required for successful deployment.
- Security credentials for deployment use are securely stored and retrieved via AWS Secrets.
- A .env file should be created for local test purposes, using variables named in test modules, to access test source and target databases.

## Folder Structure

- All files relevant to the lambda function and other util functions that are used within it can be found in the 'src' folder in another folder relevant to it, which is either called 'ingestion_lambda', 'loading_lambda' or 'remodelling_lambda'.  
- Outside of the 'src' folder, a 'Terraform' folder can be found which contains all relevant code used for the AWS lambdas can be found. This includes code which adds policies, SNS alerts, alarms and EventBridge etc. to the relevant lambdas.
- In order to test the functionality of all the code as separate functions and working together, there is a 'tests' folder, which is then further separated into a folder for each lambda. Each folder contains the test files for each function relevant to the respective lambda.


# Usage Instructions
## Ingestion Lambda
- The Ingestion Lambda is deployed using AWS Lambda via Terraform code that has been done.
The terraform within the code file configures necessary triggers, such as CloudWatch Events, to schedule the lambda execution.
- Set up required environment variables for connecting to the source data.
- Monitor logs in AWS CloudWatch for execution details.
- Github Actions can be used to run all relevant checks.
- The lambdas are deployed when something is pushed to main.

## Remodeling Lambda
- The Remodelling Lambda is deployed using AWS Lambda via Terraform code in the files.
- The terraform within the code files configures necessary triggers, such as CloudWatch Events, to schedule the lambda execution.
- Set up environment variables for S3 bucket details and any other required configurations.
- Monitor logs for execution details and data processing steps.
- The lambdas are deployed when something is pushed to main.

## Loading Lambda
- The Loading Lambda can be deployed onto AWS with the same method as above, the terraform files mean that it is automatically deployed when something is pushed to main.
- The terraform code files configure triggers and all other necessary permissions to start the lambda.
- Set up environment variables for data warehouse connections and S3 bucket details.
- Monitor logs to track data loading progress and any errors.


## Future Updates
- Implement usage of parquet files as opposed to CSV file format.
- Ingest data from a file source in JSON format, and/or an external API.
- Combine utility functions into a single aggregate function to improve code readability and make it more concise.


# Contact

For questions, feedback, or assistance, you can reach out to:
- [Matthew Heath](https://github.com/mj-heath)
- [Cameron Parsonage](https://github.com/CParso)
- [Zenab Haider](https://github.com/zenabhaider)
- [Yuri Kosticins](https://github.com/Jurijs911)
- [Lisa Scotney](https://github.com/LisaSco)
