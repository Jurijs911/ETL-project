# Northcoders ETL Data Engineering Project
## Data Ingestion and Warehousing Pipeline

A comprehensive data processing pipeline that includes an Ingestion Lambda, Remodelling Lambda, and Loading Lambda to efficiently manage data flow from source to the data lake and data warehouse hosted in Amazon Web Services. The data pipeline extracts, tranforms and then loads the primary data source, which is a moderately complex and large database called totesys that simulates the back end data of a commercial application; data is inserted and updated into this database several times a day. 


- The Ingestion Lambda fetches the source data, uploads it to an AWS S3 bucket all whilst logging information to AWS CloudWatch services.
- The Remodelling Lambda processes data that has been read from an AWS S3 bucket, it then correctly formats the data before uploading it to another AWS S3 bucket, which is then used in the Loading Lambda.
- The Loading Lambda processes data that has been read from the second loading AWS S3 bucket, then inserts it into the relevant dimension and fact tables.



# Prerequisites

- AWS Account
- Python 3.8 or higher
- AWS CLI configured with necessary permissions
- Github account to clone repository


# Installation

This project requires [Pytest](https://docs.pytest.org/en/7.4.x/) v7.2+ to run tests.

Install the dependencies and devDependencies in order to start the server. By running the following commands detailed in the MakeFile, it will set the environment and allow for the complete testing to be run. Alternatively, run make-all to do everything at once.

In the terminal run:
```sh
make create-environments
make unit-test
make run-checks
```

- All dependencies can be found in requirements.txt and will be downloaded when make create-environment is run in the terminal.

## Deployment Pipeline

- Github actions is used to deploy via automated quality and tests services. 
- Python code is pep8 complient and CI/CD is consistenly used throughout.
- Test coverage exceeds 96%.

## Folder Structure

- All files relevant to the lambda function and other util functions that are used within it can be found in the 'src' folder in another folder relevant to it, which is either called 'ingestion_lambda', 'loading' or 'remodelling'.  
- Outside of the 'src' folder, a 'Terraform' folder can be found which contains all relevant code used for the AWS lambdas can be found. This includes code which adds policies, SNS alerts, alarms and eventbridge etc. to the relevant lambdas.
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
