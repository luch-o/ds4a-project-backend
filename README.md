# DS4A Project

This repository contains the infrastructure as code to deploy the backend of the project application

## Team Members
DS4A Colombia - Cohort 6 - Team 107
  - Vanessa Flórez Angarita
  - Cristian David Lavacude Galvis
  - Jorge Martinez
  - Andres Montañez
  - William Oquendo
  - Luis Carlos Rodriguez
  - Darwin Pico

## Dependencies

Dependencies of the lambda functions are not included in the repository. In order to download them and enable serverless framework to create the lambda layers use the following commands:

```bash
$ pip install -t libs/pandas/python -r pandas_requirements.txt
$ pip install -t libs/database/python -r database_requirements.txt
$ pip install -t libs/fastapi/python -r fastapi_requirements.txt
```
## Architecture

  - Two microservices written in FastAPI deployed using AWS Lambda and API Gateway:
    - data: to access the data stored in the database.
    - predict: to make inference on the trained model.
  - Event driven data ingestion pipeline using:
    - Amazon S3 as datalake.
    - AWS Lambda for compute.
    - PostgreSQL database hosted in RDS.

Data pipeline workflow:
  1. Admin uploads preprocessed csv file.
  2. Parquetize funcion converts the file to parquet.
  3. Ingest function creates the table and inserts data.
    
![image](https://github.com/user-attachments/assets/917af916-d911-45af-89fa-53f87593c318)
