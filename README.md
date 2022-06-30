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
$ pip install -t src/libs/pandas/python -r pandas_requirements.txt
$ pip install -t src/libs/database/python -r database_requirements.txt
$ pip install -t src/libs/fastapi/python -r fastapi_requirements.txt
```
