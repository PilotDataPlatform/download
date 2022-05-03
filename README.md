# Download Service

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg?style=for-the-badge)](https://www.gnu.org/licenses/agpl-3.0)
[![Python 3.7](https://img.shields.io/badge/python-3.7-green?style=for-the-badge)](https://www.python.org/)
[![GitHub Workflow Status (branch)](https://img.shields.io/github/workflow/status/pilotdataplatform/download/CI/main?style=for-the-badge)](https://github.com/PilotDataPlatform/download/actions/workflows/cicd.yml)
[![codecov](https://img.shields.io/codecov/c/github/PilotDataPlatform/download?style=for-the-badge)](https://codecov.io/gh/PilotDataPlatform/download)

This service is built for file data downloading purpose. It's built using the FastAPI python framework.

# About The Project

The upload service is one of the component for PILOT project. The main responsibility is to handle the file/folder download from [Minio](https://min.io/) Object Storage. If api reqeusts to download multiple files or folder, the service will combine them as the zip file.

## Built With

 - [Minio](https://min.io/): The Object Storage to save the data

 - [Fastapi](https://fastapi.tiangolo.com): The async api framework for backend

 - [poetry](https://python-poetry.org/): python package management

 - [docker](https://docker.com)

# Getting Started


## Prerequisites

 1. The project is using poetry to handle the package. **Note here the poetry must install globally not in the anaconda virtual environment**

 ```
 pip install poetry
 ```

 2. add the precommit package:

 ```
 pip3 install pre_commit
 ```

## Installation

 1. git clone the project:
 ```
 git clone https://github.com/PilotDataPlatform/download.git
 ```

 2. install the package:
 ```
 poetry install
 ```

 3. create the `.env` file from `.env.schema`

 4. run it locally:
 ```
 poetry run python run.py
 ```

## Docker

To package up the service into docker pod, running following command:

```
docker build .
```

## API Documents

REST API documentation in the form of Swagger/OpenAPI can be found here: [Api Document](https://pilotdataplatform.github.io/api-docs/)

## Helm Charts

Components of the Pilot Platform are available as helm charts for installation on Kubernetes: [Download Service Helm Charts](https://github.com/PilotDataPlatform/helm-charts/tree/main/download-service)


# Colaboration

## Run tests

The test will require to start a test redis (without a password TESTONLY)

```
docker start test-redis || docker run --name test-redis -p 6379:6379 -d redis
```

To run pytest with coverage rum

```
make test
```

To run only pytest without coverage:

```
poetry run pytest
```
