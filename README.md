# Download Service

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg?style=for-the-badge)](https://www.gnu.org/licenses/agpl-3.0)
[![Python 3.7](https://img.shields.io/badge/python-3.7-green?style=for-the-badge)](https://www.python.org/)
[![GitHub Workflow Status (branch)](https://img.shields.io/github/workflow/status/pilotdataplatform/download/CI/develop?style=for-the-badge)](https://github.com/PilotDataPlatform/download/actions/workflows/cicd.yml)
[![codecov](https://img.shields.io/codecov/c/github/PilotDataPlatform/download?style=for-the-badge)](https://codecov.io/gh/PilotDataPlatform/download)

The upload service is one of the key component for PILOT project. It's built using the FastAPI python framework. The main responsibility is to handle the file/folder download from [Minio](https://min.io/) Object Storage. If api reqeusts to download multiple files or folder, the service will combine them as the zip file.

## Getting Started

This is an example of how to run the download service locally.

### Prerequisites

This project is using [Poetry](https://python-poetry.org/docs/#installation) to handle the dependencies.

    curl -sSL https://install.python-poetry.org | python3 -

### Installation & Quick Start

1. Clone the project.

       git clone https://github.com/PilotDataPlatform/download.git

2. Install dependencies.

       poetry install

3. Install any OS level dependencies if needed.

       apt install <required_package>
       brew install <required_package>

5. Add environment variables into `.env` in case it's needed. Use `.env.schema` as a reference.

6. Run application.

       poetry run python run.py

### Startup using Docker

This project can also be started using [Docker](https://www.docker.com/get-started/).

1. To build and start the service within the Docker container, run:

       docker compose up

## Resources

* [Pilot Platform API Documentation](https://pilotdataplatform.github.io/api-docs/)
* [Pilot Platform Helm Charts](https://github.com/PilotDataPlatform/helm-charts/tree/main/download-service)

## Contribution

You can contribute the project in following ways:

* Report a bug.
* Suggest a feature.
* Open a pull request for fixing issues or adding functionality. Please consider
  using [pre-commit](https://pre-commit.com) in this case.
* For general guidelines on how to contribute to the project, please take a look at the [contribution guides](CONTRIBUTING.md).
