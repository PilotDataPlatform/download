# Download Service

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg?style=for-the-badge)](https://www.gnu.org/licenses/agpl-3.0)
[![Python 3.7](https://img.shields.io/badge/python-3.7-green?style=for-the-badge)](https://www.python.org/)
[![GitHub Workflow Status (branch)](https://img.shields.io/github/workflow/status/pilotdataplatform/dataset/ci/main?style=for-the-badge)](https://github.com/PilotDataPlatform/download/actions/workflows/cicd.yml)
[![codecov](https://img.shields.io/codecov/c/github/PilotDataPlatform/dataset?style=for-the-badge)](https://codecov.io/gh/PilotDataPlatform/download)

This service is built for file data downloading purpose. It's built using the FastAPI python framework.

## Development

1. Install [Poetry](https://python-poetry.org/docs/#installation).

2. Install dependencies.

       poetry install

3. Install [Pre Commit](https://pre-commit.com/#installation)

       pip3 install pre_commit

3. Add environment variables into `.env`. taking in consideration `.env.schema`.
4. Run application.

       poetry run python run.py

## Running Tests

1. You will need to start a redis:

        docker run --name test-redis -p 6379:6379 -d redis

2. Run tests

        poetry run pytest
