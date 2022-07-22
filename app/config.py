# PILOT
# Copyright (C) 2022 Indoc Research
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from typing import Any, Dict

from common import VaultClient
from pydantic import BaseSettings, Extra
from starlette.config import Config

config = Config('.env')

SRV_NAMESPACE = config('APP_NAME', cast=str, default='service_download')
CONFIG_CENTER_ENABLED = config('CONFIG_CENTER_ENABLED', cast=str, default='false')


def load_vault_settings(settings: BaseSettings) -> Dict[str, Any]:
    if CONFIG_CENTER_ENABLED == 'false':
        return {}
    else:
        vc = VaultClient(config('VAULT_URL'), config('VAULT_CRT'), config('VAULT_TOKEN'))
        return vc.get_from_vault(SRV_NAMESPACE)


class Settings(BaseSettings):
    """Store service configuration settings."""

    APP_NAME: str = 'service_download'
    VERSION: str = '1.1.0'
    port: int = 5077
    host: str = '127.0.0.1'
    env: str = 'test'
    namespace: str

    # disk mounts
    ROOT_PATH: str
    CORE_ZONE_LABEL: str
    GREEN_ZONE_LABEL: str

    # services
    DATAOPS_SERVICE: str
    DATASET_SERVICE: str
    METADATA_SERVICE: str
    PROJECT_SERVICE: str

    # minio
    # this endpoint is internal communication
    S3_INTERNAL: str
    S3_INTERNAL_HTTPS: bool = False
    S3_ACCESS_KEY: str
    S3_SECRET_KEY: str
    # this is for presigned url
    S3_PUBLIC: str
    # by default the minio public will be https
    # for local testing add the one to .env as False
    S3_PUBLIC_HTTPS: bool = True

    # download secret
    DOWNLOAD_KEY: str
    DOWNLOAD_TOKEN_EXPIRE_AT: int = 86400

    # Redis Service
    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_USER: str = 'default'
    REDIS_DB: int
    REDIS_PASSWORD: str

    # Postgres
    # TODO remove it after add approval service
    RDS_HOST: str
    RDS_PORT: int
    RDS_PWD: str
    RDS_USER: str
    RDS_DBNAME: str
    RDS_SCHEMA_DEFAULT: str

    # kafka
    # NOTE: KAFKA URL cannot start with http://
    KAFKA_URL: str
    KAFKA_ITEM_ACTIVITY_TOPIC: str = 'metadata.items.activity'
    KAFKA_DATASET_ACTIVITY_TOPIC: str = 'dataset.activity'

    OPEN_TELEMETRY_ENABLED: bool = False
    OPEN_TELEMETRY_HOST: str = '127.0.0.1'
    OPEN_TELEMETRY_PORT: int = 6831

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
        extra = Extra.allow

        @classmethod
        def customise_sources(cls, init_settings, env_settings, file_secret_settings):
            return env_settings, load_vault_settings, init_settings, file_secret_settings

    def __init__(self) -> None:
        super().__init__()

        # services
        self.DATAOPS_SERVICE_V2 = self.DATAOPS_SERVICE + '/v2/'
        self.DATAOPS_SERVICE = self.DATAOPS_SERVICE + '/v1/'
        self.DATASET_SERVICE = self.DATASET_SERVICE + '/v1/'
        self.METADATA_SERVICE = self.METADATA_SERVICE + '/v1/'

        # minio
        self.MINIO_TMP_PATH = self.ROOT_PATH + '/tmp/'

        # postgres
        self.RDS_DB_URI = (
            f'postgresql+asyncpg://{self.RDS_USER}:{self.RDS_PWD}@{self.RDS_HOST}:'
            + f'{self.RDS_PORT}/{self.RDS_DBNAME}'
        )

        # redis
        self.REDIS_URL = (
            f'redis://{self.REDIS_USER}:{self.REDIS_PASSWORD}@{self.REDIS_HOST}' + f':{self.REDIS_PORT}/{self.REDIS_DB}'
        )


ConfigClass = Settings()
