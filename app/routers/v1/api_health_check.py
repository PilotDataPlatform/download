from typing import Tuple

import httpx
from common import LoggerFactory
from fastapi import APIRouter
from fastapi_utils import cbv
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import create_async_engine

from app.commons.data_providers.redis import SrvRedisSingleton
from app.commons.kafka_producer import get_kafka_producer
from app.config import ConfigClass
from app.models.base_models import APIResponse, EAPIResponseCode
from app.resources.error_handler import catch_internal

router = APIRouter()

_API_TAG = 'v1/data-download'
_API_NAMESPACE = 'api_health_check'


@cbv.cbv(router)
class APIHealthCheck:
    """API Data Download Class."""

    def __init__(self) -> None:
        self.__logger = LoggerFactory('api_data_download_v2').get_logger()

    async def _check_redis(self) -> Tuple[bool, dict]:
        """
        Summary:
            the function is to check if redis is available by `ping()`
            if cannot connect to redis, the function will return error
            otherwise will return online
        Return:
            - Bool, {"Redis": status}
        """

        try:
            redis_client = SrvRedisSingleton()
            if await redis_client.ping():
                return True, {'Redis': 'Online'}
            else:
                return False, {'Redis': 'Fail'}
        except Exception as e:
            return False, {'Redis': 'Fail with error: %s' % (str(e))}

    async def _check_minio(self) -> Tuple[bool, dict]:
        """
        Summary:
            the function is to check if minio is available.
            it uses the minio health check endpoint for cluster.
            For more infomation, check document:
            https://github.com/minio/minio/blob/master/docs/metrics/healthcheck/README.md
        Return:
            - Bool, {"Minio": status}
        """

        http_protocal = 'https://' if ConfigClass.S3_INTERNAL_HTTPS else 'http://'
        url = http_protocal + ConfigClass.S3_INTERNAL + '/minio/health/cluster'

        try:
            async with httpx.AsyncClient() as client:
                res = await client.get(url)

                if res.status_code != 200:
                    return False, {'Minio': 'Cluster unavailable'}
        except Exception as e:
            return False, {'Minio': 'Fail with error: %s' % (str(e))}

        return True, {'Minio': 'Online'}

    async def _check_RDS(self) -> Tuple[bool, dict]:
        """
        Summary:
            the function is to if target database table is
            created
        Return:
            - bool, {"RDS": status}
        """

        try:
            engine = create_async_engine(ConfigClass.RDS_DB_URI)
            metadata = MetaData(schema=ConfigClass.RDS_SCHEMA_DEFAULT)
            async with engine.connect() as conn:
                await conn.run_sync(metadata.reflect, only=['approval_entity'])

            if metadata.tables:
                return True, {'RDS': 'Online'}
            else:
                raise ValueError('RDS table approval_entity not found')
        except Exception as e:
            return False, {'RDS': 'Fail with error: %s' % (str(e))}

    async def _check_kafka(self) -> Tuple[bool, dict]:
        """
        Summary:
            the function is to check if kafka is available.
            this will just check if we successfully init the
            kafka producer
        Return:
            - bool, {"Kafka": status}
        """

        kafka_connection = await get_kafka_producer()
        if kafka_connection.connected is False:
            return False, {'Kafka': 'Unavailable'}

        return True, {'Kafka': 'Online'}

    @router.post(
        '/health',
        tags=[_API_TAG],
        summary='the api to check if databases are connected',
    )
    @catch_internal(_API_NAMESPACE)
    async def data_pre_download(self):
        response = APIResponse()
        response.code = EAPIResponseCode
        check_func = [self._check_kafka, self._check_minio, self._check_RDS, self._check_redis]

        connection_info = {}
        for f in check_func:
            status, detail = f()
            connection_info.update(detail)

            # if anyone of connection is failed, return 503
            # otherwise return 204

        return
