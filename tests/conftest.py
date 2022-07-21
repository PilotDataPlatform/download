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

import asyncio
import os
import shutil
import time
from io import BytesIO
from uuid import uuid4

import jwt
import pytest
import pytest_asyncio
import sqlalchemy
from aioredis import StrictRedis
from async_asgi_testclient import TestClient
from httpx import Response
from sqlalchemy import Column, MetaData, String, Table
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.asyncio import create_async_engine
from starlette.config import environ
from testcontainers.postgres import PostgresContainer
from urllib3 import HTTPResponse


@pytest.fixture(scope='session')
def db_postgres():
    with PostgresContainer('postgres:9.5') as postgres:
        yield postgres.get_connection_url().replace('+psycopg2', '+asyncpg')


@pytest_asyncio.fixture(scope='session')
async def engine(db_postgres):
    engine = create_async_engine(db_postgres)
    async with engine.begin() as conn:
        await conn.execute(sqlalchemy.schema.CreateSchema('INDOC_TEST'))
    yield engine
    await engine.dispose()


@pytest.fixture
async def metadata(engine):
    metadata = MetaData(schema='INDOC_TEST')
    table = Table(
        'approval_entity',
        metadata,
        Column('id', UUID(as_uuid=True), unique=True, primary_key=True, default=uuid4),
        Column('request_id', String()),
        Column('entity_type', String()),
        Column('review_status', String()),
        Column('name', String()),
        Column('entity_geid', String()),
    )
    async with engine.begin() as connection:
        await connection.run_sync(metadata.create_all)
        await connection.execute(
            table.insert().values(
                request_id='67e6bf62-be82-4401-9ec0-7d49ee047fe7', name='entity_name', entity_geid='fake_geid'
            )
        )
    yield metadata


@pytest.fixture(scope='session')
def event_loop(request):
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
    asyncio.set_event_loop_policy(None)


@pytest.fixture(autouse=True)
async def clean_up_redis():
    cache = StrictRedis(host=environ.get('REDIS_HOST'))
    await cache.flushall()


@pytest.fixture(scope='session', autouse=True)
def create_folders():
    folder_path = './tests/tmp/'
    os.makedirs(folder_path + 'any_id_1')
    yield
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)


@pytest.fixture(autouse=True)
def mock_settings(monkeypatch):
    from app.config import ConfigClass

    monkeypatch.setattr(ConfigClass, 'MINIO_TMP_PATH', './tests/tmp/')


@pytest.fixture
def file_folder_jwt_token():

    hash_token_dict = {
        'file_path': 'test/folder/file',
        'issuer': 'SERVICE DATA DOWNLOAD',
        'operator': 'test_user',
        'session_id': 'test_session_id',
        'job_id': 'test_job_id',
        'container_code': 'test_container',
        'container_type': 'test_type',
        'payload': {},
        'iat': int(time.time()),
        'exp': int(time.time()) + 10,
    }

    hash_code = jwt.encode(hash_token_dict, key=environ['DOWNLOAD_KEY'], algorithm='HS256').decode('utf-8')
    return hash_code


@pytest.fixture
def file_folder_jwt_token_expired():

    hash_token_dict = {
        'file_path': 'test/folder/file',
        'issuer': 'SERVICE DATA DOWNLOAD',
        'operator': 'test_user',
        'session_id': 'test_session_id',
        'job_id': 'test_job_id',
        'container_code': 'test_container',
        'container_type': 'test_type',
        'payload': {},
        'iat': int(time.time()),
        'exp': int(time.time()) - 10,
    }

    hash_code = jwt.encode(hash_token_dict, key=environ['DOWNLOAD_KEY'], algorithm='HS256').decode('utf-8')
    return hash_code


@pytest.fixture
def file_folder_jwt_token_invalid():

    hash_token_dict = {
        'issuer': 'SERVICE DATA DOWNLOAD',
        'operator': 'test_user',
        'session_id': 'test_session_id',
        'job_id': 'test_job_id',
        'container_code': 'test_container',
        'container_type': 'test_type',
        'payload': {},
        'iat': int(time.time()),
        'exp': int(time.time()) + 10,
    }

    hash_code = jwt.encode(hash_token_dict, key=environ['DOWNLOAD_KEY'], algorithm='HS256').decode('utf-8')
    return hash_code


@pytest.fixture
def dataset_download_jwt_token():

    hash_token_dict = {
        'location': 'test/folder/file',
        'iat': int(time.time()),
        'exp': int(time.time()) + 10,
    }

    hash_code = jwt.encode(hash_token_dict, key=environ['DOWNLOAD_KEY'], algorithm='HS256').decode('utf-8')
    return hash_code


@pytest.fixture
def anyio_backend():
    return 'asyncio'


@pytest.fixture
def app(anyio_backend, db_postgres):
    from app.config import ConfigClass
    from app.main import create_app

    ConfigClass.RDS_DB_URI = f'{db_postgres}?prepared_statement_cache_size=0'
    app = create_app()
    yield app


@pytest.fixture
async def client(app):
    return TestClient(app)


@pytest.fixture
def mock_boto3(monkeypatch):
    from common.object_storage_adaptor.boto3_client import Boto3Client

    class FakeObject:
        size = b'a'

    http_response = HTTPResponse()
    response = Response(status_code=200)
    response.raw = http_response
    response.raw._fp = BytesIO(b'File like object')

    async def fake_init_connection():
        pass

    async def fake_downlaod_object(x, y, z, z1):
        return response

    async def fake_get_download_presigned_url(x, y, z):
        return f'http://minio.minio:9000/{y}/{z}'

    monkeypatch.setattr(Boto3Client, 'init_connection', lambda x: fake_init_connection())
    monkeypatch.setattr(Boto3Client, 'downlaod_object', lambda x, y, z, z1: fake_downlaod_object(x, y, z, z1))
    monkeypatch.setattr(
        Boto3Client, 'get_download_presigned_url', lambda x, y, z: fake_get_download_presigned_url(x, y, z)
    )


@pytest.fixture
def mock_boto3_clients():
    from common.object_storage_adaptor.boto3_client import Boto3Client

    boto3_internal = Boto3Client('test_connection', access_key='test', secret_key='test')
    boto3_public = Boto3Client('test_connection', access_key='test', secret_key='test')

    return {'boto3_internal': boto3_internal, 'boto3_public': boto3_public}


@pytest.fixture
def mock_kafka_producer(monkeypatch):
    from app.commons.kafka_producer import KakfaProducer

    async def fake_init_connection():
        pass

    async def fake_send_message(x, y, z):
        pass

    async def fake_validate_message(x, y, z):
        pass

    async def fake_create_activity_log(x, y, z, z1):
        pass

    monkeypatch.setattr(KakfaProducer, 'init_connection', lambda x: fake_init_connection())
    monkeypatch.setattr(KakfaProducer, '_send_message', lambda x, y, z: fake_send_message(x, y, z))
    monkeypatch.setattr(KakfaProducer, '_validate_message', lambda x, y, z: fake_validate_message(x, y, z))
    monkeypatch.setattr(KakfaProducer, 'create_activity_log', lambda x, y, z, z1: fake_create_activity_log(x, y, z, z1))


@pytest.fixture
async def fake_job():
    from app.resources.helpers import set_status

    await set_status(
        'test_session_id',
        'test_job_id',
        'test/folder/file',
        'data_download',
        'PRE_UPLOADED',
        'test_container',
        'test_user',
        payload={
            'task_id': 'fake_global_entity_id',
        },
    )
