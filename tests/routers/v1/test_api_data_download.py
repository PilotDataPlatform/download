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

import time

import jwt
import pytest

from app.config import ConfigClass

pytestmark = pytest.mark.asyncio


async def test_v1_download_status_should_return_500_when_when_token_not_verified(client):
    resp = await client.get(
        '/v1/download/status/bad_token',
    )
    assert resp.status_code == 500
    assert resp.json() == {
        'code': 500,
        'error_msg': 'Not enough segments',
        'page': 0,
        'total': 1,
        'num_of_pages': 1,
        'result': [],
    }


async def test_v1_download_status_should_return_404_when_job_not_found(client, jwt_token):
    resp = await client.get(
        f'/v1/download/status/{jwt_token}',
    )
    assert resp.status_code == 404
    assert resp.json() == {
        'code': 404,
        'error_msg': '[Invalid Job ID] Not Found',
        'page': 0,
        'total': 1,
        'num_of_pages': 1,
        'result': [],
    }


async def test_v1_download_status_should_return_200_when_success(
    client,
    jwt_token,
    fake_job,
):
    resp = await client.get(
        f'/v1/download/status/{jwt_token}',
    )
    assert resp.status_code == 200
    result = resp.json()['result']
    assert result['session_id'] == 'test_session_id'
    assert result['job_id'] == 'test_job_id'
    assert result['source'] == 'test/folder/file'
    assert result['action'] == 'data_download'
    assert result['status'] == 'PRE_UPLOADED'
    assert result['operator'] == 'test_user'
    assert result['payload']['task_id'] == 'fake_global_entity_id'


async def test_v1_download_should_return_500_when_invalid_token(client, jwt_token, fake_job):
    resp = await client.get(
        '/v1/download/bad_token',
    )
    assert resp.status_code == 500
    assert resp.json() == {
        'code': 500,
        'error_msg': 'Not enough segments',
        'page': 0,
        'total': 1,
        'num_of_pages': 1,
        'result': [],
    }


async def test_v1_download_should_return_404_when_path_not_found(client, jwt_token):

    resp = await client.get(
        f'/v1/download/{jwt_token}',
    )
    assert resp.status_code == 404
    assert resp.json() == {
        'code': 404,
        'error_msg': '[File not found] test/folder/file.',
        'page': 0,
        'total': 1,
        'num_of_pages': 1,
        'result': [],
    }


async def test_v1_download_should_return_200_when_success(
    client,
    fake_job,
    httpx_mock,
):
    hash_token_dict = {
        'file_path': 'tests/routers/v1/empty.txt',
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

    hash_code = jwt.encode(hash_token_dict, key=ConfigClass.DOWNLOAD_KEY, algorithm='HS256').decode('utf-8')

    httpx_mock.add_response(method='POST', url='http://provenance_service/v1/audit-logs', json={}, status_code=200)
    resp = await client.get(
        f'/v1/download/{hash_code}',
    )
    assert resp.status_code == 200
    assert resp.text == 'file content\n'
