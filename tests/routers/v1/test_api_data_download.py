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

import pytest

pytestmark = pytest.mark.asyncio


async def test_v1_download_status_should_return_401_when_when_token_not_verified(client):
    resp = await client.get(
        '/v1/download/status/bad_token',
    )
    assert resp.status_code == 401
    assert resp.json() == {
        'code': 401,
        'error_msg': '[Invalid Token] Not enough segments',
        'page': 0,
        'total': 1,
        'num_of_pages': 1,
        'result': None,
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
    assert result['session_id'] == '123'
    assert result['job_id'] == 'fake_global_entity_id'
    assert result['source'] == 'tests/routers/v1/empty.txt'
    assert result['action'] == 'data_download'
    assert result['status'] == 'PRE_UPLOADED'
    assert result['operator'] == 'me'
    assert result['payload']['task_id'] == 'fake_global_entity_id'
    assert result['payload']['resumable_identifier'] == 'fake_global_entity_id'


async def test_v1_download_should_return_401_when_invalid_token(client, jwt_token, fake_job):
    resp = await client.get(
        '/v1/download/bad_token',
    )
    assert resp.status_code == 401
    assert resp.json() == {
        'code': 401,
        'error_msg': '[Invalid Token] Not enough segments',
        'page': 0,
        'total': 1,
        'num_of_pages': 1,
        'result': None,
    }


async def test_v1_download_should_return_404_when_path_not_found(client):
    token = (
        'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIi'
        'OiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0I'
        'joxNTE2MjM5MDIyLCJmdWxsX3BhdGgiOiJhbnkiLCJzZXNzaW'
        '9uX2lkIjoxMjMsImpvYl9pZCI6ImZha2VfZ2xvYmFsX2VudGl'
        '0eV9pZCIsInByb2plY3RfY29kZSI6ImFueSIsIm9wZXJhdG9y'
        'IjoibWUiLCJnZWlkIjoiZmFrZV9nbG9iYWxfZW50aXR5X2lkI'
        'n0.l64K1z0ppK8X99G5CgEPJMR544xCcqsHBvMIyKiRWrI'
    )

    resp = await client.get(
        f'/v1/download/{token}',
    )
    assert resp.status_code == 404
    assert resp.json() == {
        'code': 404,
        'error_msg': '[File not found] any.',
        'page': 0,
        'total': 1,
        'num_of_pages': 1,
        'result': None,
    }


async def test_v1_download_should_return_200_when_success(
    client,
    jwt_token,
    fake_job,
    httpx_mock,
):
    httpx_mock.add_response(method='POST', url='http://provenance_service/v1/audit-logs', json={}, status_code=200)
    resp = await client.get(
        f'/v1/download/{jwt_token}',
    )
    assert resp.status_code == 200
    assert resp.text == 'file content\n'
