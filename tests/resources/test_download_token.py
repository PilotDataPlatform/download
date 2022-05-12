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

from app.resources.download_token_manager import InvalidToken
from app.resources.download_token_manager import generate_token
from app.resources.download_token_manager import verify_download_token


@pytest.mark.asyncio
async def test_token_flow():
    '''
    Summary:
        the test will use encode and decode at same time to make sure
        the hash code is properly generated and no data loss
    '''

    container_code = 'test_container'
    container_type = 'test_type'
    file_path = 'tests/routers/v1/empty.txt'
    operator = 'test_user'
    session_id = 'test_session_id'
    job_id = 'test_job_id'
    payload = {'test_payload': 'test_value'}

    hash_code = await generate_token(container_code, container_type, file_path, operator, session_id, job_id, payload)

    decoded_payload = await verify_download_token(hash_code)

    assert decoded_payload.get('container_code') == container_code
    assert decoded_payload.get('container_type') == container_type
    assert decoded_payload.get('file_path') == file_path
    assert decoded_payload.get('operator') == operator
    assert decoded_payload.get('session_id') == session_id
    assert decoded_payload.get('job_id') == job_id
    assert decoded_payload.get('payload') == payload


@pytest.mark.asyncio
async def test_token_expired():
    hash_token_dict = {
        'file_path': 'test/folder/file',
        'issuer': 'SERVICE DATA DOWNLOAD',
        'operator': 'test_user',
        'session_id': 'test_session_id',
        'job_id': 'test_job_id',
        'container_code': 'test_container',
        'container_type': 'test_type',
        'payload': {},
        'iat': int(time.time()) - 100,
        'exp': int(time.time()) - 100,
    }

    hash_code = jwt.encode(hash_token_dict, key='test_key', algorithm='HS256').decode('utf-8')

    try:
        verify_download_token(hash_code)
    except Exception as e:
        assert type(e) == jwt.ExpiredSignatureError


@pytest.mark.asyncio
async def test_token_invalid():
    hash_token_dict = {
        # 'file_path': "test/folder/file", << here remove the file_path
        'issuer': 'SERVICE DATA DOWNLOAD',
        'operator': 'test_user',
        'session_id': 'test_session_id',
        'job_id': 'test_job_id',
        'container_code': 'test_container',
        'container_type': 'test_type',
        'payload': {},
        'iat': int(time.time()),
        'exp': int(time.time()) + 100,
    }

    hash_code = jwt.encode(hash_token_dict, key='test_key', algorithm='HS256').decode('utf-8')

    try:
        verify_download_token(hash_code)
    except Exception as e:
        assert type(e) == InvalidToken


@pytest.mark.asyncio
async def test_token_wrong_key():
    hash_token_dict = {
        'file_path': 'test/folder/file',  # <<here we have path
        'issuer': 'SERVICE DATA DOWNLOAD',
        'operator': 'test_user',
        'session_id': 'test_session_id',
        'job_id': 'test_job_id',
        'container_code': 'test_container',
        'container_type': 'test_type',
        'payload': {},
        'iat': int(time.time()),
        'exp': int(time.time()) + 100,
    }

    # but verified with wrong key
    hash_code = jwt.encode(hash_token_dict, key='test_key', algorithm='HS256').decode('utf-8')

    try:
        verify_download_token(hash_code)
    except Exception as e:
        assert type(e) == InvalidToken
