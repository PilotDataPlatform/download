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


async def test_v2_dataset_download_should_return_401_when_wrong_token(
    client,
):
    resp = await client.get(
        '/v2/dataset/download/bad_token',
    )
    assert resp.status_code == 401
    assert resp.json() == {'code': 401, 'error_msg': 'n', 'page': 0, 'total': 1, 'num_of_pages': 1, 'result': []}


async def test_v2_dataset_download_should_return_200_when_success(client, jwt_token, mock_minio):
    resp = await client.get(
        f'/v2/dataset/download/{jwt_token}',
    )
    assert resp.status_code == 200
    assert resp.text == 'File like object'


async def test_v2_dataset_download_should_return_200_when_minio_raise_error(client, jwt_token, httpx_mock):
    resp = await client.get(
        f'/v2/dataset/download/{jwt_token}',
    )
    assert resp.status_code == 200
    assert 'error_msg' in resp.json()
    assert 'Error getting file from minio' in resp.json()['error_msg']
