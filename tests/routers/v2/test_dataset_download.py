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


async def test_v2_dataset_download_should_return_200_when_success(client, dataset_download_jwt_token, mock_boto3):
    resp = await client.get(
        f'/v2/dataset/download/{dataset_download_jwt_token}', headers={'Authorization': 'test'}, allow_redirects=False
    )

    assert resp.status_code == 200


async def test_v2_dataset_download_should_return_200_when_minio_raise_error(
    client, dataset_download_jwt_token, httpx_mock, mock_boto3, mocker
):

    m = mocker.patch(
        'common.object_storage_adaptor.boto3_client.Boto3Client.get_download_presigned_url', return_value=[]
    )
    m.side_effect = Exception()

    resp = await client.get(f'/v2/dataset/download/{dataset_download_jwt_token}', allow_redirects=False)

    assert resp.status_code == 400
    assert 'error_msg' in resp.json()
    assert 'Error getting file:' in resp.json()['error_msg']
