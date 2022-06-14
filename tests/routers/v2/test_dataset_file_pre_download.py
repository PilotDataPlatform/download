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


async def test_v2_dataset_download_pre_return_200_when_success(client, httpx_mock, mock_minio, mocker):
    dataset_code = 'fake_project_code'

    httpx_mock.add_response(
        method='GET',
        url='http://metadata_service/v1/items/search/?container_code=fake_project_code&'
        'container_type=dataset&zone=1&recursive=true&archived=false&parent_path=&owner'
        '=me&type=file',
        json={
            'result': [
                {
                    'storage': {'location_uri': 'http://anything.com/bucket/obj/path'},
                    'id': 'fake_geid',
                    'operator': 'me',
                    'parent_path': 'admin',
                    'type': 'file',
                    'container_code': 'fake_project_code',
                    'container_type': 'dataset',
                    'zone': 0,
                }
            ]
        },
    )

    httpx_mock.add_response(method='POST', url='http://data_ops_util/v2/resource/lock/bulk', json={}, status_code=200)
    httpx_mock.add_response(method='DELETE', url='http://data_ops_util/v2/resource/lock/bulk', json={}, status_code=200)

    # temporary add the this mock. Remove it after migration
    httpx_mock.add_response(
        method='GET',
        url='http://dataset_service/v1/dataset-peek/' + dataset_code,
        status_code=200,
        json={'result': {'id': 'fake_id'}},
    )

    mocker.patch(
        'app.commons.download_manager.dataset_download_manager.DatasetDownloadClient.add_schemas', return_value=[]
    )
    mocker.patch(
        'app.commons.download_manager.dataset_download_manager.DatasetDownloadClient._zip_worker', return_value=[]
    )

    httpx_mock.add_response(
        method='POST',
        url='http://queue_service/v1/broker/pub',
        json={},
    )
    resp = await client.post(
        '/v2/dataset/download/pre', json={'session_id': 1234, 'operator': 'me', 'dataset_code': dataset_code}
    )

    assert resp.status_code == 200
    result = resp.json()['result']

    assert result['job_id']
    assert result['project_code'] in result['source']
    assert result['action'] == 'data_download'
    assert result['status'] == 'ZIPPING'
    assert result['project_code'] == dataset_code
    assert result['operator'] == 'me'
    assert result['payload']['hash_code']


async def test_v2_dataset_download_pre_empty_dataset_return_200_when_success(client, httpx_mock, mocker):
    dataset_code = 'fake_project_code'
    httpx_mock.add_response(
        method='GET',
        url='http://metadata_service/v1/items/search/?container_code=fake_project_code'
        '&container_type=dataset&zone=1&recursive=true&archived=false&parent_path=&own'
        'er=me&type=file',
        json={'result': []},
    )

    httpx_mock.add_response(method='POST', url='http://data_ops_util/v2/resource/lock/bulk', json={}, status_code=200)
    httpx_mock.add_response(method='DELETE', url='http://data_ops_util/v2/resource/lock/bulk', json={}, status_code=200)

    # temporary add the this mock. Remove it after migration
    httpx_mock.add_response(
        method='GET',
        url='http://dataset_service/v1/dataset-peek/' + dataset_code,
        status_code=200,
        json={'result': {'id': 'fake_id'}},
    )

    mocker.patch(
        'app.commons.download_manager.dataset_download_manager.DatasetDownloadClient.add_schemas', return_value=[]
    )
    mocker.patch(
        'app.commons.download_manager.dataset_download_manager.DatasetDownloadClient._zip_worker', return_value=[]
    )

    httpx_mock.add_response(
        method='POST',
        url='http://queue_service/v1/broker/pub',
        json={},
    )
    resp = await client.post(
        '/v2/dataset/download/pre', json={'session_id': 1234, 'operator': 'me', 'dataset_code': dataset_code}
    )

    assert resp.status_code == 200
    result = resp.json()['result']

    assert result['job_id']
    assert result['project_code'] in result['source']
    assert result['action'] == 'data_download'
    assert result['status'] == 'ZIPPING'
    assert result['project_code'] == dataset_code
    assert result['operator'] == 'me'
    assert result['payload']['hash_code']
