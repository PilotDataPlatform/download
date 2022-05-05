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


async def test_v2_download_pre_return_400_when_project_code_and_dataset_geid_are_missing(client):
    resp = await client.post('/v2/download/pre/', json={'session_id': '123', 'operator': 'me', 'files': [{}]})

    assert resp.status_code == 400
    assert resp.json() == {
        'code': 400,
        'error_msg': 'project_code or dataset_geid required',
        'page': 0,
        'total': 1,
        'num_of_pages': 1,
        'result': [],
    }


async def test_v2_download_pre_return_500_when_fail_to_add_files_to_zip(
    client,
    httpx_mock,
):
    httpx_mock.add_response(
        method='GET',
        url='http://metadata_service/v1/item/fake_geid/',
        json={},
        status_code=404,
    )

    resp = await client.post(
        '/v2/download/pre/',
        json={
            'session_id': '123',
            'operator': 'me',
            'project_id': 'any',
            'dataset_geid': 'fake_geid',
            'project_code': 'any_project_code',
            'files': [{'geid': 'fake_geid'}],
        },
    )
    assert resp.status_code == 500
    assert resp.json() == {
        'code': 500,
        'error_msg': '[Internal] api_data_download File fake_geid does not exist',
        'page': 0,
        'total': 1,
        'num_of_pages': 1,
        'result': None,
    }


async def test_v2_download_pre_return_200_when_approval_request_id(client, httpx_mock, metadata):
    httpx_mock.add_response(
        method='GET',
        url='http://metadata_service/v1/item/fake_geid/',
        json={
            'result': {
                'code': 'any_code',
                'labels': 'any_label',
                'storage': {'location_uri': 'http://anything.com/bucket/obj/path'},
                'id': 'fake_geid',
                'operator': 'me',
                'parent_path': 'admin',
                'type': 'file',
                'container_code': 'fake_project_code',
                'zone': 0,
            }
        },
    )

    httpx_mock.add_response(method='POST', url='http://data_ops_util/v2/resource/lock/bulk', json={}, status_code=200)
    httpx_mock.add_response(method='DELETE', url='http://data_ops_util/v2/resource/lock/bulk', json={}, status_code=200)

    resp = await client.post(
        '/v2/download/pre/',
        json={
            'session_id': '123',
            'operator': 'me',
            'project_id': 'any',
            'project_code': 'fake_code',
            'files': [{'geid': 'fake_geid'}],
            'approval_request_id': '67e6bf62-be82-4401-9ec0-7d49ee047fe7',
        },
    )
    result = resp.json()['result']
    assert resp.status_code == 200
    assert result['session_id'] == '123'
    assert result['job_id']
    assert result['geid'] == 'fake_geid'
    assert 'obj/path' in result['source']
    assert result['action'] == 'data_download'
    assert result['status'] == 'ZIPPING'
    assert result['project_code'] == 'fake_code'
    assert result['operator'] == 'me'
    assert result['progress'] == 0
    assert result['payload']['hash_code']


async def test_v2_download_pre_return_200_when_label_is_not_Folder(
    client,
    httpx_mock,
):
    httpx_mock.add_response(
        method='GET',
        url='http://metadata_service/v1/item/fake_geid/',
        json={
            'result': {
                'code': 'any_code',
                'labels': 'any_label',
                'storage': {'location_uri': 'http://anything.com/bucket/obj/path'},
                'id': 'fake_geid',
                'operator': 'me',
                'parent_path': 'admin',
                'type': 'file',
                'container_code': 'fake_project_code',
                'zone': 0,
            }
        },
    )

    httpx_mock.add_response(method='POST', url='http://data_ops_util/v2/resource/lock/bulk', json={}, status_code=200)
    httpx_mock.add_response(method='DELETE', url='http://data_ops_util/v2/resource/lock/bulk', json={}, status_code=200)

    resp = await client.post(
        '/v2/download/pre/',
        json={
            'session_id': '123',
            'operator': 'me',
            'project_id': 'any',
            'project_code': 'any_project_code',
            'files': [{'geid': 'fake_geid'}],
        },
    )
    result = resp.json()['result']
    assert resp.status_code == 200
    assert result['session_id'] == '123'
    assert result['job_id']
    assert result['geid'] == 'fake_geid'
    assert 'obj/path' in result['source']
    assert result['action'] == 'data_download'
    assert result['status'] == 'ZIPPING'
    assert result['project_code'] == 'any_project_code'
    assert result['operator'] == 'me'
    assert result['progress'] == 0
    assert result['payload']['hash_code']


async def test_v2_download_pre_return_200_when_label_is_Folder(
    client,
    httpx_mock,
):
    httpx_mock.add_response(
        method='GET',
        url='http://metadata_service/v1/item/fake_geid/',
        json={
            'result': {
                'code': 'any_code',
                'labels': 'any_label',
                'storage': {'location_uri': 'http://anything.com/bucket/obj/path'},
                'id': 'fake_geid',
                'type': 'folder',
                'operator': 'me',
                'parent_path': 'admin',
                'container_code': 'fake_project_code',
                'zone': 0,
                'name': 'fake_file',
            }
        },
    )

    httpx_mock.add_response(
        method='GET',
        url='http://metadata_service/v1/items/search/?container_code=fake_project_code&zone=0&'
        'recursive=true&archived=false&parent_path=admin.fake_file&owner=me',
        json={
            'results': [
                {
                    'code': 'any_code',
                    'labels': 'any_label',
                    'storage': {'location_uri': 'http://anything.com/bucket/obj/path'},
                    'id': 'fake_geid',
                    'type': 'file',
                    'operator': 'me',
                    'parent_path': 'admin',
                    'container_code': 'fake_project_code',
                    'zone': 0,
                }
            ]
        },
    )

    httpx_mock.add_response(method='POST', url='http://data_ops_util/v2/resource/lock/bulk', json={}, status_code=200)
    httpx_mock.add_response(method='DELETE', url='http://data_ops_util/v2/resource/lock/bulk', json={}, status_code=200)

    resp = await client.post(
        '/v2/download/pre/',
        json={
            'session_id': '123',
            'operator': 'me',
            'project_id': 'any',
            'project_code': 'any_project_code',
            'files': [{'geid': 'fake_geid'}],
        },
    )
    result = resp.json()['result']
    assert resp.status_code == 200
    assert result['session_id'] == '123'
    assert result['job_id']
    assert result['geid'] == 'fake_geid'
    assert result['project_code'] in result['source']
    assert result['action'] == 'data_download'
    assert result['status'] == 'ZIPPING'
    assert result['project_code'] == 'any_code'
    assert result['operator'] == 'me'
    assert result['progress'] == 0
    assert result['payload']['hash_code']
