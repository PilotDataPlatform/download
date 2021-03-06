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

import pytest
from common import ProjectNotFoundException

pytestmark = pytest.mark.asyncio


async def test_v2_download_pre_return_422_when_container_code_and_type_are_missing(client):
    resp = await client.post('/v2/download/pre/', json={'session_id': '123', 'operator': 'me', 'files': [{}]})

    assert resp.status_code == 422


async def test_v2_download_pre_return_404_when_project_not_exist(
    client,
    httpx_mock,
    mocker,
):

    m = mocker.patch('common.ProjectClient.get', return_value={})
    m.side_effect = ProjectNotFoundException

    resp = await client.post(
        '/v2/download/pre/',
        json={
            'session_id': '123',
            'operator': 'me',
            'project_id': 'any',
            'container_code': 'fake_geid',
            'container_type': 'project',
            'files': [{'id': 'fake_geid'}],
        },
    )
    assert resp.status_code == 404
    assert resp.json() == {
        'code': 404,
        'error_msg': 'Project not found',
        'page': 0,
        'total': 1,
        'num_of_pages': 1,
        'result': [],
    }


async def test_v2_download_pre_return_404_when_fail_to_add_files_to_zip(
    client,
    httpx_mock,
    mocker,
):

    mocker.patch('common.ProjectClient.get', return_value={'any': 'any', 'global_entity_id': 'fake_global_entity_id'})

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
            'container_code': 'fake_geid',
            'container_type': 'project',
            'files': [{'id': 'fake_geid'}],
        },
    )
    assert resp.status_code == 404
    assert resp.json() == {
        'code': 404,
        'error_msg': 'resource fake_geid does not exist',
        'page': 0,
        'total': 1,
        'num_of_pages': 1,
        'result': [],
    }


async def test_v2_download_pre_return_200_when_approval_request_id(
    client,
    httpx_mock,
    metadata,
    mock_boto3,
    mocker,
    mock_kafka_producer,
):

    mocker.patch('common.ProjectClient.get', return_value={'any': 'any', 'global_entity_id': 'fake_global_entity_id'})

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
                'container_type': 'project',
                'zone': 0,
                'name': 'test_item',
            }
        },
    )

    httpx_mock.add_response(method='POST', url='http://dataops_service/v2/resource/lock/bulk', json={}, status_code=200)
    httpx_mock.add_response(
        method='DELETE', url='http://dataops_service/v2/resource/lock/bulk', json={}, status_code=200
    )

    resp = await client.post(
        '/v2/download/pre/',
        json={
            'operator': 'me',
            'project_id': 'any',
            'container_code': 'fake_code',
            'container_type': 'project',
            'files': [{'id': 'fake_geid'}],
            'approval_request_id': '67e6bf62-be82-4401-9ec0-7d49ee047fe7',
        },
    )

    assert resp.status_code == 200
    result = resp.json()['result']
    assert result['job_id']
    assert 'obj/path' in result['source']
    assert result['action'] == 'data_download'
    assert result['status'] == 'ZIPPING'
    assert result['project_code'] == 'fake_code'
    assert result['operator'] == 'me'
    assert result['payload']['hash_code']


async def test_v2_download_pre_return_200_when_label_is_not_Folder(
    client,
    httpx_mock,
    metadata,
    mock_boto3,
    mocker,
    mock_kafka_producer,
):

    mocker.patch('common.ProjectClient.get', return_value={'any': 'any', 'global_entity_id': 'fake_global_entity_id'})

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
                'container_type': 'project',
                'zone': 0,
                'name': 'test_item',
            }
        },
    )

    httpx_mock.add_response(method='POST', url='http://dataops_service/v2/resource/lock/bulk', json={}, status_code=200)
    httpx_mock.add_response(
        method='DELETE', url='http://dataops_service/v2/resource/lock/bulk', json={}, status_code=200
    )

    resp = await client.post(
        '/v2/download/pre/',
        json={
            'session_id': '123',
            'operator': 'me',
            'project_id': 'any',
            'container_code': 'any_project_code',
            'container_type': 'project',
            'files': [{'id': 'fake_geid'}],
        },
    )

    assert resp.status_code == 200
    result = resp.json()['result']
    assert result['job_id']
    assert 'obj/path' in result['source']
    assert result['action'] == 'data_download'
    assert result['status'] == 'ZIPPING'
    assert result['project_code'] == 'any_project_code'
    assert result['operator'] == 'me'
    assert result['payload']['hash_code']


async def test_v2_download_pre_return_200_when_type_is_Folder(
    client,
    httpx_mock,
    metadata,
    mock_boto3,
    mocker,
    mock_kafka_producer,
):

    mocker.patch('common.ProjectClient.get', return_value={'any': 'any', 'global_entity_id': 'fake_global_entity_id'})

    httpx_mock.add_response(
        method='GET',
        url='http://metadata_service/v1/item/fake_geid/',
        json={
            'result': {
                'storage': {'location_uri': 'http://anything.com/bucket/obj/path'},
                'id': 'fake_geid',
                'type': 'folder',
                'owner': 'me',
                'parent_path': 'admin',
                'container_code': 'fake_project_code',
                'zone': 0,
                'name': 'fake_file',
            }
        },
    )

    container_code = 'fake_project_code'
    httpx_mock.add_response(
        method='GET',
        url='http://metadata_service/v1/items/search/?container_code=fake_project_code'
        '&container_type=project&zone=0&recursive=true&archived=false&parent_path=admi'
        'n.fake_file&owner=me&type=file',
        json={
            'result': [
                {
                    'storage': {'location_uri': f'http://anything.com/{container_code}/obj/path'},
                    'id': 'fake_geid',
                    'type': 'file',
                    'owner': 'me',
                    'parent_path': 'admin',
                    'container_code': container_code,
                    'container_type': 'project',
                    'zone': 0,
                    'name': 'test_item_2',
                }
            ]
        },
    )

    mocker.patch('app.commons.download_manager.file_download_manager.FileDownloadClient._zip_worker', return_value={})

    httpx_mock.add_response(method='POST', url='http://dataops_service/v2/resource/lock/bulk', json={}, status_code=200)
    httpx_mock.add_response(
        method='DELETE', url='http://dataops_service/v2/resource/lock/bulk', json={}, status_code=200
    )

    resp = await client.post(
        '/v2/download/pre/',
        json={
            'session_id': '123',
            'operator': 'me',
            'project_id': 'any',
            'container_code': container_code,
            'container_type': 'project',
            'files': [{'id': 'fake_geid'}],
        },
    )

    assert resp.status_code == 200
    result = resp.json()['result']
    assert result['job_id']
    assert result['project_code'] in result['source']
    assert result['action'] == 'data_download'
    assert result['status'] == 'ZIPPING'
    assert result['project_code'] == container_code
    assert result['operator'] == 'me'
    assert result['payload']['hash_code']
