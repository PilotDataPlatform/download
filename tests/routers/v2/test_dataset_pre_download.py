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


async def test_v2_dataset_download_pre_return_500_when_query_not_found(
    client,
    httpx_mock,
):
    dataset_geid = 'fake_dataset_geid'
    httpx_mock.add_response(
        method='POST', url='http://neo4j_service/v2/neo4j/relations/query', json={}, status_code=404
    )
    httpx_mock.add_response(
        method='GET', url=f'http://neo4j_service/v1/neo4j/nodes/geid/{dataset_geid}', json=[], status_code=404
    )
    resp = await client.post(
        '/v2/dataset/download/pre', json={'session_id': 1234, 'operator': 'me', 'dataset_geid': dataset_geid}
    )
    assert resp.status_code == 500
    assert resp.json() == {
        'code': 500,
        'error_msg': 'Error when getting node for neo4j',
        'page': 0,
        'total': 1,
        'num_of_pages': 1,
        'result': [],
    }


async def test_v2_dataset_download_pre_return_200_when_success(
    client,
    httpx_mock,
):
    dataset_geid = 'fake_dataset_geid'
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v2/neo4j/relations/query',
        json={
            'results': [
                {
                    'code': 'any_code',
                    'labels': 'File',
                    'location': 'http://anything.com/bucket/obj/path',
                    'global_entity_id': 'fake_geid',
                    'project_code': '',
                    'operator': 'me',
                    'parent_folder': '',
                    'dataset_code': 'fake_dataset_code',
                }
            ]
        },
    )
    httpx_mock.add_response(
        method='GET',
        url=f'http://neo4j_service/v1/neo4j/nodes/geid/{dataset_geid}',
        json=[
            {
                'code': 'any_code',
                'labels': ['Folder'],
                'location': 'http://anything.com/bucket/obj/path',
                'global_entity_id': 'fake_geid',
                'project_code': '',
                'operator': 'me',
                'parent_folder': '',
                'dataset_code': 'fake_dataset_code',
            }
        ],
    )
    httpx_mock.add_response(
        method='GET',
        url='http://neo4j_service/v1/neo4j/nodes/geid/fake_geid',
        json=[
            {
                'code': 'any_code',
                'labels': ['File'],
                'location': 'http://anything.com/bucket/obj/path',
                'global_entity_id': 'fake_geid',
                'project_code': '',
                'operator': 'me',
                'parent_folder': '',
                'dataset_code': 'fake_dataset_code',
            }
        ],
    )

    httpx_mock.add_response(
        method='POST',
        url='http://queue_service/v1/broker/pub',
        json={},
    )
    resp = await client.post(
        '/v2/dataset/download/pre', json={'session_id': 1234, 'operator': 'me', 'dataset_geid': dataset_geid}
    )

    assert resp.status_code == 200
    result = resp.json()['result']
    assert resp.status_code == 200
    assert result['session_id'] == '1234'
    assert result['job_id']
    assert result['geid'] == 'fake_geid'
    assert result['project_code'] in result['source']
    assert result['action'] == 'data_download'
    assert result['status'] == 'ZIPPING'
    assert result['project_code'] == 'any_code'
    assert result['operator'] == 'me'
    assert result['progress'] == 0
    assert result['payload']['hash_code']


async def test_v2_dataset_download_pre_empty_dataset_return_200_when_success(
    client,
    httpx_mock,
):
    dataset_geid = 'fake_dataset_geid'
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v2/neo4j/relations/query',
        json={'results': []},
    )
    httpx_mock.add_response(
        method='GET',
        url=f'http://neo4j_service/v1/neo4j/nodes/geid/{dataset_geid}',
        json=[
            {
                'code': 'any_code',
                'labels': ['Folder'],
                'location': 'http://anything.com/bucket/obj/path',
                'global_entity_id': 'fake_geid',
                'project_code': '',
                'operator': 'me',
                'parent_folder': '',
                'dataset_code': 'fake_dataset_code',
            }
        ],
    )
    httpx_mock.add_response(
        method='POST',
        url='http://queue_service/v1/broker/pub',
        json={},
    )
    resp = await client.post(
        '/v2/dataset/download/pre', json={'session_id': 1234, 'operator': 'me', 'dataset_geid': dataset_geid}
    )

    assert resp.status_code == 200
    result = resp.json()['result']
    assert resp.status_code == 200
    assert result['session_id'] == '1234'
    assert result['job_id']
    assert result['geid'] == 'fake_dataset_geid'
    assert result['project_code'] in result['source']
    assert result['action'] == 'data_download'
    assert result['status'] == 'ZIPPING'
    assert result['project_code'] == 'any_code'
    assert result['operator'] == 'me'
    assert result['progress'] == 0
    assert result['payload']['hash_code']
