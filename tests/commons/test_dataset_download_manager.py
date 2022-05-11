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

from app.commons.download_manager.dataset_download_manager import (
    create_dataset_download_client,
)

pytestmark = pytest.mark.asyncio


async def test_download_client_without_files(httpx_mock, mock_minio):
    httpx_mock.add_response(
        method='GET',
        url='http://metadata_service/v1/items/search/?container_code=any_code'
        '&container_type=dataset&zone=0&recursive=true&archived=false&parent_path=&owner=me',
        json={'result': []},
    )

    download_client = await create_dataset_download_client(
        auth_token={'at': 'token', 'rt': 'refresh_token'},
        operator='me',
        container_code='any_code',
        container_type='project',
        session_id='1234',
    )

    assert len(download_client.files_to_zip) == 0


async def test_download_client_add_file(httpx_mock):
    httpx_mock.add_response(
        method='GET',
        url='http://metadata_service/v1/items/search/?container_code=any_code'
        '&container_type=dataset&zone=0&recursive=true&archived=false&parent_path=&owner=me',
        json={
            'result': [
                {
                    'code': 'any_code',
                    'labels': 'any_label',
                    'storage': {'location_uri': 'http://anything.com/bucket/obj/path'},
                    'id': 'geid_1',
                    'operator': 'me',
                    'parent_path': 'admin',
                    'type': 'file',
                    'container_code': 'fake_project_code',
                    'zone': 1,
                }
            ]
        },
    )

    download_client = await create_dataset_download_client(
        auth_token={'at': 'token', 'rt': 'refresh_token'},
        operator='me',
        container_code='any_code',
        container_type='project',
        session_id='1234',
    )

    assert len(download_client.files_to_zip) == 1
    assert download_client.files_to_zip[0].get('id') == 'geid_1'
