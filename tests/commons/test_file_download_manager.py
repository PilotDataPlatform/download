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

from unittest import mock

import minio
import pytest

from app.commons.download_manager.file_download_manager import FileDownloadClient
from app.commons.download_manager.file_download_manager import (
    create_file_download_client,
)
from app.models.models_data_download import EDataDownloadStatus
from app.resources.error_handler import APIException

pytestmark = pytest.mark.asyncio


async def test_download_client_without_files_should_raise_exception(httpx_mock, mock_minio):
    with pytest.raises(APIException):
        await create_file_download_client(
            files=[],
            auth_token={'at': 'token', 'rt': 'refresh_token'},
            operator='me',
            container_code='any_code',
            container_type='project',
            session_id='1234',
        )


async def test_download_client_add_file(httpx_mock):
    httpx_mock.add_response(
        method='GET',
        url='http://metadata_service/v1/item/geid_1/',
        json={
            'result': {
                'code': 'any_code',
                'labels': 'any_label',
                'storage': {'location_uri': 'http://anything.com/bucket/obj/path'},
                'id': 'geid_1',
                'operator': 'me',
                'parent_path': 'admin',
                'type': 'file',
                'container_code': 'fake_project_code',
                'zone': 0,
            }
        },
    )

    download_client = await create_file_download_client(
        files=[{'geid': 'geid_1'}],
        auth_token={'at': 'token', 'rt': 'refresh_token'},
        operator='me',
        container_code='any_code',
        container_type='project',
        session_id='1234',
    )

    assert len(download_client.files_to_zip) == 1
    assert download_client.files_to_zip[0].get('id') == 'geid_1'


async def test_zip_worker_set_status_READY_FOR_DOWNLOADING_when_success(httpx_mock, mock_minio):
    httpx_mock.add_response(
        method='GET',
        url='http://metadata_service/v1/item/geid_1/',
        json={
            'result': {
                'code': 'any_code',
                'labels': 'any_label',
                'storage': {'location_uri': 'http://anything.com/bucket/obj/path'},
                'id': 'geid_1',
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

    download_client = await create_file_download_client(
        files=[{'geid': 'geid_1'}],
        auth_token={'at': 'token', 'rt': 'refresh_token'},
        operator='me',
        container_code='any_code',
        container_type='project',
        session_id='1234',
    )
    with mock.patch.object(FileDownloadClient, 'set_status') as fake_set:
        await download_client.background_worker('fake_hash')
    fake_set.assert_called_once_with(EDataDownloadStatus.READY_FOR_DOWNLOADING, payload={'hash_code': 'fake_hash'})


async def test_zip_worker_set_status_CANCELLED_when_success(httpx_mock, mocker):
    httpx_mock.add_response(
        method='GET',
        url='http://metadata_service/v1/item/geid_1/',
        json={
            'result': {
                'code': 'any_code',
                'labels': 'any_label',
                'storage': {'location_uri': 'http://anything.com/bucket/obj/path'},
                'id': 'geid_1',
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
    # mock the exception
    m = mocker.patch(
        'app.commons.locks.bulk_lock_operation',
        return_value={},
    )
    m.side_effect = Exception

    download_client = await create_file_download_client(
        files=[{'geid': 'geid_1'}],
        auth_token='token',
        operator='me',
        container_code='any_code',
        container_type='project',
        session_id='1234',
    )
    with mock.patch.object(FileDownloadClient, 'set_status') as fake_set:
        await download_client.background_worker('fake_hash')
    fake_set.assert_called_once_with(
        EDataDownloadStatus.CANCELLED, payload={'error_msg': 'string indices must be integers'}
    )


@mock.patch('app.commons.service_connection.minio_client.Minio')
@pytest.mark.parametrize(
    'exception_code,result',
    [
        (
            'any',
            {
                'status': EDataDownloadStatus.CANCELLED,
                'payload': {
                    'error_msg': (
                        'S3 operation failed; code: any, message: any msg'
                        ', resource: any, request_id: any, host_id: any'
                    )
                },
            },
        ),
        ('NoSuchKey', {'status': EDataDownloadStatus.READY_FOR_DOWNLOADING, 'payload': {'hash_code': 'fake_hash'}}),
    ],
)
async def test_zip_worker_raise_exception_when_minio_return_error(mock_minio, httpx_mock, exception_code, result):
    httpx_mock.add_response(
        method='GET',
        url='http://metadata_service/v1/item/geid_1/',
        json={
            'result': {
                'code': 'any_code',
                'labels': 'any_label',
                'storage': {'location_uri': 'http://anything.com/bucket/obj/path'},
                'id': 'geid_1',
                'operator': 'me',
                'parent_path': 'admin',
                'type': 'file',
                'container_code': 'fake_project_code',
                'zone': 0,
            }
        },
    )
    httpx_mock.add_response(method='POST', url='http://data_ops_util/v2/resource/lock/bulk', status_code=200, json={})
    httpx_mock.add_response(method='DELETE', url='http://data_ops_util/v2/resource/lock/bulk', status_code=200, json={})
    minio_exception = minio.error.S3Error(
        code=exception_code, message='any msg', resource='any', request_id='any', host_id='any', response='error'
    )
    mock_minio().fget_object.side_effect = [minio_exception]

    download_client = await create_file_download_client(
        files=[{'geid': 'geid_1'}],
        auth_token={'at': 'token', 'rt': 'refresh_token'},
        operator='me',
        container_code='any_code',
        container_type='project',
        session_id='1234',
    )
    with mock.patch.object(FileDownloadClient, 'set_status') as fake_set:
        await download_client.background_worker('fake_hash')
    fake_set.assert_called_once_with(result['status'], payload=result['payload'])
