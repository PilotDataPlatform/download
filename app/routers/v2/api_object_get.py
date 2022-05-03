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

import shutil
import time

import httpx
from common import LoggerFactory
from fastapi import APIRouter
from fastapi import Header
from fastapi.responses import FileResponse
from fastapi.responses import StreamingResponse
from fastapi_utils import cbv

from app.commons.service_connection.minio_client import get_minio_client
from app.config import ConfigClass
from app.models.base_models import APIResponse
from app.resources.error_handler import catch_internal
from app.resources.helpers import get_files_recursive

router = APIRouter()

_API_TAG = 'v2/data-download'
_API_NAMESPACE = 'api_data_download'


@cbv.cbv(router)
class APIObjectGet:
    """API Object Get Class."""

    def __init__(self):
        self.__logger = LoggerFactory('api_data_download').get_logger()

    @router.get('/object/{obj_geid}', tags=[_API_TAG], summary='zip as a package if more than 1 file')
    @catch_internal(_API_NAMESPACE)
    async def get_object(self, obj_geid, Authorization: str = Header(None), refresh_token: str = Header(None)):
        """Get Object API."""
        # pass the access and refresh token to minio operation
        auth_token = {'at': Authorization, 'rt': refresh_token}
        zip_list = []
        query = {'global_entity_id': obj_geid}
        entity_type = 'File'
        async with httpx.AsyncClient() as client:
            resp = await client.post(ConfigClass.NEO4J_SERVICE + 'nodes/File/query', json=query)
        # if not resp, consider it as a Folder
        json_respon = resp.json()
        if not json_respon:
            entity_type = 'Folder'
        # handle file stream
        if entity_type == 'Folder':
            zip_list = await pack_zip_list(self.__logger, obj_geid)
            return await folder_stream(self.__logger, zip_list, obj_geid, auth_token)
        elif entity_type == 'File':
            file_node = json_respon[0]
            return await file_stream(self.__logger, file_node, auth_token)


async def file_stream(__logger, file_node, auth_token):
    try:
        location = file_node['location']
        minio_path = location.split('//')[-1]
        _, bucket, file_path = tuple(minio_path.split('/', 2))
        filename = file_path.split('/')[-1]
        mc = await get_minio_client(auth_token['at'], auth_token['rt'])
        result = await mc.stat_object(bucket, file_path)
        headers = {'Content-Length': str(result.size), 'Content-Disposition': f'attachment; filename={filename}'}
        response = await mc.get_object(bucket, file_path)
    except Exception as e:
        api_response = APIResponse()
        error_msg = f'Error getting file from minio: {str(e)}'
        __logger.error(error_msg)
        api_response.error_msg = error_msg
        return api_response.json_response()
    return StreamingResponse(response.stream(), headers=headers)


async def folder_stream(__logger, zip_list, folder_name, auth_token):
    tmp_folder = ConfigClass.MINIO_TMP_PATH + folder_name + '_' + str(time.time())
    zip_name = folder_name + '.zip'
    zipped_path = await zip_worker(__logger, zip_list, tmp_folder, auth_token)
    return FileResponse(path=zipped_path, filename=zip_name)


async def pack_zip_list(__logger, obj_geid):
    cache = []
    __logger.info('Getting folder from geid: ' + str(obj_geid))
    all_files = []
    __logger.info('Got files from folder:' + str(all_files))
    all_files = await get_files_recursive(obj_geid)
    __logger.info(f'Got files from folder after filter: {all_files}')
    for node in all_files:
        __logger.info('file node archived: ' + str(node.get('archived', False)))
        if node.get('archived', False):
            __logger.info('file node archived skipped' + str(node))
            continue
        cache.append(
            {
                'location': node['location'],
                'geid': node['global_entity_id'],
                'project_code': node.get('project_code', ''),
                'parent_folder': obj_geid,
            }
        )
    return cache


async def zip_worker(_logger, zip_list, tmp_folder, auth_token):
    """async zip worker."""
    try:
        mc = await get_minio_client(auth_token['at'], auth_token['rt'])
        # download all file to tmp folder
        for obj in zip_list:
            await mc.fget_object(obj, tmp_folder)
        shutil.make_archive(tmp_folder, 'zip', tmp_folder)
        disk_full_path = tmp_folder + '.zip'
        return disk_full_path
    except Exception:
        raise
