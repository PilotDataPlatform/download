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
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Set

from common import LoggerFactory
from starlette.concurrency import run_in_threadpool

from app.commons.locks import bulk_lock_operation
from app.commons.service_connection.minio_client import Minio_Client
from app.commons.service_connection.minio_client import get_minio_client
from app.config import ConfigClass
from app.models.base_models import EAPIResponseCode
from app.models.models_data_download import EDataDownloadStatus
from app.resources.download_token_manager import generate_token
from app.resources.error_handler import APIException
from app.resources.helpers import get_files_folder_by_id
from app.resources.helpers import get_files_folder_recursive
from app.resources.helpers import set_status


async def create_file_download_client(
    files: List[Dict[str, Any]],
    auth_token: Dict[str, Any],
    operator: str,
    container_code: str,
    container_type: str,
    session_id: str,
    file_geids_to_include: Optional[Set[str]] = None,
):
    download_client = FileDownloadClient(
        auth_token=auth_token,
        operator=operator,
        container_code=container_code,
        container_type=container_type,
        session_id=session_id,
        file_geids_to_include=file_geids_to_include,
    )

    # add files into the list. It will check if we try to
    # download the empty project folder
    for file in files:
        await download_client.add_files_to_list(file['geid'])

    if len(download_client.files_to_zip) < 1 and container_type == 'project':
        error_msg = '[Invalid file amount] must greater than 0'
        download_client.logger.error(error_msg)
        raise APIException(status_code=EAPIResponseCode.bad_request.value, error_msg=error_msg)

    return download_client


class FileDownloadClient:
    def __init__(
        self,
        auth_token: Dict[str, Any],
        operator: str,
        container_code: str,
        container_type: str,
        session_id: str,
        file_geids_to_include: Optional[Set[str]] = None,
    ):
        self.job_id = 'data-download-' + str(int(time.time()))
        self.job_status = EDataDownloadStatus.INIT
        self.files_to_zip = []
        self.operator = operator
        self.container_code = container_code
        self.tmp_folder = ConfigClass.MINIO_TMP_PATH + container_type + container_code + '_' + str(time.time())
        self.result_file_name = ''
        self.auth_token = auth_token
        self.session_id = session_id
        self.container_type = container_type
        self.file_geids_to_include = file_geids_to_include

        self.logger = LoggerFactory('api_data_download').get_logger()

    async def set_status(self, status: EDataDownloadStatus, payload: dict):
        # pick up the first file for the metadata setup
        download_file = self.files_to_zip[0]
        file_id = download_file.get('id')
        payload.update({'zone': download_file.get('zone')})

        return await set_status(
            self.session_id,
            self.job_id,
            self.result_file_name,
            'data_download',
            status,
            self.container_code,
            self.operator,
            file_id,
            payload=payload,
        )

    async def add_files_to_list(self, geid: str) -> None:
        ff_object = await get_files_folder_by_id(geid)

        file_list = []
        if 'folder' == ff_object.get('type'):
            self.logger.info(f'Getting folder from geid: {geid}')

            folder_tree = await get_files_folder_recursive(
                self.container_code,
                self.container_type,
                ff_object.get('zone'),
                ff_object.get('parent_path') + '.' + ff_object.get('name'),
                ff_object.get('owner'),
            )
            # only take the file for downloading
            for x in folder_tree:
                if 'file' == x.get('type'):
                    file_list.append(x)

        else:
            file_list = [ff_object]

        # this is to download from approval panel
        if self.file_geids_to_include is not None:
            file_list = [file for file in file_list if file['id'] in self.file_geids_to_include]

        for file in file_list:
            # flatten the storage url
            file.update({'location': file.get('storage', {}).get('location_uri')})
            self.files_to_zip.append(file)

        return None

    def generate_hash_code(self) -> str:
        if len(self.files_to_zip) > 1:
            self.result_file_name = self.tmp_folder + '.zip'
        else:
            location = self.files_to_zip[0]['location']
            self.result_file_name = self.tmp_folder + '/' + Minio_Client.parse_minio_location(location)[1]

        _id = self.files_to_zip[0]['id'] if len(self.files_to_zip) > 0 else self.geid

        return generate_token(
            {
                'geid': _id,
                'full_path': self.result_file_name,
                'issuer': 'SERVICE DATA DOWNLOAD',
                'operator': self.operator,
                'session_id': self.session_id,
                'job_id': self.job_id,
                'project_code': self.container_code,
                'iat': int(time.time()),
                'exp': int(time.time()) + (ConfigClass.DOWNLOAD_TOKEN_EXPIRE_AT * 60),
            }
        )

    async def zip_worker(self, hash_code: str) -> None:
        lock_keys = []
        try:
            # add the file lock
            bucket_prefix = 'gr-' if ConfigClass.namespace == 'greenroom' else 'core-'
            for nodes in self.files_to_zip:
                # for project we have the bucket prefix
                # but for dataset we dont have it
                if self.container_type == 'project':
                    bucket = bucket_prefix + nodes.get('container_code')
                else:
                    bucket = nodes.get('container_code')
                lock_keys.append('%s/%s/%s' % (bucket, nodes.get('parent_path'), nodes.get('name')))
            await bulk_lock_operation(lock_keys, 'read')

            # download all file to tmp folder
            mc = await get_minio_client(self.auth_token['at'], self.auth_token['rt'])
            for obj in self.files_to_zip:
                await mc.fget_object(obj, self.tmp_folder)
                self.logger.info(f'File downloaded: {str(obj)}')

            # zip the files under the tmp folder if we have number > 1
            if len(self.files_to_zip) > 1:
                self.logger.info('Start to ZIP files')
                await run_in_threadpool(shutil.make_archive, self.tmp_folder, 'zip', self.tmp_folder)
                self.logger.info('ZIP File created')

            await self.set_status(EDataDownloadStatus.READY_FOR_DOWNLOADING.name, payload={'hash_code': hash_code})
        except Exception as e:
            self.logger.error('Error in background job: ' + (str(e)))
            payload = {'error_msg': str(e)}
            await self.set_status(EDataDownloadStatus.CANCELLED.name, payload=payload)
        finally:
            self.logger.info('Start to unlock the nodes')
            await bulk_lock_operation(lock_keys, 'read', lock=False)

        self.logger.info('BACKGROUND TASK DONE')

        return None
