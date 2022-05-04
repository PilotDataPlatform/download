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

import json
import shutil
import time
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Set

import aiofiles.os
import httpx
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
from app.resources.helpers import set_status


async def create_download_client(
    files: List[Dict[str, Any]],
    auth_token: Dict[str, Any],
    operator: str,
    project_code: str,
    geid: str,
    session_id: str,
    download_type: str = 'project',
    file_geids_to_include: Optional[Set[str]] = None,
):
    download_client = _DownloadClient(
        files=files,
        auth_token=auth_token,
        operator=operator,
        project_code=project_code,
        geid=geid,
        session_id=session_id,
        download_type=download_type,
        file_geids_to_include=file_geids_to_include,
    )
    for file in files:
        await download_client.add_files_to_list(file['geid'])

    if len(download_client.files_to_zip) < 1 and download_client.download_type != 'full_dataset':
        error_msg = '[Invalid file amount] must greater than 0'
        download_client.logger.error(error_msg)
        raise APIException(status_code=EAPIResponseCode.bad_request.value, error_msg=error_msg)
    return download_client


class _DownloadClient:
    def __init__(
        self,
        files: List[Dict[str, Any]],
        auth_token: Dict[str, Any],
        operator: str,
        project_code: str,
        geid: str,
        session_id: str,
        download_type: str = 'project',
        file_geids_to_include: Optional[Set[str]] = None,
    ):
        self.job_id = 'data-download-' + str(int(time.time()))
        self.job_status = EDataDownloadStatus.INIT
        self.files = files
        self.file_nodes = []
        self.files_to_zip = []
        self.operator = operator
        self.project_code = project_code
        self.tmp_folder = ConfigClass.MINIO_TMP_PATH + project_code + '_' + str(time.time())
        self.result_file_name = ''
        self.auth_token = auth_token
        self.session_id = session_id
        self.download_type = download_type
        self.file_geids_to_include = file_geids_to_include
        self.geid = geid
        self.contains_folder = True if self.download_type == 'full_dataset' else False
        self.logger = LoggerFactory('api_data_download').get_logger()

    async def set_status(self, status, payload):
        # pick up the first file for the metadata setup
        if len(self.files_to_zip) > 0:
            download_file = self.files_to_zip[0]
            geid = download_file.get('geid')
            payload.update({'zone': download_file.get('label')})
        else:
            geid = self.geid
        return await set_status(
            self.session_id,
            self.job_id,
            self.result_file_name,
            'data_download',
            status,
            self.project_code,
            self.operator,
            geid,
            payload=payload,
        )

    async def add_files_to_list(self, geid: str):
        # url = ConfigClass.NEO4J_SERVICE + f'nodes/geid/{geid}'
        url = ConfigClass.METADATA_SERVICE + f'item/{geid}/'
        try:
            async with httpx.AsyncClient() as client:
                res = await client.get(url)

            response = res.json().get('result', {})
            file_list = []
            if 'folder' == response.get('type'):
                self.logger.info(f'Getting folder from geid: {geid}')

                payload = {
                    'container_code': self.project_code,
                    'zone': response.get('zone'),
                    'recursive': True,
                    'archived': False,
                    'parent_path': response.get('parent_path') + '.' + response.get('name'),
                    'owner': response.get('owner'),
                }
                url = ConfigClass.METADATA_SERVICE + 'items/search/'
                async with httpx.AsyncClient() as client:
                    res = await client.get(url, params=payload)

                # only take the file for downloading
                for x in res.json().get('result', []):
                    if 'file' == x.get('type'):
                        file_list.append(x)

            else:
                file_list = [response]

            # this is to download from approval panel
            # this operation will not happen with normal download together
            if self.file_geids_to_include is not None:
                file_list = [file for file in file_list if file['global_entity_id'] in self.file_geids_to_include]

            for file in file_list:
                self.files_to_zip.append(
                    {
                        'label': file.get('zone'),
                        'location': file.get('storage', {}).get('location_uri'),
                        'geid': file.get('id'),
                        'project_code': file.get('container_code', ''),
                        'operator': self.operator,
                        # ?
                        'dataset_code': file.get('container_code', ''),
                    }
                )
        except Exception as e:
            self.logger.error(f'Fail to add files to list: {str(e)}')
            raise

    def generate_hash_code(self):
        if len(self.files_to_zip) > 1 or self.contains_folder:
            self.result_file_name = self.tmp_folder + '.zip'
        else:
            location = self.files_to_zip[0]['location']
            self.result_file_name = self.tmp_folder + '/' + Minio_Client.parse_minio_location(location)[1]

        geid = self.files_to_zip[0]['geid'] if len(self.files_to_zip) > 0 else self.geid
        return generate_token(
            {
                'geid': geid,
                'full_path': self.result_file_name,
                'issuer': 'SERVICE DATA DOWNLOAD',
                'operator': self.operator,
                'session_id': self.session_id,
                'job_id': self.job_id,
                'project_code': self.project_code,
                'iat': int(time.time()),
                'exp': int(time.time()) + (ConfigClass.DOWNLOAD_TOKEN_EXPIRE_AT * 60),
            }
        )

    async def add_schemas(self, dataset_geid):
        """Saves schema json files to folder that will zipped."""
        try:
            if not await aiofiles.os.path.isdir(self.tmp_folder):
                await aiofiles.os.mkdir(self.tmp_folder)
                await aiofiles.os.mkdir(self.tmp_folder + '/data')

            payload = {
                'dataset_geid': dataset_geid,
                'standard': 'default',
                'is_draft': False,
            }
            async with httpx.AsyncClient() as client:
                response = await client.post(ConfigClass.DATASET_SERVICE + 'schema/list', json=payload)
            for schema in response.json()['result']:
                with open(self.tmp_folder + '/default_' + schema['name'], 'w') as w:
                    w.write(json.dumps(schema['content'], indent=4, ensure_ascii=False))

            payload = {
                'dataset_geid': dataset_geid,
                'standard': 'open_minds',
                'is_draft': False,
            }
            async with httpx.AsyncClient() as client:
                response = await client.post(ConfigClass.DATASET_SERVICE + 'schema/list', json=payload)
            for schema in response.json()['result']:
                with open(self.tmp_folder + '/openMINDS_' + schema['name'], 'w') as w:
                    w.write(json.dumps(schema['content'], indent=4, ensure_ascii=False))
        except Exception as e:
            self.logger.error(f'Fail to create schemas: {str(e)}')
            raise

    async def zip_worker(self, hash_code):
        lock_keys = []
        try:
            # add the file lock
            bucket_prefix = 'gr-' if ConfigClass.namespace == 'greenroom' else 'core-'
            for nodes in self.files_to_zip:
                bucket = bucket_prefix + nodes.get('project_code')
                lock_keys.append('%s/%s/%s' % (bucket, nodes.get('parent_path'), nodes.get('name')))

            await bulk_lock_operation(lock_keys, 'read')

            mc = await get_minio_client(self.auth_token['at'], self.auth_token['rt'])
            # download all file to tmp folder
            for obj in self.files_to_zip:
                await mc.fget_object(obj, self.tmp_folder)
                self.logger.info(f'File downloaded: {str(obj)}')

            if self.download_type == 'full_dataset':
                await self.add_schemas(self.geid)

            if len(self.files_to_zip) > 1 or self.contains_folder:
                self.logger.info('Start to ZIP files')
                await run_in_threadpool(shutil.make_archive, self.tmp_folder, 'zip', self.tmp_folder)
                self.logger.info('ZIP File created')

            if self.download_type == 'dataset_files':
                # Dataset file download
                # get list of file names
                filenames = ['/'.join(i['location'].split('/')[7:]) for i in self.files_to_zip]
                await self.update_activity_log(
                    self.geid,
                    filenames,
                    'DATASET_FILEDOWNLOAD_SUCCEED',
                )
            await self.set_status(EDataDownloadStatus.READY_FOR_DOWNLOADING.name, payload={'hash_code': hash_code})
        except Exception as e:
            payload = {'error_msg': str(e)}
            await self.set_status(EDataDownloadStatus.CANCELLED.name, payload=payload)
        finally:
            self.logger.info('Start to unlock the nodes')
            await bulk_lock_operation(lock_keys, 'read', lock=False)
        self.logger.info('BACKGROUND TASK DONE')

    async def update_activity_log(self, dataset_geid, source_entry, event_type):
        url = ConfigClass.QUEUE_SERVICE + 'broker/pub'
        post_json = {
            'event_type': event_type,
            'payload': {
                'dataset_geid': dataset_geid,
                'operator': self.operator,
                'action': 'DOWNLOAD',
                'resource': 'Dataset',
                'detail': {'source': source_entry},
            },
            'queue': 'dataset_actlog',
            'routing_key': '',
            'exchange': {'name': 'DATASET_ACTS', 'type': 'fanout'},
        }
        async with httpx.AsyncClient() as client:
            res = await client.post(url, json=post_json)
        if res.status_code != 200:
            error_msg = 'update_activity_log {}: {}'.format(res.status_code, res.text)
            self.logger.error(error_msg)
            raise Exception(error_msg)
        return res
