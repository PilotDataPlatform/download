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
from typing import Tuple

import httpx
from common import LoggerFactory
from common import get_boto3_client
from starlette.concurrency import run_in_threadpool

from app.commons.locks import bulk_lock_operation
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
    auth_token: Dict[str, str],
    operator: str,
    container_code: str,
    container_type: str,
    session_id: str,
    file_geids_to_include: Optional[Set[str]] = None,
):
    '''
    Summary:
        The function will create the FileDownloadClient object asynchronously.
        also it will call the FileDownloadClient.add_files_to_list to prepare
        the info for downloading.

        If there is no file to be added (some empty forlder). The function will
        raise error.

    Parameter:
        - files(list): the list of file will be added into object
        - auth_token(dict of str pairs): the auth/refresh token to access minio
        - operator(string): the user who takes the operation
        - container_code(string): the unique code for project/dataset
        - container_type(string): the type will be dataset or project
        - session_id(string): the unique id to track the user login session
        - file_geids_to_include(list): this is to check if request files are included
            by copy approval

    Return:
        - FileDownloadClient
    '''
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
        await download_client.add_files_to_list(file['id'])

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

        # here the bool is to handle the conner case that if user try to
        # download the folder where only has one file, the api should still
        # stream back the zip file
        self.folder_download = False

        self.logger = LoggerFactory('api_data_download').get_logger()

    async def _parse_minio_location(self, location: str) -> Tuple[str, str]:
        '''
        Summary:
            The function will parse out the minio location and return
            the bucket & object path

        Parameter:
            - location(str): the object location from metadata. The format
            of location will be:
                <http or https>://<minio_endpoint>/<bucket>/<object_path>

        Return:
            - bucket: the bucket in the minio
            - object_path: the path for the object
        '''
        minio_path = location.split('//')[-1]
        _, bucket, obj_path = tuple(minio_path.split('/', 2))

        return bucket, obj_path

    async def set_status(self, status: EDataDownloadStatus, payload: dict):
        '''
        Summary:
            The function will set the job status for current object in
            redis

        Parameter:
            - status(EDataDownloadStatus): the job status
            - payload(dict): the extra infomation

        Return:
            - dict: detail job info
        '''
        if len(self.files_to_zip) > 0:
            download_file = self.files_to_zip[0]
            payload.update({'zone': download_file.get('zone')})

        return await set_status(
            self.session_id,
            self.job_id,
            self.result_file_name,
            'data_download',
            status,
            self.container_code,
            self.operator,
            payload=payload,
        )

    async def add_files_to_list(self, _id: str) -> None:
        '''
        Summary:
            The function will add the file/folder with input _id into list.
            if input _id points to a folder then it will call the metadata
            api to get ALL files under it and its subfolders

        Parameter:
            - _id(str): the uuid of file/folder

        Return:
            - None
        '''
        ff_object = await get_files_folder_by_id(_id)

        file_list = []
        if 'folder' == ff_object.get('type'):
            self.logger.info(f'Getting folder from geid: {_id}')

            # raise the flag to True and later the zip_work will pack
            # the file(s) anyway
            self.folder_download = True

            # conner case: some of first level folder dont have any parent path(None)
            if ff_object.get('parent_path'):
                parent_path = ff_object.get('parent_path') + '.' + ff_object.get('name')
            else:
                parent_path = ff_object.get('name')

            folder_tree = await get_files_folder_recursive(
                self.container_code,
                self.container_type,
                ff_object.get('owner'),
                zone=ff_object.get('zone'),
                parent_path=parent_path,
            )

            # only take the file for downloading
            file_list = folder_tree

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

    async def generate_hash_code(self) -> str:
        '''
        Summary:
            The function will create the hashcode for download api.
            In the funtion will check if user download single file
            OR multiple files to generate the file_path differently

        Return:
            - str: hash code
        '''

        if len(self.files_to_zip) == 1:
            boto3_client = await get_boto3_client(
                ConfigClass.MINIO_ENDPOINT, token=self.auth_token['at'], https=ConfigClass.MINIO_HTTPS
            )
            bucket, file_path = await self._parse_minio_location(self.files_to_zip[0].get('location'))
            self.result_file_name = await boto3_client.get_download_presigned_url(bucket, file_path)
        else:
            self.result_file_name = self.tmp_folder + '.zip'

        return await generate_token(
            self.container_code,
            self.container_type,
            self.result_file_name,
            self.operator,
            self.session_id,
            self.job_id,
        )

    async def _file_download_worker(self, hash_code: str) -> None:
        '''
        Summary:
            The function will download all the file that has been added
            into the list to tmp folder. Before downloading the file, the
            function will lock ALL of them. After downloading, it will
            set the job status to finish.

        Parameter:
            - hash_code(str): the hashcode

        Return:
            - None
        '''

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

            # # download all file to tmp folder
            # mc = await get_minio_client(self.auth_token['at'], self.auth_token['rt'])
            # for obj in self.files_to_zip:
            #     await mc.fget_object(obj, self.tmp_folder)
            #     self.logger.info(f'File downloaded: {str(obj)}')

            boto3_client = await get_boto3_client(
                ConfigClass.MINIO_ENDPOINT, token=self.auth_token['at'], https=ConfigClass.MINIO_HTTPS
            )
            for obj in self.files_to_zip:
                bucket, obj_path = await self._parse_minio_location(obj.get('location'))
                await boto3_client.downlaod_object(bucket, obj_path, self.tmp_folder)

        except Exception as e:
            self.logger.error('Error in background job: ' + (str(e)))
            payload = {'error_msg': str(e)}
            await self.set_status(EDataDownloadStatus.CANCELLED, payload=payload)
            raise Exception(str(e))
        finally:
            self.logger.info('Start to unlock the nodes')
            await bulk_lock_operation(lock_keys, 'read', lock=False)

        self.logger.info('BACKGROUND TASK DONE')

        return None

    async def update_activity_log(self, dataset_geid: str, source_entry: list, event_type: str) -> dict:
        '''
        Summary:
            The function will create activity log for dataset file download
            ONLY.

        Parameter:
            - dataset_geid(str): the identifier of dataset
            - source_entry(list): the list of file has been downloaded
            - event_type(str): in download service this will be DATASET_FILEDOWNLOAD_SUCCEED

        Return:
            - dict: http reponse
        '''

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
        return res.json()

    async def _zip_worker(self):
        '''
        Summary:
            The function will zip the files under the tmp folder.

        Return:
            - None
        '''

        self.logger.info('Start to ZIP files')
        await run_in_threadpool(shutil.make_archive, self.tmp_folder, 'zip', self.tmp_folder)

        return None

    async def background_worker(self, hash_code: str) -> None:
        '''
        Summary:
            The function is the core of the object. this is a background job and
            will be trigger by api. Funtion will download all file in the file_to_zip
            list and zip them together

        Parameter:
            - hash_code(str): the hash code for downloading

        Return:
            - None
        '''

        await self._file_download_worker(hash_code)

        # zip the files under the tmp folder if we have number > 1
        if self.folder_download or len(self.files_to_zip) > 1:
            await self._zip_worker()

        # NOTE: the status of job will be updated ONLY after the zip worker
        await self.set_status(EDataDownloadStatus.READY_FOR_DOWNLOADING, payload={'hash_code': hash_code})

        # add the activity logs
        if self.container_type == 'dataset':

            # REMOVE THIS AFTER MIGRATION
            node_query_url = ConfigClass.DATASET_SERVICE + 'dataset-peek/' + self.container_code
            with httpx.Client() as client:
                response = client.get(node_query_url)
            dataset_geid = response.json().get('result', {}).get('id')

            filenames = ['/'.join(i['location'].split('/')[7:]) for i in self.files_to_zip]
            await self.update_activity_log(
                dataset_geid,
                filenames,
                'DATASET_FILEDOWNLOAD_SUCCEED',
            )

        return None
