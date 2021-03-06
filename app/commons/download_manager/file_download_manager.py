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

import os
import shutil
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from common import LoggerFactory
from common.object_storage_adaptor.boto3_client import Boto3Client
from starlette.concurrency import run_in_threadpool

from app.commons.kafka_producer import get_kafka_producer
from app.commons.locks import bulk_lock_operation
from app.config import ConfigClass
from app.models.base_models import EAPIResponseCode
from app.models.models_data_download import EDataDownloadStatus
from app.resources.download_token_manager import generate_token
from app.resources.error_handler import APIException
from app.resources.helpers import (
    get_files_folder_by_id,
    get_files_folder_recursive,
    set_status,
)

ITEM_MESSAGE_SCHEMA = 'metadata_items_activity.avsc'


async def create_file_download_client(
    files: List[Dict[str, Any]],
    boto3_clients: Dict[str, Boto3Client],
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
        - boto3_clients(dict of Boto3Client):
            - boto3_internal: the instance of boto3client with private domain
            - boto3_public: the instance of boto3client with public domain
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
        # auth_token=auth_token,
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

    # set the boto3 client between public and private domain
    await download_client._set_connection(boto3_clients)

    if len(download_client.files_to_zip) < 1 and container_type == 'project':
        error_msg = '[Invalid file amount] must greater than 0'
        download_client.logger.error(error_msg)
        raise APIException(status_code=EAPIResponseCode.bad_request.value, error_msg=error_msg)

    return download_client


class FileDownloadClient:
    def __init__(
        self,
        # auth_token: Dict[str, Any],
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
        # self.auth_token = auth_token
        self.session_id = session_id
        self.container_type = container_type
        self.file_geids_to_include = file_geids_to_include

        # here the bool is to handle the conner case that if user try to
        # download the folder where only has one file, the api should still
        # stream back the zip file
        self.folder_download = False

        # if number of file is 1 without any folder, the boto3_client
        # will use the instance with private domain. Otherwise, it will
        # use the public domain
        self.boto3_client = None

        self.logger = LoggerFactory('file_download_manager').get_logger()

    async def _set_connection(self, boto3_clients: Dict[str, Boto3Client]):
        '''
        Summary:
            if number of file is 1 without any folder, the boto3_client
            will use the instance with private domain. Otherwise, it will
            use the public domain
        Parameter:
            - boto3_clients(dict of Boto3Client):
                - boto3_internal: the instance of boto3client with private domain
                - boto3_public: the instance of boto3client with public domain
        '''

        if self.folder_download or len(self.files_to_zip) > 1:
            self.boto3_client = boto3_clients.get('boto3_internal')
        else:
            self.boto3_client = boto3_clients.get('boto3_public')

        return

    async def _parse_object_location(self, location: str) -> Tuple[str, str]:
        '''
        Summary:
            The function will parse out the object location and return
            the bucket & object path

        Parameter:
            - location(str): the object location from metadata. The format
            of location will be:
                <http or https>://<storage_endpoint>/<bucket>/<object_path>

        Return:
            - bucket: the bucket in the storage
            - object_path: the path for the object
        '''
        object_path = location.split('//')[-1]
        _, bucket, obj_path = tuple(object_path.split('/', 2))

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
            # also make the parent path None to empty string
            if file.get('parent_path') is None:
                file.update({'parent_path': ''})
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

        if self.folder_download or len(self.files_to_zip) > 1:
            self.result_file_name = self.tmp_folder + '.zip'
        else:
            # Note here if minio can be public assessible then the endpoint
            # must be domain name
            bucket, file_path = await self._parse_object_location(self.files_to_zip[0].get('location'))
            self.result_file_name = await self.boto3_client.get_download_presigned_url(bucket, file_path)

        # since the file or files are from some zone/project
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

            # then download from object storage
            for obj in self.files_to_zip:
                bucket, obj_path = await self._parse_object_location(obj.get('location'))
                await self.boto3_client.downlaod_object(bucket, obj_path, self.tmp_folder + '/' + obj_path)

        except Exception as e:
            self.logger.error(
                'Error in background job: ' + (str(e)),
            )
            payload = {'error_msg': str(e)}
            await self.set_status(EDataDownloadStatus.CANCELLED, payload=payload)
            raise Exception(str(e))
        finally:
            self.logger.info('Start to unlock the nodes')
            await bulk_lock_operation(lock_keys, 'read', lock=False)

        self.logger.info('BACKGROUND TASK DONE')

        return None

    async def update_activity_log(self) -> dict:
        '''
        Summary:
            The function will create activity log for dataset file download
            ONLY. this file download will send to item activity log index

        Return:
            - dict: http reponse
        '''

        kp = await get_kafka_producer()

        # generate the some basic info for log. if multiple files are
        # downloaded, hide some infomation to aviod misleading
        source_node = self.files_to_zip[0]
        if len(self.files_to_zip) != 1:
            source_node.update({'id': None, 'name': os.path.basename(self.result_file_name)})

        message = {
            'activity_type': 'download',
            'activity_time': datetime.utcnow(),
            'item_id': source_node.get('id'),
            'item_type': source_node.get('type'),
            'item_name': source_node.get('name'),
            'item_parent_path': source_node.get('parent_path'),
            'container_code': source_node.get('container_code'),
            'container_type': source_node.get('container_type'),
            'zone': source_node.get('zone'),
            'user': self.operator,
            'imported_from': '',
            'changes': [],
        }

        await kp.create_activity_log(
            message,
            ITEM_MESSAGE_SCHEMA,
            ConfigClass.KAFKA_ITEM_ACTIVITY_TOPIC,
        )

        return

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
        await self.update_activity_log()
        # MOVE TO PRE
        # if self.container_type == 'dataset':

        #     # REMOVE THIS AFTER MIGRATION
        #     node_query_url = ConfigClass.DATASET_SERVICE + 'dataset-peek/' + self.container_code
        #     with httpx.Client() as client:
        #         response = client.get(node_query_url)
        #     dataset_geid = response.json().get('result', {}).get('id')

        #     filenames = ['/'.join(i['location'].split('/')[7:]) for i in self.files_to_zip]
        #     await self.update_activity_log(
        #         dataset_geid,
        #         filenames,
        #         'DATASET_FILEDOWNLOAD_SUCCEED',
        #     )

        return None
