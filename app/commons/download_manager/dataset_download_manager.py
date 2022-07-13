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
from datetime import datetime
from typing import Dict

import aiofiles.os
import httpx
from common.object_storage_adaptor.boto3_client import Boto3Client

from app.commons.download_manager.file_download_manager import FileDownloadClient
from app.commons.kafka_producer import get_kafka_producer
from app.config import ConfigClass
from app.models.models_data_download import EDataDownloadStatus
from app.resources.download_token_manager import generate_token
from app.resources.helpers import get_files_folder_recursive

DATASET_MESSAGE_SCHEMA = 'dataset.activity.avsc'


async def create_dataset_download_client(
    boto3_clients: Dict[str, Boto3Client],
    operator: str,
    container_code: str,
    container_id: str,
    container_type: str,
    session_id: str,
):
    '''
    Summary:
        The function will create the DatasetDownloadClient object asynchronously.
        also it will call the DatasetDownloadClient.add_files_to_list to prepare
        the files for downloading.

        Note: this class is different with FileDownloadClient, which allows the
        empty file/folder

    Parameter:
        - auth_token(dict of str pairs): the auth/refresh token to access minio
        - operator(string): the user who takes the operation
        - container_code(string): the unique code for project/dataset
        - container_type(string): the type will be dataset or project
        - session_id(string): the unique id to track the user login session

    Return:
        - DatasetDownloadClient
    '''

    download_client = DatasetDownloadClient(
        operator=operator,
        container_code=container_code,
        container_id=container_id,
        container_type=container_type,
        session_id=session_id,
    )

    await download_client.add_files_to_list(container_code)
    # use the private domain for dataset download
    await download_client._set_connection(boto3_clients.get('boto3'))

    return download_client


class DatasetDownloadClient(FileDownloadClient):
    def __init__(
        self,
        # auth_token: Dict[str, Any],
        operator: str,
        container_code: str,
        container_id: str,
        container_type: str,
        session_id: str,
    ):
        super().__init__(
            # auth_token,
            operator,
            container_code,
            container_type,
            session_id,
            [],
        )

        self.container_id = container_id

    async def _set_connection(self, boto3_client: Boto3Client):
        '''
        Summary:
            The dataset connection will be alway private domain
        '''

        self.boto3_client = boto3_client

        return

    async def add_schemas(self, dataset_geid: str) -> None:
        '''
        Summary:
            The function will call the dataset shema api to get detail of schemas.
            and then saves schema json files to folder that will zipped.

        Parameter:
            - dataset_geid(str): the identifier of dataset

        Return:
            - None
        '''

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

    async def generate_hash_code(self) -> str:
        '''
        Summary:
            The function will create the hashcode for download api.

        Return:
            - str: hash code
        '''

        self.result_file_name = self.tmp_folder + '.zip'

        return await generate_token(
            self.container_code,
            self.container_type,
            self.result_file_name,
            self.operator,
            self.session_id,
            self.job_id,
        )

    async def update_activity_log(self) -> dict:
        '''
        Summary:
            The function will create activity log for dataset file download
            ONLY. this file download will send to item activity log index

        Return:
            - dict: http reponse
        '''

        kp = await get_kafka_producer()

        message = {
            'activity_type': 'download',
            'activity_time': datetime.utcnow(),
            'container_code': self.container_code,
            # the version is not necessary for the datset logs. The download logic
            # can get the version but this need some extra efforts.
            'version': None,
            'target_name': self.result_file_name,
            'user': self.operator,
            'changes': [],
        }

        await kp.create_activity_log(
            message,
            DATASET_MESSAGE_SCHEMA,
            ConfigClass.KAFKA_DATASET_ACTIVITY_TOPIC,
        )

        return

    async def add_files_to_list(self, dataset_code):
        '''
        Summary:
            The function will add the file/folder with input geid into list.
            It is slightly different with file download. The dataset download
            will try to query ALL the file/folders under the target dataset.

        Parameter:
            - dataset_code(str): the unique code of dataset

        Return:
            - None
        '''

        folder_tree = await get_files_folder_recursive(
            dataset_code,
            'dataset',
            self.operator,
            zone=1,
        )
        # only take the file for downloading
        for x in folder_tree:
            # flatten the storage url
            x.update({'location': x.get('storage', {}).get('location_uri')})
            self.files_to_zip.append(x)

    async def background_worker(self, hash_code: str) -> None:
        '''
        Summary:
            The function is the core of the object. this is a background job and
            will be trigger by api. Funtion will make following actions:
                - download all files in the file_to_zip
                - download all schemas under dataset
                - zip files/schemas into a zip file
                - create the activity logs for dataset

        Parameter:
            - hash_code(str): the hash code for downloading

        Return:
            - dict: None
        '''

        await self._file_download_worker(hash_code)

        await self.add_schemas(self.container_id)  # update here once back

        # here is different since the dataset will have the default schema
        # no matter how, we will zip all the file under the temp folder
        await self._zip_worker()

        # NOTE: the status of job will be updated ONLY after the zip worker
        await self.set_status(EDataDownloadStatus.READY_FOR_DOWNLOADING, payload={'hash_code': hash_code})

        await self.update_activity_log()

        return None
