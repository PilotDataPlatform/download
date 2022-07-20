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

import asyncio
from typing import Optional, Union

import httpx
from common import (
    LoggerFactory,
    ProjectClient,
    ProjectNotFoundException,
    get_boto3_client,
)
from fastapi import APIRouter, BackgroundTasks, Cookie, Header
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi_utils import cbv
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import create_async_engine

from app.commons.download_manager.dataset_download_manager import (
    create_dataset_download_client,
)
from app.commons.download_manager.file_download_manager import (
    create_file_download_client,
)
from app.config import ConfigClass
from app.models.base_models import APIResponse, EAPIResponseCode
from app.models.models_data_download import (
    DatasetPrePOST,
    EDataDownloadStatus,
    PreDataDownloadPOST,
    PreDataDownloadResponse,
)
from app.resources.download_token_manager import verify_dataset_version_token
from app.resources.error_handler import catch_internal
from app.resources.helpers import ResourceNotFound
from app.services.approval.client import ApprovalServiceClient

router = APIRouter()

_API_TAG = 'v2/data-download'
_API_NAMESPACE = 'api_data_download'


@cbv.cbv(router)
class APIDataDownload:
    """API Data Download Class."""

    def __init__(self):
        self.__logger = LoggerFactory('api_data_download_v2').get_logger()
        self.project_client = ProjectClient(ConfigClass.PROJECT_SERVICE, ConfigClass.REDIS_URL)
        # here are something different with upload. This varible will have
        # following structure: {"boto3_internal":<Boto3Client>, "boto3_public":<Boto3Client>}
        # The reason is presigned url will require we initialize class
        # with public domain so user can access with. However, some internal
        # operations will go through the prive domain within cluster to boost performance
        self.boto3_clients = self._connect_to_object_storage()

    def _connect_to_object_storage(self):
        '''
        Summary:
            Setup the two connection class:
                - boto3_internal: use private domain
                - boto3_public: use public domain
        '''
        loop = asyncio.new_event_loop()

        self.__logger.info('Initialize the boto3 clients')
        try:
            boto3_internal = loop.run_until_complete(
                get_boto3_client(
                    ConfigClass.S3_INTERNAL,
                    access_key=ConfigClass.S3_ACCESS_KEY,
                    secret_key=ConfigClass.S3_SECRET_KEY,
                    https=ConfigClass.S3_INTERNAL_HTTPS,
                )
            )

            boto3_public = loop.run_until_complete(
                get_boto3_client(
                    ConfigClass.S3_PUBLIC,
                    access_key=ConfigClass.S3_ACCESS_KEY,
                    secret_key=ConfigClass.S3_SECRET_KEY,
                    https=ConfigClass.S3_PUBLIC_HTTPS,
                )
            )
        except Exception as e:
            error_msg = str(e)
            self.__logger.error('Fail to create connection with boto3: %s', error_msg)
            raise e

        loop.close()
        return {'boto3_internal': boto3_internal, 'boto3_public': boto3_public}

    @router.post(
        '/download/pre/',
        tags=[_API_TAG],
        response_model=PreDataDownloadResponse,
        summary='Pre download process, zip as a package if more than 1 file, '
        'used in project files download and dataset single file download.',
    )
    @catch_internal(_API_NAMESPACE)
    async def data_pre_download(
        self,
        data: PreDataDownloadPOST,
        background_tasks: BackgroundTasks,
        sessionId: str = Cookie(None),
    ) -> JSONResponse:
        '''
        Summary:
            The API serves as the pre file download operation. Since the file
            downloading is a background job, this api will download target
            files and zip them under the tmp folder.

            Afterwards, the frontend will all the /v1/downlaod/<hashcode> to
            download the zipped file or a single file

        Payload:
             - files(list): the list of target id file will be
                downloaded (either in project or dataset)
             - operator(str): the user who takes the operation
             - container_code(str): the unique code of project
             - container_type(str): the type of container will be project/dataset
             - approval_request_id(UUID): the unique identifier for approval

        Header:
             - authorization(str): the access token from auth service
             - refresh_token(str): the refresh token from auth service
        Cookies:
             - sessionId(str): the session id generate for each user login

        Return:
            - 200
        '''

        self.__logger.info('Recieving request on /download/pre/')
        response = APIResponse()

        # check the container exist
        self.__logger.info(f'Check container: {data.container_type} {data.container_code}.')
        try:
            if data.container_type == 'project':
                _ = await self.project_client.get(code=data.container_code)
            elif data.container_type == 'dataset':
                node_query_url = ConfigClass.DATASET_SERVICE + 'dataset-peek/' + data.container_code
                with httpx.Client() as client:
                    dataset_response = client.get(node_query_url)
                if dataset_response.status_code != 200:
                    raise Exception('Fetch dataset error: %s', dataset_response.json())

        except ProjectNotFoundException as e:
            response.error_msg = e.error_msg
            response.code = EAPIResponseCode.not_found
            return response.json_response()

        # the special requirement to download the file from a set of
        # approval files. Fetch files from Postgres by approval id
        file_geids_to_include = None
        if data.approval_request_id:
            self.__logger.info(f'download from approval request: {data.approval_request_id}')

            engine = create_async_engine(ConfigClass.RDS_DB_URI)
            metadata = MetaData(schema=ConfigClass.RDS_SCHEMA_DEFAULT)
            async with engine.connect() as conn:
                await conn.run_sync(metadata.reflect, only=['approval_entity'])

            # get the set of approved files
            approval_service_client = ApprovalServiceClient(engine, metadata)
            request_approval_entities = await approval_service_client.get_approval_entities(
                str(data.approval_request_id)
            )
            file_geids_to_include = set(request_approval_entities.keys())

            self.__logger.info(f'File included in approval request: {file_geids_to_include}')

        try:
            self.__logger.info('Initialize the data download client')
            download_client = await create_file_download_client(
                data.files,
                self.boto3_clients,
                data.operator,
                data.container_code,
                data.container_type,
                sessionId,
                file_geids_to_include,
            )

            download_client.logger.info('generate hash token')
            hash_code = await download_client.generate_hash_code()

            download_client.logger.info('Init the download job status')
            status_result = await download_client.set_status(
                EDataDownloadStatus.ZIPPING, payload={'hash_code': hash_code}
            )

            download_client.logger.info(
                f'Starting background job for: {data.container_code}.'
                f'number of files {len(download_client.files_to_zip)}'
            )
            # start the background job for the zipping
            background_tasks.add_task(download_client.background_worker, hash_code)

            response.result = status_result
            response.code = EAPIResponseCode.success

        except ResourceNotFound as e:
            response.error_msg = str(e)
            response.code = EAPIResponseCode.not_found
        except Exception as e:
            response.error_msg = str(e)
            response.code = EAPIResponseCode.internal_error

        return response.json_response()

    @router.post('/dataset/download/pre', tags=[_API_TAG], summary='Download all files & schemas in a dataset')
    @catch_internal(_API_NAMESPACE)
    async def dataset_pre_download(
        self,
        data: DatasetPrePOST,
        background_tasks: BackgroundTasks,
        sessionId: str = Cookie(None),
    ) -> JSONResponse:

        '''
        Summary:
            The API serves as the pre download for whole dataset. All files
            and schemas will be download and packed as zip file under tmp folder.

            Afterwards, the frontend will all the /v1/downlaod/<hashcode> to
            download the zipped file or a single file

        Payload:
            - dataset_code(list): the unique code of dataset
            - operator(str): the user who takes the operation

        Header:
             - authorization(str): the access token from auth service
             - refresh_token(str): the refresh token from auth service
        Cookies:
             - sessionId(str): the session id generate for each user login

        Return:
            - 200
        '''

        self.__logger.info('Recieving request on /dataset/download/pre')
        api_response = APIResponse()

        # check the dataset exist
        node_query_url = ConfigClass.DATASET_SERVICE + 'dataset-peek/' + data.dataset_code
        with httpx.Client() as client:
            response = client.get(node_query_url)
        dataset_id = response.json().get('result', {}).get('id')

        self.__logger.info('Initialize the dataset download client')
        download_client = await create_dataset_download_client(
            self.boto3_clients,
            data.operator,
            data.dataset_code,
            dataset_id,
            'dataset',
            sessionId,
        )
        hash_code = await download_client.generate_hash_code()
        status_result = await download_client.set_status(EDataDownloadStatus.ZIPPING, payload={'hash_code': hash_code})
        download_client.logger.info(
            f'Starting background job for: {data.dataset_code}.' f'number of files {len(download_client.files_to_zip)}'
        )
        background_tasks.add_task(download_client.background_worker, hash_code)

        api_response.result = status_result
        api_response.code = EAPIResponseCode.success
        return api_response.json_response()

    @router.get('/dataset/download/{hash_code}', tags=[_API_TAG], summary='Download dataset version')
    async def download_dataset_version(
        self,
        hash_code: str,
        authorization: Optional[str] = Header(None),
    ) -> Union[StreamingResponse, JSONResponse]:

        '''
        Summary:
            This api is different with others. This is for dataset *version* download. The reason
            is the dataset version is not save as file node instead it has a seperate table to
            keep track the version. And this version is already zipped.

            The hashcode is generated by dataset service. The code will contains the minio location
            for this api to download directly from minio.
        Parameter:
            - hash_code(string): the HS256 generate by dataset service

        Return:
            - url: the presigned url to download the file
        '''

        self.__logger.info('Recieving request on /dataset/download/{hash_code}')
        api_response = APIResponse()
        valid, result = verify_dataset_version_token(hash_code)
        if not valid:
            api_response.code = EAPIResponseCode.unauthorized
            api_response.error_msg = result[1]
            return api_response.json_response()

        minio_path = result['location'].split('//')[-1]
        _, bucket, file_path = tuple(minio_path.split('/', 2))

        try:
            self.__logger.info('Generate presigned url')
            # here is a special case that we generate presigned url
            # without going through the bff. so I use the token to generate
            boto3_client = await get_boto3_client(
                ConfigClass.S3_PUBLIC, token=authorization, https=ConfigClass.S3_PUBLIC_HTTPS
            )
            presigned_url = await boto3_client.get_download_presigned_url(bucket, file_path)

        except Exception as e:
            error_msg = f'Error getting file: {str(e)}'
            self.__logger.error(error_msg)
            api_response.error_msg = error_msg
            api_response.code = EAPIResponseCode.bad_request
            return api_response.json_response()

        return {'url': presigned_url}
