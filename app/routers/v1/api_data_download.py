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

from common import LoggerFactory
from fastapi import APIRouter
from fastapi.responses import FileResponse
from fastapi.responses import RedirectResponse
from fastapi_utils import cbv
from jwt import ExpiredSignatureError
from jwt.exceptions import DecodeError

from app.config import ConfigClass
from app.models.base_models import APIResponse
from app.models.base_models import EAPIResponseCode
from app.models.models_data_download import EDataDownloadStatus
from app.models.models_data_download import GetDataDownloadStatusResponse
from app.resources.download_token_manager import InvalidToken
from app.resources.download_token_manager import verify_download_token
from app.resources.error_handler import ECustomizedError
from app.resources.error_handler import catch_internal
from app.resources.error_handler import customized_error_template
from app.resources.helpers import get_status
from app.resources.helpers import set_status
from app.resources.helpers import update_file_operation_logs

router = APIRouter()

_API_TAG = 'v1/data-download'
_API_NAMESPACE = 'api_data_download'


@cbv.cbv(router)
class APIDataDownload:
    """API Data Download Class."""

    def __init__(self):
        self.__logger = LoggerFactory('api_data_download').get_logger()

    @router.get(
        '/download/status/{hash_code}',
        tags=[_API_TAG],
        response_model=GetDataDownloadStatusResponse,
        summary='Check download status',
    )
    @catch_internal(_API_NAMESPACE)
    async def data_download_status(self, hash_code):
        '''
        Summary:
            The API is to return the download status by the hashcode

        Parameter:
            - hash_code(str): hashcode return from /v1/download/pre

        Return:
            - 200
        '''

        response = APIResponse()
        # verify hash code
        try:
            res_verify_token = await verify_download_token(hash_code)
        except ExpiredSignatureError as e:
            response.code = EAPIResponseCode.unauthorized
            response.error_msg = str(e)
            return response.json_response()
        except (DecodeError, InvalidToken) as e:
            response.code = EAPIResponseCode.bad_request
            response.error_msg = str(e)
            return response.json_response()
        except Exception as e:
            response.code = EAPIResponseCode.internal_error
            response.error_msg = str(e)
            return response.json_response()

        # use retrieved the payload to get the job status
        session_id = res_verify_token.get('session_id')
        job_id = res_verify_token.get('job_id')
        project_code = res_verify_token.get('container_code')
        operator = res_verify_token.get('operator')
        job_fatched = await get_status(session_id, job_id, project_code, 'data_download', operator)
        self.__logger.info('job_fatched list: ' + str(job_fatched))

        if len(job_fatched):
            response.code = EAPIResponseCode.success
            response.result = job_fatched[0]
        else:
            self.__logger.error(f'Status not found {res_verify_token} in namespace {ConfigClass.namespace}')
            response.code = EAPIResponseCode.not_found
            response.error_msg = customized_error_template(ECustomizedError.JOB_NOT_FOUND)

        return response.json_response()

    @router.get(
        '/download/{hash_code}',
        tags=[_API_TAG],
        summary='Download the data, asynchronously streams a file as the response.',
    )
    @catch_internal(_API_NAMESPACE)
    async def data_download(self, hash_code: str):
        '''
        Summary:
            The API is the actual download api to send file to the frontend
            specified by hashcode

        Parameter:
            - hash_code(str): hashcode return from /v1/download/pre

        Return:
            - file response
        '''

        response = APIResponse()
        self.__logger.info(f'Check downloading request: {hash_code}')

        # Verify and decode token
        try:
            res_verify_token = await verify_download_token(hash_code)
        except ExpiredSignatureError as e:
            response.code = EAPIResponseCode.unauthorized
            response.error_msg = str(e)
            return response.json_response()
        except (DecodeError, InvalidToken) as e:
            response.code = EAPIResponseCode.bad_request
            response.error_msg = str(e)
            return response.json_response()
        except Exception as e:
            response.code = EAPIResponseCode.internal_error
            response.error_msg = str(e)
            return response.json_response()

        # using the file path in the token to fetch file by
        # 1. if number of file > 1, the files are zipped locally. and response
        #    will 307 redirection.
        # 2. if number = 1, the path presigned url from object storage. and
        #    the response will be 200 with file stream
        file_path = res_verify_token.get('file_path')
        if file_path.startswith('http'):
            response = RedirectResponse(file_path)
        else:
            if not os.path.exists(file_path):
                self.__logger.error(f'File not found {file_path} in namespace {ConfigClass.namespace}')
                response.code = EAPIResponseCode.not_found
                response.error_msg = customized_error_template(ECustomizedError.FILE_NOT_FOUND) % file_path
                return response.json_response()

            filename = os.path.basename(file_path)
            response = FileResponse(path=file_path, filename=filename)

        # Add download file log for project
        # will be removed after kafka consumer setup
        await update_file_operation_logs(
            res_verify_token.get('operator'),
            file_path,
            res_verify_token.get('container_code'),
        )

        # here we assume to overwrite the job with hashcode payload
        # no matter what (if the old doesnot exist or something else happens)
        status_update_res = await set_status(
            res_verify_token.get('session_id'),
            res_verify_token.get('job_id'),
            file_path,
            'data_download',
            EDataDownloadStatus.SUCCEED,
            res_verify_token.get('contianer_type'),
            res_verify_token.get('operator'),
            res_verify_token.get('payload', {}),
        )

        self.__logger.debug(status_update_res)

        return response
