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

import httpx
from logger import LoggerFactory
from minio import Minio
from minio.credentials.providers import ClientGrantsProvider
from minio.error import S3Error
from starlette.concurrency import run_in_threadpool

from app.config import ConfigClass

logger = LoggerFactory(__name__).get_logger()


async def get_minio_client(access_token=None, refresh_token=None):
    minio_client = Minio_Client(access_token, refresh_token)
    await minio_client.get_client()
    return minio_client


class Minio_Client:
    def __init__(self, access_token, refresh_token):
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.__client = None

    async def get_client(self):
        if not self.access_token and not self.refresh_token:
            credentials = {
                'access_key': ConfigClass.MINIO_ACCESS_KEY,
                'secret_key': ConfigClass.MINIO_SECRET_KEY,
                'secure': ConfigClass.MINIO_HTTPS,
            }
        else:
            credentials = {
                'credentials': await run_in_threadpool(self.get_provider),
                'secure': ConfigClass.MINIO_HTTPS,
            }
        self.__client = Minio(ConfigClass.MINIO_ENDPOINT, **credentials)
        logger.info('Minio Connection Success')

    @classmethod
    def parse_minio_location(self, location):
        minio_path = location.split('//')[-1]
        _, bucket, obj_path = tuple(minio_path.split('/', 2))
        return bucket, obj_path

    # function helps to get new token/refresh the token
    def _get_jwt(self):
        # enable the token exchange with different azp
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        payload = {
            'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
            'subject_token': self.access_token.replace('Bearer ', ''),
            'subject_token_type': 'urn:ietf:params:oauth:token-type:access_token',
            'requested_token_type': 'urn:ietf:params:oauth:token-type:refresh_token',
            'client_id': 'minio',
            'client_secret': ConfigClass.KEYCLOAK_MINIO_SECRET,
        }
        with httpx.Client() as client:
            result = client.post(ConfigClass.KEYCLOAK_URL, data=payload, headers=headers)
        if result.status_code != 200:
            raise Exception('Token refresh failed with ' + str(result.json()))
        self.access_token = result.json().get('access_token')
        self.refresh_token = result.json().get('refresh_token')

        jwt_object = result.json()
        return jwt_object

    # use the function above to create a credential object in minio
    # it will use the jwt function to refresh token if token expired
    def get_provider(self):
        minio_http = ('https://' if ConfigClass.MINIO_HTTPS else 'http://') + ConfigClass.MINIO_ENDPOINT
        provider = ClientGrantsProvider(
            self._get_jwt,
            minio_http,
        )

        return provider

    async def fget_object(self, obj, tmp_folder):
        # minio location is minio://http://<end_point>/bucket/user/object_path
        bucket, obj_path = self.__class__.parse_minio_location(obj['location'])

        try:
            await run_in_threadpool(self.__client.fget_object, bucket, obj_path, tmp_folder + '/' + obj_path)
        except S3Error as e:
            if e.code == 'NoSuchKey':
                logger.info('File not found, skipping: ' + str(e))
            else:
                raise e

    async def stat_object(self, bucket, file_path):
        return await run_in_threadpool(self.__client.stat_object, bucket, file_path)

    async def get_object(self, bucket, file_path):
        return await run_in_threadpool(self.__client.get_object, bucket, file_path)
