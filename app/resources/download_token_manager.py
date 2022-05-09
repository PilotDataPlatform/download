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

import time

import jwt

from app.config import ConfigClass


class InvalidToken(Exception):
    pass


def verify_dataset_version_token(token):
    """verify download token with the download key."""
    try:
        res = jwt.decode(token, ConfigClass.DOWNLOAD_KEY, algorithms=['HS256'])
        return True, res
    except jwt.ExpiredSignatureError:
        return False, 'expired'
    except Exception:
        return False, 'invalid'


async def verify_download_token(token: str) -> dict:
    '''
    Summary:
        The function will decode the input token to retrieve the payload
        of file_path and return for download api. There will be two exception:
            - InvalidToken: missing the `file_path` field in payload.
            - jwt.ExpiredSignatureError: token expired

    Parameter:
        - token(string): the HS256 generate by `generate_token` function

    Return:
        - dict: the hash code
    '''

    res = jwt.decode(token, ConfigClass.DOWNLOAD_KEY, algorithms=['HS256'])
    # if the download path is missing
    if 'file_path' not in res:
        raise InvalidToken('Invalid download token')

    return res


async def generate_token(
    container_code: str,
    container_type: str,
    file_path: str,
    operator: str,
    session_id: str,
    job_id: str,
    payload: dict = None,
) -> str:
    '''
    Summary:
        The function will generate the hash code with specific download key
        by using HS256 encoding. All the parameter will be embedded into hash
        code for future verification

    Parameter:
        - container_code(string): the unique code of the container
        - container_type(string): the type of container can be project or
            dataset for now
        - file_path(string): the location of the file which will be downloaded
        - operator(string): the user who takes the operation
        - session_id(string): the unique id to track the user login session
        - job_id(string): the unique id for the job,
        - payload(dict): some of the extra infomation saved in dict

    Return:
        - str: the hash code
    '''

    if not payload:
        payload = {}

    hash_token_dict = {
        'file_path': file_path,
        'issuer': 'SERVICE DATA DOWNLOAD',
        'operator': operator,
        'session_id': session_id,
        'job_id': job_id,
        'container_code': container_code,
        'container_type': container_type,
        'payload': payload,
        'iat': int(time.time()),
        'exp': int(time.time()) + (ConfigClass.DOWNLOAD_TOKEN_EXPIRE_AT * 60),
    }

    return jwt.encode(hash_token_dict, key=ConfigClass.DOWNLOAD_KEY, algorithm='HS256').decode('utf-8')
