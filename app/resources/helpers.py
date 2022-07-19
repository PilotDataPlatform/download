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
import time
from typing import List
from uuid import UUID

import httpx

from app.commons.data_providers.redis import SrvRedisSingleton
from app.config import ConfigClass
from app.models.base_models import EAPIResponseCode
from app.models.models_data_download import EDataDownloadStatus


class ResourceNotFound(Exception):
    pass


async def get_files_folder_recursive(
    container_code: str, container_type: str, owner: str, zone: int = 0, parent_path: str = ''
) -> List[dict]:
    '''
    Summary:
        The function will call the api into metadata service and fetch
        the file/folder object match the parameters recursively.

    Parameter:
        - container_code(str): the code of container
        - container_type(string): the type can project or dataset
        - owner(str): the owner of file/object
        - zone(int) default=0: 0 for greenroom, 1 for core
        - parent_path(str) default='': the parent folder path of target file/folder

    Return:
        - list: the list of file/folder meatch the searching parameter
    '''

    payload = {
        'container_code': container_code,
        'container_type': container_type,
        'zone': zone,
        'recursive': True,
        'archived': False,
        'parent_path': parent_path,
        'owner': owner,
        'type': 'file',
    }

    url = ConfigClass.METADATA_SERVICE + 'items/search/'
    async with httpx.AsyncClient() as client:
        res = await client.get(url, params=payload)
        if res.status_code != 200:
            raise Exception('Error when query the folder tree %s' % (str(res.text)))

    return res.json().get('result', [])


async def get_files_folder_by_id(_id: UUID) -> dict:
    '''
    Summary:
        The function will call the api into metadata service and fetch
        the file/folder object by item.

    Parameter:
        - _id(str): uuid of the file/folder

    Return:
        - dict: the detail info of item with target id
    '''

    url = ConfigClass.METADATA_SERVICE + f'item/{_id}/'
    async with httpx.AsyncClient() as client:
        res = await client.get(url)
    file_folder_object = res.json().get('result', {})

    # raise not found if the resource not exist
    if len(file_folder_object) == 0 or res.status_code == EAPIResponseCode.not_found:
        raise ResourceNotFound('resource %s does not exist' % _id)
    elif res.status_code != 200:
        raise Exception('Error when get resource: %s' % res.text)

    return file_folder_object


async def set_status(
    session_id: str,
    job_id: str,
    source: str,
    action: str,
    target_status: EDataDownloadStatus,
    project_code: str,
    operator: str,
    payload: dict = None,
) -> dict:
    '''
    Summary:
        The function will call the api into redis api and store
        the inputs as the download job status

    Parameter:
        - session_id(str): the session id for current user
        - job_id(str): the job identifier for running action
        - source(str): the source file of current action. if multiple
            files are involved in one action, the source will be the
            zip file.
        - action(str): in download service this will be marked as data_download
        - target_status(EDataDownloadStatus): the job status please
            check EDataDownloadStatus object
        - project_code(str): the unique code of project
        - operator(str): the user who takes current action
        - payload(dict) defaul=None: fields for extra infomation

    Return:
        - dict: the detail job info
    '''

    srv_redis = SrvRedisSingleton()
    my_key = 'dataaction:{}:Container:{}:{}:{}:{}:{}'.format(session_id, job_id, action, project_code, operator, source)
    payload = payload if payload else {}
    record = {
        'session_id': session_id,
        'job_id': job_id,
        'source': source,
        'action': action,
        'status': str(target_status),
        'project_code': project_code,
        'operator': operator,
        'payload': payload,
        'update_timestamp': str(round(time.time())),
    }
    my_value = json.dumps(record)
    await srv_redis.set_by_key(my_key, my_value)
    return record


async def get_status(session_id: str, job_id: str, project_code: str, action: str, operator: str = None) -> List[dict]:
    '''
    Summary:
        The function will fetch the existing job from redis by the input.
        Return empty list if job does not exist

    Parameter:
        - session_id(str): the session id for current user
        - job_id(str): the job identifier for running action
        - project_code(str): the unique code of project
        - action(str): in download service this will be marked as data_download
        - operator(str) default=None: the user who takes current action

    Return:
        - dict: the detail job info
    '''

    srv_redis = SrvRedisSingleton()
    my_key = 'dataaction:{}:Container:{}:{}:{}'.format(session_id, job_id, action, project_code)
    if operator:
        my_key = 'dataaction:{}:Container:{}:{}:{}:{}'.format(session_id, job_id, action, project_code, operator)
    res_binary = await srv_redis.mget_by_prefix(my_key)
    return [json.loads(record.decode('utf-8')) for record in res_binary] if res_binary else []
