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
import os
import time
from typing import List
from uuid import UUID

import httpx

from app.commons.data_providers.redis import SrvRedisSingleton
from app.config import ConfigClass
from app.models.base_models import EAPIResponseCode

from .error_handler import internal_jsonrespon_handler


async def get_files_folder_recursive(
    container_code: str, container_type: str, owner: str, zone: int = 0, parent_path: str = ''
) -> List[dict]:

    payload = {
        'container_code': container_code,
        'container_type': container_type,
        'zone': zone,
        'recursive': True,
        'archived': False,
        'parent_path': parent_path,
        'owner': owner,
    }
    url = ConfigClass.METADATA_SERVICE + 'items/search/'
    async with httpx.AsyncClient() as client:
        res = await client.get(url, params=payload)
        if res.status_code != 200:
            raise Exception('Error when query the folder tree %s' % (str(res.text)))

    return res.json().get('result', [])


async def get_files_folder_by_id(_id: UUID) -> dict:

    url = ConfigClass.METADATA_SERVICE + f'item/{_id}/'
    async with httpx.AsyncClient() as client:
        res = await client.get(url)
    file_folder_object = res.json().get('result', {})

    # raise not found if the resource not exist
    if len(file_folder_object) == 0 or res.status_code == EAPIResponseCode.not_found:
        raise Exception('resource %s does not exist' % _id)
    elif res.status_code != 200:
        raise Exception('Error when get resource: %s' % res.text)

    return file_folder_object


# should be deprecated
async def get_children_nodes(start_geid: str) -> list:
    """The function is different than above one this one will return next layer folder or files under the start_geid."""

    payload = {
        'label': 'own',
        'start_label': 'Folder',
        'start_params': {'global_entity_id': start_geid},
    }

    node_query_url = ConfigClass.NEO4J_SERVICE + 'relations/query'
    async with httpx.AsyncClient() as client:
        response = await client.post(node_query_url, json=payload)
    ffs = [x.get('end_node') for x in response.json()]

    return ffs


# should be deprecated
async def get_resource_bygeid(geid) -> dict:
    """function will call the neo4j api to get the node by geid.

    raise exception if the geid is not exist
    """
    url = ConfigClass.NEO4J_SERVICE + 'nodes/geid/%s' % geid
    async with httpx.AsyncClient() as client:
        res = await client.get(url)
    nodes = res.json()

    if len(nodes) == 0:
        raise Exception('Not found resource: ' + geid)

    return nodes[0]


async def set_status(
    session_id, job_id, source, action, target_status, project_code, operator, geid, payload=None, progress=0
):
    """Set session job status."""

    srv_redis = SrvRedisSingleton()
    my_key = 'dataaction:{}:Container:{}:{}:{}:{}:{}'.format(session_id, job_id, action, project_code, operator, source)
    payload = payload if payload else {}
    # payload["zone"] = ConfigClass.disk_namespace
    # payload["frontend_zone"] = get_frontend_zone(ConfigClass.disk_namespace)
    record = {
        'session_id': session_id,
        'job_id': job_id,
        'geid': geid,
        'source': source,
        'action': action,
        'status': target_status,
        'project_code': project_code,
        'operator': operator,
        'progress': progress,
        'payload': payload,
        'update_timestamp': str(round(time.time())),
    }
    my_value = json.dumps(record)
    await srv_redis.set_by_key(my_key, my_value)
    return record


async def get_status(session_id, job_id, project_code, action, operator=None) -> List[str]:
    """Get session job status from datastore."""
    srv_redis = SrvRedisSingleton()
    my_key = 'dataaction:{}:Container:{}:{}:{}'.format(session_id, job_id, action, project_code)
    if operator:
        my_key = 'dataaction:{}:Container:{}:{}:{}:{}'.format(session_id, job_id, action, project_code, operator)
    res_binary = await srv_redis.mget_by_prefix(my_key)
    return [json.loads(record.decode('utf-8')) for record in res_binary] if res_binary else []


async def delete_by_session_id(session_id: str, job_id: str = '*', action: str = '*'):
    """Delete status by session id."""

    srv_redis = SrvRedisSingleton()
    prefix = 'dataaction:' + session_id + ':' + job_id + ':' + action
    await srv_redis.mdelete_by_prefix(prefix)
    return True


async def update_file_operation_logs(operator, download_path, project_code, operation_type='data_download', extra=None):
    """Endpoint."""
    # new audit log api
    url_audit_log = ConfigClass.PROVENANCE_SERVICE + 'audit-logs'
    payload = {
        'action': operation_type,
        'operator': operator,
        'target': download_path,
        'outcome': download_path,
        'resource': 'file',
        'display_name': os.path.basename(download_path),
        'project_code': project_code,
        'extra': extra if extra else {},
    }
    async with httpx.AsyncClient() as client:
        res_audit_logs = await client.post(url_audit_log, json=payload)
    return internal_jsonrespon_handler(url_audit_log, res_audit_logs)
