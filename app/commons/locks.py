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

from app.config import ConfigClass
from app.resources.helpers import get_children_nodes
from app.resources.helpers import get_resource_bygeid


class ResourceAlreadyInUsed(Exception):
    pass


async def data_ops_request(resource_key: str, operation: str, method: str) -> dict:
    url = ConfigClass.DATA_OPS_UT_V2 + 'resource/lock/'
    post_json = {'resource_key': resource_key, 'operation': operation}
    async with httpx.AsyncClient() as client:
        response = await client.request(url=url, method=method, json=post_json)
    if response.status_code != 200:
        raise Exception('resource %s already in used' % resource_key)

    return response.json()


async def lock_resource(resource_key: str, operation: str) -> dict:
    return await data_ops_request(resource_key, operation, 'POST')


async def unlock_resource(resource_key: str, operation: str) -> dict:
    return await data_ops_request(resource_key, operation, 'DELETE')


async def bulk_lock_operation(resource_key: list, operation: str, lock=True) -> dict:
    # base on the flag toggle the http methods
    method = 'POST' if lock else 'DELETE'

    # operation can be either read or write
    url = ConfigClass.DATA_OPS_UT_V2 + 'resource/lock/bulk'
    post_json = {'resource_keys': resource_key, 'operation': operation}
    async with httpx.AsyncClient() as client:
        response = await client.request(method, url, json=post_json, timeout=3600)
    if response.status_code != 200:
        raise ResourceAlreadyInUsed('resource %s already in used' % resource_key)

    return response.json()


# should be deprecated
async def recursive_lock(code: str, ff_geids: list, new_name: str = None) -> (list, Exception):
    """the function will recursively lock the node tree."""

    # this is for crash recovery, if something trigger the exception
    # we will unlock the locked node only. NOT the whole tree. The example
    # case will be copy the same node, if we unlock the whole tree in exception
    # then it will affect the processing one.
    locked_node, err = [], None

    async def recur_walker(currenct_nodes, new_name=None):
        """recursively trace down the node tree and run the lock function."""
        for ff_object in currenct_nodes:
            # we will skip the deleted nodes
            if ff_object.get('archived', False):
                continue
            # conner case here, we DONT lock the name folder
            # for the copy we will lock the both source and target
            if ff_object.get('display_path') != ff_object.get('uploader'):
                bucket_prefix = ''
                if ConfigClass.GREEN_ZONE_LABEL in ff_object.get('labels'):
                    bucket_prefix = 'gr-'
                elif ConfigClass.CORE_ZONE_LABEL in ff_object.get('labels'):
                    bucket_prefix = 'core-'
                bucket = bucket_prefix + code
                minio_obj_path = ff_object.get('display_path')

                source_key = '{}/{}'.format(bucket, minio_obj_path)
                await lock_resource(source_key, 'read')
                locked_node.append((source_key, 'read'))

            # open the next recursive loop if it is folder
            if 'Folder' in ff_object.get('labels'):
                children_nodes = await get_children_nodes(ff_object.get('global_entity_id', None))
                await recur_walker(children_nodes)

        return

    # start here
    try:
        # slightly different here, since the download only gives
        # the folder/file geid. then I have to get node by geid so
        # that we can get the path/
        nodes = [await get_resource_bygeid(geid.get('geid')) for geid in ff_geids]
        await recur_walker(nodes, new_name)
    except Exception as e:
        err = e

    return locked_node, err
