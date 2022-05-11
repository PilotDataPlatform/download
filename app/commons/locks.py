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


class ResourceAlreadyInUsed(Exception):
    pass


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
