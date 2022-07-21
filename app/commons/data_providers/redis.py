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

from aioredis import StrictRedis
from common import LoggerFactory

from app.config import ConfigClass

_logger = LoggerFactory('SrvRedisSingleton').get_logger()


class SrvRedisSingleton:

    __instance = {}

    REDIS = StrictRedis(
        host=ConfigClass.REDIS_HOST,
        port=ConfigClass.REDIS_PORT,
        db=ConfigClass.REDIS_DB,
        password=ConfigClass.REDIS_PASSWORD,
    )

    async def set_by_key(self, key: str, content: str):
        await self.REDIS.set(key, content)
        _logger.debug('redis set by key: ' + key + ':  ' + content)

    async def mget_by_prefix(self, prefix: str):
        _logger.debug(prefix)
        query = '{}:*'.format(prefix)
        keys = await self.REDIS.keys(query)
        return await self.REDIS.mget(keys)

    async def mdelete_by_prefix(self, prefix: str):
        _logger.debug(prefix)
        query = '{}:*'.format(prefix)
        keys = await self.REDIS.keys(query)
        for key in keys:
            await self.REDIS.delete(key)

    async def ping(self):
        return await self.REDIS.ping()
