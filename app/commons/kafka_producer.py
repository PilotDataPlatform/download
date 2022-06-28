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

import io
import os
from datetime import datetime

from aiokafka import AIOKafkaProducer
from fastavro import schema
from fastavro import schemaless_writer


async def get_kafka_producer(endpoint: str, topic: str):
    kakfa_producer = KakfaProducer(endpoint, topic)
    await kakfa_producer.init_connection()

    return kakfa_producer


class KakfaProducer:
    def __init__(self, endpoint: str, topic: str) -> None:
        self.endpoint = endpoint
        self.topic = topic
        self.schema_path = 'app/commons'

    async def init_connection(self):
        self.producer = AIOKafkaProducer(bootstrap_servers=self.endpoint)

    async def _send_message(self, content: bytes) -> dict:

        # Get cluster layout and initial topic/partition leadership information
        await self.producer.start()
        try:
            # Produce message
            await self.producer.send_and_wait(self.topic, content)
        finally:
            # Wait for all pending messages to be delivered or expire.
            await self.producer.stop()

    async def _validate_message(self, schema_name: str, message: dict) -> bytes:

        bio = io.BytesIO()
        SCHEMA = schema.load_schema(os.path.join(self.schema_path, schema_name))
        schemaless_writer(bio, SCHEMA, message)

        message = bio.getvalue()  # give this message to producer

        return message

    async def create_activity_log(self, source_node: dict, schema_name: str, operator: str):

        message = {
            'activity_type': 'upload',
            'activity_time': datetime.now(),
            'item_id': source_node.get('id'),
            'item_type': source_node.get('item_type'),
            'item_name': source_node.get('item_name'),
            'item_parent_path': source_node.get('item_parent_path'),
            'container_code': source_node.get('container_code'),
            'container_type': source_node.get('container_type'),
            'zone': source_node.get('zone'),
            'user': operator,
            'imported_from': '',
            'changes': [],
        }

        byte_message = await self._validate_message(schema_name, message)
        # print(byte_message)

        await self._send_message(byte_message)

        return


# items = {
#     'activity_type': 'upload',
#     'activity_time': '2022-04-13 13:30:10.890347',
#     'id': '2955ed78-d9e5-4a06-898e-a26db470d132',
#     'item_type': 'file',
#     'item_name': 'file_name.txt',
#     'item_parent_path': 'erik.folder_name',
#     'container_code': 'erik_container',
#     'container_type': 'project',
#     'zone': 0,
#     'user': 'erik',
#     'imported_from': "",
#     'changes': [],
# }

# async def main():
#     kp = await get_kafka_producer('10.3.7.113:9092', 'test_topic_1')

#     await kp.create_activity_log(items, 'metadata_items_activity.avsc', 'admin')


# loop = asyncio.get_event_loop()
# loop.run_until_complete(main())
