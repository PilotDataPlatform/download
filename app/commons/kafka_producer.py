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
from datetime import datetime

import avro.io
from kafka import KafkaProducer


class KakfaProducer:
    def __init__(self, endpoint: str) -> None:
        self.producer = KafkaProducer(bootstrap_servers=[endpoint])

    def _send_message(self, topic: str, content: bytes) -> dict:
        self.producer.send(topic, content)
        self.producer.flush()

    def _get_schema_encoder(self, schema_name: str):
        schema = open(schema_name).read()
        SCHEMA = avro.schema.parse(schema)
        writer = avro.io.DatumWriter(SCHEMA)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)

        return encoder, writer

    def create_activity_log(self, source_node: dict, schema_name: str, operator: str):

        encoder, writer = self._get_schema_encoder(schema_name)

        message = {
            'activity_type': 'upload',
            'activity_time': datetime.now(tz=avro.timezones.utc),
            'item_id': source_node.get('id'),
            'item_type': source_node.get('item_type'),
            'item_name': source_node.get('item_name'),
            'item_parent_path': source_node.get('item_parent_path'),
            'container_code': source_node.get('container_code'),
            'container_type': source_node.get('container_type'),
            'zone': source_node.get('zone'),
            'user': operator,
            'changes': [],
        }

        writer.write(message, encoder)
        # raw_bytes = bytes_writer.getvalue()


SCHEMA = avro.schema.parse(open('metadata.items.activity.avsc').read())
writer = avro.io.DatumWriter(SCHEMA)
bytes_writer = io.BytesIO()
encoder = avro.io.BinaryEncoder(bytes_writer)

producer = KafkaProducer(bootstrap_servers=['10.3.7.113:9092'])

message = {
    'activity_type': 'upload',
    'activity_time': '2022-04-13 13:30:10.890347',
    'item_id': '2955ed78-d9e5-4a06-898e-a26db470d132',
    'item_type': 'file',
    'item_name': 'file_name.txt',
    'item_parent_path': 'erik.folder_name',
    'container_code': 'erik_container',
    'container_type': 'project',
    'zone': 0,
    'user': 'erik',
    'imported_from': None,
    'changes': [],
}

writer.write(message, encoder)

raw_bytes = bytes_writer.getvalue()

producer.send('metadata.items.activity', raw_bytes)
producer.flush()
# print('message sent')
