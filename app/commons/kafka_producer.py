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
from common import LoggerFactory
from fastavro import schema, schemaless_writer

from app.config import ConfigClass


class KakfaProducer:

    producer = None
    schema_path = 'app/commons'
    logger = LoggerFactory('KakfaProducer').get_logger()

    async def init_connection(self) -> None:
        '''
        Summary:
            the function for producer to connect the kafka.
        '''

        if self.producer is None:
            self.logger.info('Initializing the kafka producer')
            self.producer = AIOKafkaProducer(bootstrap_servers=ConfigClass.KAFKA_URL)
            try:
                # Get cluster layout and initial topic/partition leadership information
                await self.producer.start()
            except Exception as e:
                self.logger.error('Fail to start kafka producer:%s' % (str(e)))
                raise e

    async def close_connection(self) -> None:
        '''
        Summary:
            the function for producer to close the kafka connection.
        '''
        # Wait for all pending messages to be delivered or expire.
        if self.producer is not None:
            self.logger.info('Closing the kafka producer')
            await self.producer.stop()

    async def _send_message(self, topic: str, content: bytes) -> None:
        '''
        Summary:
            the function will send the byte message to kafka topic

        Parameter:
            - topic(str): the name of kafka topic
            - content(bytes): the byte message that will be sent to topic
        '''

        try:
            await self.producer.send_and_wait(topic, content)
        except Exception as e:
            self.logger.error('Fail to send message:%s' % (str(e)))
            raise e

    async def _validate_message(self, schema_name: str, message: dict) -> bytes:
        '''
        Summary:
            the function will validate the dict message with specified schema
            and return the byte message to caller

        Parameter:
            - schema_name(str): the name of kafka topic
            - message(dict): the message generated by service logic

        Return:
            - byte message
        '''

        bio = io.BytesIO()
        SCHEMA = schema.load_schema(os.path.join(self.schema_path, schema_name))
        schemaless_writer(bio, SCHEMA, message)

        message = bio.getvalue()  # give this message to producer

        return message

    async def create_activity_log(self, source_node: dict, schema_name: str, operator: str, topic: str):
        '''
        Summary:
            the function will validate the dict message with specified schema
            and return the byte message to caller

        Parameter:
            - source_node(dict): the source node contains item infomation
            - schema_name(str): the name of schema
            - operator(str): the user who take the action
            - topic(str): the target topic that message will be sent into

        Return:
            - byte message
        '''

        self.logger.info('Create %s activity log to topic: %s' % (operator, topic))

        message = {
            'activity_type': 'download',
            'activity_time': datetime.utcnow(),
            'item_id': source_node.get('id'),
            'item_type': source_node.get('type'),
            'item_name': source_node.get('name'),
            'item_parent_path': source_node.get('parent_path'),
            'container_code': source_node.get('container_code'),
            'container_type': source_node.get('container_type'),
            'zone': source_node.get('zone'),
            'user': operator,
            'imported_from': '',
            'changes': [],
        }

        byte_message = await self._validate_message(schema_name, message)
        await self._send_message(topic, byte_message)

        return


kakfa_producer = KakfaProducer()


async def get_kafka_producer() -> KakfaProducer:
    '''
    Summary:
        the function will initialize the producer if the function
        is called for first time

    Return:
        - KakfaProducer: the global variable
    '''

    await kakfa_producer.init_connection()

    return kakfa_producer
