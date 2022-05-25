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

from enum import Enum
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from uuid import UUID

from pydantic import BaseModel
from pydantic import Field

from .base_models import APIResponse


class PreDataDownloadPOST(BaseModel):
    """Pre download payload model."""

    files: List[Dict[str, Any]]
    operator: str
    container_code: str
    container_type: str
    approval_request_id: Optional[UUID] = None


class DatasetPrePOST(BaseModel):
    """Pre download dataset payload model."""

    dataset_code: str
    operator: str


class PreSignedDownload(BaseModel):
    """Pre signed download url payload for minio."""

    object_path: str


class PreSignedBatchDownload(BaseModel):
    """Pre signed download url payload for minio but accept a list."""

    object_path: list


class PreDataDownloadResponse(APIResponse):
    """Pre download response class."""

    result: dict = Field(
        {},
        example={
            'session_id': 'unique_session_id',
            'job_id': 'data-download-1613507376',
            'source': './test_project/workdir/test_project_zipped_1613507376.zip',
            'action': 'data_download',
            'status': 'ZIPPING',
            'project_code': 'test_project',
            'operator': 'zhengyang',
            'progress': 0,
            'payload': {
                'hash_code': (
                    'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJmdWxsX3BhdGgiOiIuL3Rlc3'
                    'RfcHJvamVjdC93b3JrZGlyL3Rlc3RfcHJvamVjdF96aXBwZWRfMTYxMzUwNzM3N'
                    'i56aXAiLCJpc3N1ZXIiOiJTRVJWSUNFIERBVEEgRE9XTkxPQUQgIiwib3BlcmF0'
                    'b3IiOiJ6aGVuZ3lhbmciLCJzZXNzaW9uX2lkIjoidW5pcXVlX3Nlc3Npb25faWQ'
                    'iLCJqb2JfaWQiOiJkYXRhLWRvd25sb2FkLTE2MTM1MDczNzYiLCJwcm9qZWN0X2'
                    'NvZGUiOiJ0ZXN0X3Byb2plY3QiLCJpYXQiOjE2MTM1MDczNzYsImV4cCI6MTYxM'
                    'zUwNzY3Nn0.ipzWy6y79QxRGhQQ_VWIk-Lz8Iv8zU7JHGF3ZBoNt-g'
                )
            },
            'update_timestamp': '1613507376',
        },
    )


class GetDataDownloadStatusResponse(APIResponse):
    """Get data download status."""

    result: dict = Field(
        {},
        example={
            'session_id': 'unique_session_id',
            'job_id': 'data-download-1613507376',
            'source': './test_project/workdir/test_project_zipped_1613507376.zip',
            'action': 'data_download',
            'status': 'READY_FOR_DOWNLOADING',
            'project_code': 'test_project',
            'operator': 'zhengyang',
            'progress': 0,
            'payload': {
                'hash_code': (
                    'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJmdWxsX3BhdGgiOiIuL3Rlc3'
                    'RfcHJvamVjdC93b3JrZGlyL3Rlc3RfcHJvamVjdF96aXBwZWRfMTYxMzUwNzM3N'
                    'i56aXAiLCJpc3N1ZXIiOiJTRVJWSUNFIERBVEEgRE9XTkxPQUQgIiwib3BlcmF0'
                    'b3IiOiJ6aGVuZ3lhbmciLCJzZXNzaW9uX2lkIjoidW5pcXVlX3Nlc3Npb25faWQ'
                    'iLCJqb2JfaWQiOiJkYXRhLWRvd25sb2FkLTE2MTM1MDczNzYiLCJwcm9qZWN0X2'
                    'NvZGUiOiJ0ZXN0X3Byb2plY3QiLCJpYXQiOjE2MTM1MDczNzYsImV4cCI6MTYxM'
                    'zUwNzY3Nn0.ipzWy6y79QxRGhQQ_VWIk-Lz8Iv8zU7JHGF3ZBoNt-g'
                )
            },
            'update_timestamp': '1613507385',
        },
    )


class DownloadStatusListResponse(APIResponse):
    """List data download status."""

    result: dict = Field(
        {},
        example=[
            {
                'session_id': 'unique_session_id',
                'job_id': 'data-download-1613507376',
                'source': './test_project/workdir/test_project_zipped_1613507376.zip',
                'action': 'data_download',
                'status': 'READY_FOR_DOWNLOADING',
                'project_code': 'test_project',
                'operator': 'zhengyang',
                'progress': 0,
                'payload': {
                    'hash_code': (
                        'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJmdWxsX3BhdGgiOiIuL3Rlc3'
                        'RfcHJvamVjdC93b3JrZGlyL3Rlc3RfcHJvamVjdF96aXBwZWRfMTYxMzUwNzM3N'
                        'i56aXAiLCJpc3N1ZXIiOiJTRVJWSUNFIERBVEEgRE9XTkxPQUQgIiwib3BlcmF0'
                        'b3IiOiJ6aGVuZ3lhbmciLCJzZXNzaW9uX2lkIjoidW5pcXVlX3Nlc3Npb25faWQ'
                        'iLCJqb2JfaWQiOiJkYXRhLWRvd25sb2FkLTE2MTM1MDczNzYiLCJwcm9qZWN0X2'
                        'NvZGUiOiJ0ZXN0X3Byb2plY3QiLCJpYXQiOjE2MTM1MDczNzYsImV4cCI6MTYxM'
                        'zUwNzY3Nn0.ipzWy6y79QxRGhQQ_VWIk-Lz8Iv8zU7JHGF3ZBoNt-g'
                    )
                },
                'update_timestamp': '1613507385',
            }
        ],
    )


class EDataDownloadStatus(Enum):
    INIT = 0
    CANCELLED = 1
    ZIPPING = 3
    READY_FOR_DOWNLOADING = 5
    SUCCEED = 7

    def __str__(self):
        return '%s' % self.name
