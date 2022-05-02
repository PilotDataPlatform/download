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
from typing import Optional
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy.engine import CursorResult


class EntityType(str, Enum):
    FOLDER = 'folder'
    FILE = 'file'


class ReviewStatus(str, Enum):
    DENIED = 'denied'
    PENDING = 'pending'
    APPROVED = 'approved'


class CopyStatus(str, Enum):
    PENDING = 'pending'
    COPIED = 'copied'


class ApprovalEntity(BaseModel):
    """Model to represent one approval entity."""

    id: UUID
    request_id: Optional[UUID]
    entity_geid: Optional[str]
    entity_type: Optional[EntityType]
    review_status: Optional[ReviewStatus]
    parent_geid: Optional[str]
    copy_status: Optional[CopyStatus]
    name: str

    class Config:
        orm_mode = True


class ApprovalEntities(dict):
    """Store multiple approval entities from one request using entity geid as a key."""

    @classmethod
    def from_cursor(cls, result: CursorResult) -> 'ApprovalEntities':
        """Load approval entities from sqlalchemy cursor result."""
        instance = cls()
        for entity in result:
            approval_entity = ApprovalEntity.from_orm(entity)
            instance[approval_entity.entity_geid] = approval_entity

        return instance
