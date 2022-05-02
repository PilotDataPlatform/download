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

from uuid import uuid4

from sqlalchemy import Column
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.future import Engine

from app.services.approval.models import ApprovalEntities


class ApprovalServiceClient:
    """Get information about approval request or entities for copy request."""

    def __init__(self, engine: Engine, metadata: MetaData) -> None:
        self.engine = engine
        self.metadata = metadata
        self.approval_entity = Table(
            'approval_entity',
            self.metadata,
            Column('id', UUID(as_uuid=True), unique=True, primary_key=True, default=uuid4),
            keep_existing=True,
        )

    async def get_approval_entities(self, request_id: str) -> ApprovalEntities:
        """Return all approval entities related to request id."""
        statement = select(self.approval_entity).filter_by(request_id=request_id)
        async with self.engine.begin() as conn:
            cursor = await conn.execute(statement)

        request_approval_entities = ApprovalEntities.from_cursor(cursor)

        return request_approval_entities
