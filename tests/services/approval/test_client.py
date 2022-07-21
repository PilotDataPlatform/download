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

import pytest

from app.services.approval.client import ApprovalServiceClient
from app.services.approval.models import ApprovalEntities

pytestmark = pytest.mark.asyncio


@pytest.fixture
def approval_service_client(engine, metadata):
    yield ApprovalServiceClient(engine, metadata)


class TestApprovalServiceClient:
    async def test_get_approval_entities_returns_instance_of_approval_entities(self, approval_service_client, faker):
        request_id = faker.uuid4()

        result = await approval_service_client.get_approval_entities(request_id)
        assert isinstance(result, ApprovalEntities)
