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

import random

from app.services.approval.models import (
    ApprovalEntity,
    CopyStatus,
    EntityType,
    ReviewStatus,
)


class TestApprovalEntity:
    def test_model_creates_successfully(self, faker):
        ApprovalEntity(
            id=faker.uuid4(),
            request_id=faker.uuid4(),
            entity_geid=faker.uuid4(),
            entity_type=random.choice(list(EntityType)),
            review_status=random.choice(list(ReviewStatus)),
            parent_geid=faker.uuid4(),
            copy_status=random.choice(list(CopyStatus)),
            name=faker.word(),
        )
