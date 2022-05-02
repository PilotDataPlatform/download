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

from fastapi import FastAPI

from app.routers import api_root
from app.routers.v1 import api_data_download
from app.routers.v2 import api_data_download as api_data_download_v2
from app.routers.v2 import api_object_get as api_object_get


def api_registry(app: FastAPI):
    app.include_router(api_root.router)
    app.include_router(api_data_download.router, prefix='/v1')
    app.include_router(api_data_download_v2.router, prefix='/v2')
    app.include_router(api_object_get.router, prefix='/v2')
