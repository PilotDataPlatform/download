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

import jwt

from app.config import ConfigClass

from .error_handler import ECustomizedError
from .error_handler import customized_error_template


def verify_dataset_version_token(token):
    """verify download token with the download key."""
    try:
        res = jwt.decode(token, ConfigClass.DOWNLOAD_KEY, algorithms=['HS256'])
        return True, res
    except jwt.ExpiredSignatureError:
        return False, 'expired'
    except Exception:
        return False, 'invalid'


def verify_download_token(token):
    """verify download token with the download key."""
    try:
        res = jwt.decode(token, ConfigClass.DOWNLOAD_KEY, algorithms=['HS256'])
        for param in ['full_path']:
            if param not in res:
                # forged token probably
                return False, customized_error_template(ECustomizedError.FORGED_TOKEN)
        return True, res
    except jwt.ExpiredSignatureError:
        return False, customized_error_template(ECustomizedError.TOKEN_EXPIRED)
    except Exception as e:
        return False, customized_error_template(ECustomizedError.INVALID_TOKEN) % str(e)


def generate_token(payload: dict):
    """generate jwt token with the download key."""
    return jwt.encode(payload, key=ConfigClass.DOWNLOAD_KEY, algorithm='HS256').decode('utf-8')
