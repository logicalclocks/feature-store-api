#
#   Copyright 2021 Logical Clocks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

import json
import humps

from hsfs import util


class User:
    def __init__(
        self,
        username=None,
        email=None,
        first_name=None,
        last_name=None,
        status=None,
        secret=None,
        chosen_password=None,
        repeated_password=None,
        tos=None,
        two_factor=None,
        tours_state=None,
        max_num_projects=None,
        num_created_projects=None,
        test_user=None,
        user_account_type=None,
        num_active_projects=None,
        num_remaining_projects=None,
    ):
        self._username = username
        self._email = email
        self._first_name = first_name
        self._last_name = last_name
        self._status = status
        self._secret = secret
        self._chosen_password = chosen_password
        self._repeated_password = repeated_password
        self._tos = tos
        self._two_factor = two_factor
        self._tours_state = tours_state
        self._max_num_projects = max_num_projects
        self._num_created_projects = num_created_projects
        self._test_user = test_user
        self._user_account_type = user_account_type
        self._num_active_projects = num_active_projects
        self._num_remaining_projects = num_remaining_projects

    @classmethod
    def from_response_json(cls, json_dict):
        if json_dict:
            json_decamelized = humps.decamelize(json_dict)
            return cls(**json_decamelized)
        else:
            return None

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    @property
    def username(self):
        return self._username

    @property
    def email(self):
        return self._email

    @property
    def first_name(self):
        return self._first_name

    @property
    def last_name(self):
        return self._last_name

    @property
    def status(self):
        return self._status

    @property
    def secret(self):
        return self._secret

    @property
    def chosen_password(self):
        return self._chosen_password

    @property
    def repeated_password(self):
        return self._repeated_password

    @property
    def tos(self):
        return self._tos

    @property
    def two_factor(self):
        return self._two_factor

    @property
    def tours_state(self):
        return self._tours_state

    @property
    def max_num_projects(self):
        return self._max_num_projects

    @property
    def num_created_projects(self):
        return self._num_created_projects

    @property
    def test_user(self):
        return self._test_user

    @property
    def user_account_type(self):
        return self._user_account_type

    @property
    def num_active_projects(self):
        return self._num_active_projects

    @property
    def num_remaining_projects(self):
        return self._num_remaining_projects
