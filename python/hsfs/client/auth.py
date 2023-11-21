#
#   Copyright 2020 Logical Clocks AB
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

import requests


class BearerAuth(requests.auth.AuthBase):
    """Class to encapsulate a Bearer token."""

    def __init__(self, token):
        self._token = token.strip()

    def __call__(self, r):
        r.headers["Authorization"] = "Bearer " + self._token
        return r


class ApiKeyAuth(requests.auth.AuthBase):
    """Class to encapsulate an API key."""

    def __init__(self, token):
        self._token = token.strip()

    def __call__(self, r):
        r.headers["Authorization"] = "ApiKey " + self._token
        return r
