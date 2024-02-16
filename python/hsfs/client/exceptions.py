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

from enum import Enum


class RestAPIError(Exception):
    """REST Exception encapsulating the response object and url."""

    class FeatureStoreErrorCode(int, Enum):
        FEATURE_GROUP_COMMIT_NOT_FOUND = 270227
        STATISTICS_NOT_FOUND = 270228

        def __eq__(self, other):
            if isinstance(other, int):
                return self.value == other
            if isinstance(other, self.__class__):
                return self is other
            return False

    def __init__(self, url, response):
        try:
            error_object = response.json()
        except Exception:
            error_object = {}
        message = (
            "Metadata operation error: (url: {}). Server response: \n"
            "HTTP code: {}, HTTP reason: {}, body: {}, error code: {}, error msg: {}, user "
            "msg: {}".format(
                url,
                response.status_code,
                response.reason,
                response.content,
                error_object.get("errorCode", ""),
                error_object.get("errorMsg", ""),
                error_object.get("usrMsg", ""),
            )
        )
        super().__init__(message)
        self.url = url
        self.response = response


class UnknownSecretStorageError(Exception):
    """This exception will be raised if an unused secrets storage is passed as a parameter."""


class FeatureStoreException(Exception):
    """Generic feature store exception"""


class DataValidationException(FeatureStoreException):
    """Raised when data validation fails only when using "STRICT" validation ingestion policy."""

    def __init__(self, message):
        super().__init__(message)


class ExternalClientError(TypeError):
    """Raised when external client cannot be initialized due to missing arguments."""

    def __init__(self, missing_argument):
        message = (
            "{0} cannot be of type NoneType, {0} is a non-optional "
            "argument to connect to hopsworks from an external environment."
        ).format(missing_argument)
        super().__init__(message)
