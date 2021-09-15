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

import datetime

from hsfs import code
from hsfs.core import code_api
import os


class CodeEngine:

    KERNEL_ENV = "HOPSWORKS_KERNEL_ID"
    WEB_PROXY_ENV = "APPLICATION_WEB_PROXY_BASE"

    def __init__(self, feature_store_id, entity_type):
        self._code_api = code_api.CodeApi(feature_store_id, entity_type)

    def save_code(self, metadata_instance):
        """Compute code for a dataframe and send the result json to Hopsworks."""
        kernel_id = os.environ.get(CodeEngine.KERNEL_ENV)
        if not kernel_id:
            return

        web_proxy = os.environ.get(CodeEngine.WEB_PROXY_ENV)
        code_entity = code.Code(
            commit_time=int(float(datetime.datetime.now().timestamp()) * 1000),
            application_id=web_proxy[7:] if web_proxy else None,
        )
        self._code_api.post(metadata_instance, code_entity, kernel_id, RunType.JUPYTER)


class RunType:
    JUPYTER = "JUPYTER"
    JOB = "JOB"
    DATABRICKS = "DATABRICKS"
