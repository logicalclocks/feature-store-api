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
import importlib.util

from hsfs import code
from hsfs.core import code_api
import json
import os


class CodeEngine:
    WEB_PROXY_ENV = "APPLICATION_WEB_PROXY_BASE"

    # JUPYTER
    KERNEL_ENV = "HOPSWORKS_KERNEL_ID"

    # JOB
    JOB_ENV = "HOPSWORKS_JOB_NAME"

    # DATABRICKS
    DATABRICKS_EXTRA_CONTEXT = "extraContext"
    DATABRICKS_NOTEBOOK_PATH = "notebook_path"
    DATABRICKS_TAGS = "tags"
    DATABRICKS_BROWSER_HOST_NAME = "browserHostName"

    def __init__(self, feature_store_id, entity_type):
        self._code_api = code_api.CodeApi(feature_store_id, entity_type)

    def save_code(self, metadata_instance):
        """Compute code for a dataframe and send the result json to Hopsworks."""

        # JUPYTER
        kernel_id = os.environ.get(CodeEngine.KERNEL_ENV)
        # JOB
        job_name = os.environ.get(CodeEngine.JOB_ENV)
        # DATABRICKS
        try:
            databricks = importlib.util.find_spec("pyspark.dbutils")
        except ModuleNotFoundError:
            databricks = False

        web_proxy = os.environ.get(CodeEngine.WEB_PROXY_ENV)
        code_entity = code.Code(
            commit_time=int(float(datetime.datetime.now().timestamp()) * 1000),
            application_id=web_proxy[7:] if web_proxy else None,
        )

        if kernel_id:
            self._code_api.post(
                metadata_instance=metadata_instance,
                code=code_entity,
                entity_id=kernel_id,
                code_type=RunType.JUPYTER,
            )
        elif job_name:
            self._code_api.post(
                metadata_instance=metadata_instance,
                code=code_entity,
                entity_id=job_name,
                code_type=RunType.JOB,
            )
        elif databricks:
            from pyspark.dbutils import DBUtils

            dbuts = DBUtils()

            context = json.loads(
                dbuts.notebook.entry_point.getDbutils().notebook().getContext().toJson()
            )
            notebook_path = context[CodeEngine.DATABRICKS_EXTRA_CONTEXT].get(
                CodeEngine.DATABRICKS_NOTEBOOK_PATH
            )
            browser_host_name = context[CodeEngine.DATABRICKS_TAGS].get(
                CodeEngine.DATABRICKS_BROWSER_HOST_NAME
            )

            # Save HTML
            self._code_api.post(
                metadata_instance=metadata_instance,
                code=code_entity,
                entity_id=notebook_path,
                code_type=RunType.DATABRICKS,
                databricks_cluster_id=browser_host_name,
            )


class RunType:
    JUPYTER = "JUPYTER"
    JOB = "JOB"
    DATABRICKS = "DATABRICKS"
