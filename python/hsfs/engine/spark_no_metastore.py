#
#   Copyright 2023 Hopsworks AB
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
from __future__ import annotations


try:
    from pyspark.sql import SparkSession
except ImportError:
    pass

from hsfs.engine import spark


class Engine(spark.Engine):
    def __init__(self) -> None:
        self._spark_session = SparkSession.builder.getOrCreate()
        self._spark_context = self._spark_session.sparkContext
        self._jvm = self._spark_context._jvm

        super().__init__()

    def _sql_offline(self, sql_query, feature_store):
        # Spark no metastore does not require the
        return self._spark_session.sql(sql_query)
