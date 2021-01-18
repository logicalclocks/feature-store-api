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

from hsfs.engine import spark
from hsfs.client import exceptions

_engine = None
_engine_type = None


def init(engine_type):
    global _engine_type
    global _engine
    if not _engine:
        if engine_type == "spark":
            _engine_type = "spark"
            _engine = spark.Engine()
        elif engine_type == "hive":
            try:
                from hsfs.engine import hive
            except ImportError:
                raise exceptions.FeatureStoreException(
                    "Trying to instantiate Hive as engine, but 'hive' extras are "
                    "missing in HSFS installation. Install with `pip install "
                    "hsfs[hive]`."
                )
            _engine_type = "hive"
            _engine = hive.Engine()


def get_instance():
    global _engine
    if _engine:
        return _engine
    raise Exception("Couldn't find execution engine. Try reconnecting to Hopsworks.")


def get_type():
    global _engine_type
    if _engine_type:
        return _engine_type
    raise Exception("Couldn't find execution engine. Try reconnecting to Hopsworks.")


def stop():
    global _engine
    _engine = None
