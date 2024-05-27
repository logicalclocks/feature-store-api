#
#   Copyright 2022 Hopsworks AB
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
import os

import pytest


FIXTURES_DIR = os.path.dirname(os.path.abspath(__file__))

FIXTURES = [
    "execution",
    "expectation_suite",
    "external_feature_group_alias",
    "external_feature_group",
    "feature",
    "feature_descriptive_statistics",
    "feature_group_commit",
    "feature_group",
    "feature_monitoring_config",
    "feature_monitoring_result",
    "feature_store",
    "feature_view",
    "filter",
    "fs_query",
    "ge_expectation",
    "ge_validation_result",
    "hudi_feature_group_alias",
    "ingestion_job",
    "inode",
    "job",
    "join",
    "logic",
    "prepared_statement_parameter",
    "query",
    "serving_prepared_statement",
    "split_statistics",
    "statistics_config",
    "statistics",
    "storage_connector",
    "tag",
    "training_dataset_feature",
    "training_dataset",
    "training_dataset_split",
    "transformation_function",
    "user",
    "validation_report",
    "serving_keys",
    "rondb_server",
    "spine_group",
]

backend_fixtures_json = {}
for fixture in FIXTURES:
    with open(os.path.join(FIXTURES_DIR, f"{fixture}_fixtures.json"), "r") as json_file:
        backend_fixtures_json[fixture] = json.load(json_file)


@pytest.fixture
def backend_fixtures():
    return backend_fixtures_json
