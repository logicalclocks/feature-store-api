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

from hsfs import client, training_dataset
from hsfs.constructor.query import Query
from hsfs.core import job
from hsfs.constructor import serving_prepared_statement
from hsfs.constructor.join_suggestion import JoinSuggestion

class TrainingDatasetWizardApi:

    def __init__(self, feature_store_id):
        self._feature_store_id = feature_store_id

    def discover(self, training_dataset_wizard_instance):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "trainingdatasetwizard",
            "discover"
        ]
        headers = {"content-type": "application/json"}
        return training_dataset_wizard_instance.update_from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                data=training_dataset_wizard_instance.json(),
            ),
        )


    def construct_query(self, training_dataset_wizard_instance):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "trainingdatasetwizard",
            "constructquery"
        ]
        headers = {"content-type": "application/json"}
        return Query._hopsworks_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                data=training_dataset_wizard_instance.json(),
            ),
        )

    def feature_selection(self, training_dataset_wizard_instance):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "trainingdatasetwizard",
            "featureselection"
        ]
        headers = {"content-type": "application/json"}
        return training_dataset_wizard_instance.update_from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                data=training_dataset_wizard_instance.json(),
            ),
        )