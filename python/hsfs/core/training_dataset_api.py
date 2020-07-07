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

from hsfs import client, training_dataset, tag


class TrainingDatasetApi:
    def __init__(self, feature_store_id):
        self._feature_store_id = feature_store_id

    def post(self, training_dataset_instance):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "trainingdatasets",
        ]
        headers = {"content-type": "application/json"}
        return training_dataset_instance.update_from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                data=training_dataset_instance.json(),
            ),
        )

    def get(self, name, version):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "trainingdatasets",
            name,
        ]
        query_params = {"version": version}
        return training_dataset.TrainingDataset.from_response_json(
            _client._send_request("GET", path_params, query_params)[0],
        )

    def add_tag(self, training_dataset_instance, name, value):
        """Attach a name/value tag to a training dataset.

        A tag can consist of a name only or a name/value pair. Tag names are
        unique identifiers.

        :param training_dataset_instance: metadata object of training dataset
            to add the tag for
        :type training_dataset_instance: TrainingDataset
        :param name: name of the tag to be added
        :type name: str
        :param value: value of the tag to be added
        :type value: str
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "trainingdatasets",
            training_dataset_instance.id,
            "tags",
            name,
        ]
        query_params = {"value": value} if value else None
        _client._send_request("PUT", path_params, query_params=query_params)

    def delete_tag(self, training_dataset_instance, name):
        """Delete a tag from a training dataset.

        Tag names are unique identifiers.

        :param training_dataset_instance: metadata object of training dataset
            to delete the tag for
        :type training_dataset_instance: TrainingDataset
        :param name: name of the tag to be removed
        :type name: str
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "trainingdatasets",
            training_dataset_instance.id,
            "tags",
            name,
        ]
        _client._send_request("DELETE", path_params)

    def get_tags(self, training_dataset_instance, name):
        """Get the tags of a training dataset.

        Gets all tags if no tag name is specified.

        :param training_dataset_instance: metadata object of training dataset
            to get the tags for
        :type training_dataset_instance: TrainingDataset
        :param name: tag name
        :type name: str
        :return: list of tags as name/value pairs
        :rtype: list of dict
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "trainingdatasets",
            training_dataset_instance.id,
            "tags",
        ]

        if name:
            path_params.append(name)

        return tag.Tag.from_response_json(_client._send_request("GET", path_params))
