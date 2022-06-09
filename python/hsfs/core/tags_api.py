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

import json

from hsfs import client, tag, feature_view


class TagsApi:
    def __init__(self, feature_store_id, entity_type):
        """Tags endpoint for `trainingdatasets` and `featuregroups` resource.

        :param feature_store_id: id of the respective featurestore
        :type feature_store_id: int
        :param entity_type: "trainingdatasets" or "featuregroups"
        :type entity_type: str
        """
        self._feature_store_id = feature_store_id
        self._entity_type = entity_type

    def add(self, metadata_instance, name, value, training_dataset_version=None):
        """Attach a name/value tag to a training dataset or feature group.

        A tag consists of a name/value pair. Tag names are unique identifiers.
        The value of a tag can be any valid json - primitives, arrays or json objects.

        :param metadata_instance: metadata object of the instance to add the
            tag for
        :type metadata_instance: TrainingDataset, FeatureGroup
        :param name: name of the tag to be added
        :type name: str
        :param value: value of the tag to be added
        :type value: str
        """
        _client = client.get_instance()
        path_params = self.get_path(metadata_instance, training_dataset_version) + [
            name
        ]
        headers = {"content-type": "application/json"}
        json_value = json.dumps(value)
        _client._send_request("PUT", path_params, headers=headers, data=json_value)

    def delete(self, metadata_instance, name, training_dataset_version=None):
        """Delete a tag from a training dataset or feature group.

        Tag names are unique identifiers.

        :param metadata_instance: metadata object of training dataset
            to delete the tag for
        :type metadata_instance: TrainingDataset, FeatureGroup
        :param name: name of the tag to be removed
        :type name: str
        """
        _client = client.get_instance()
        path_params = self.get_path(metadata_instance, training_dataset_version) + [
            name
        ]

        _client._send_request("DELETE", path_params)

    def get(self, metadata_instance, name: str = None, training_dataset_version=None):
        """Get the tags of a training dataset or feature group.

        Gets all tags if no tag name is specified.

        :param metadata_instance: metadata object of training dataset
            to get the tags for
        :type metadata_instance: TrainingDataset, FeatureGroup
        :param name: tag name
        :type name: str
        :return: dict of tag name/values
        :rtype: dict
        """
        _client = client.get_instance()
        path_params = self.get_path(metadata_instance, training_dataset_version)

        if name is not None:
            path_params.append(name)

        return {
            tag._name: json.loads(tag._value)
            for tag in tag.Tag.from_response_json(
                _client._send_request("GET", path_params)
            )
        }

    def get_path(self, metadata_instance, training_dataset_version=None):
        _client = client.get_instance()
        if isinstance(metadata_instance, feature_view.FeatureView):
            path = [
                "project",
                _client._project_id,
                "featurestores",
                self._feature_store_id,
                "featureview",
                metadata_instance.name,
                "version",
                metadata_instance.version,
            ]
            if training_dataset_version:
                return path + [
                    "trainingdatasets",
                    "version",
                    training_dataset_version,
                    "tags",
                ]
            else:
                return path + ["tags"]
        else:
            return [
                "project",
                _client._project_id,
                "featurestores",
                self._feature_store_id,
                self._entity_type,
                metadata_instance.id,
                "tags",
            ]
