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

from hsfs import client, statistics, feature_view
from hsfs.core import job


class StatisticsApi:
    def __init__(self, feature_store_id, entity_type):
        """Statistics endpoint for `trainingdatasets` and `featuregroups` resource.

        :param feature_store_id: id of the respective featurestore
        :type feature_store_id: int
        :param entity_type: "trainingdatasets" or "featuregroups"
        :type entity_type: str
        """
        self._feature_store_id = feature_store_id
        self._entity_type = entity_type  # TODO: Support FV

    def post(self, metadata_instance, stats, training_dataset_version):
        _client = client.get_instance()
        path_params = self.get_path(metadata_instance, training_dataset_version)

        headers = {"content-type": "application/json"}
        return statistics.Statistics.from_response_json(
            _client._send_request(
                "POST", path_params, headers=headers, data=stats.json()
            )
        )

    def get_last_computed(
        self, metadata_instance, for_transformation, training_dataset_version
    ):
        """Get the last computed statistics.

        These statistics are not necessarily computed on the last feature values. For instance, they could be statistics
        computed on a specific commit time window of a time-travel-enabled feature group.

        This method is used for feature groups statistics without time-travel enabled or training dataset statistics.
        """
        _client = client.get_instance()
        path_params = self.get_path(metadata_instance, training_dataset_version)
        headers = {"content-type": "application/json"}
        query_params = {
            "sort_by": "commit_time:desc",
            "offset": 0,
            "limit": 1,
            "fields": "content",
        }

        if for_transformation is not None:
            query_params["for_transformation"] = for_transformation

        # get statistics by feature group + filter (commit_time:desc)

        return statistics.Statistics.from_response_json(
            _client._send_request("GET", path_params, query_params, headers=headers)
        )

    def get_computed_at(
        self,
        metadata_instance,
        computed_at,
        for_transformation,
        training_dataset_version,
        transformed_with_version,
    ):
        """Gets the statistics computed at a specific time for an instance."""
        _client = client.get_instance()
        path_params = self.get_path(metadata_instance, training_dataset_version)

        headers = {"content-type": "application/json"}
        query_params = {
            "filter_by": "commit_time_eq:" + str(computed_at),
            "fields": "content",
        }

        if for_transformation is not None:
            query_params["for_transformation"] = for_transformation
        if transformed_with_version is not None:
            query_params["transformed_with_version"] = transformed_with_version

        # get statistics by feature group + filter (commit_time)

        return statistics.Statistics.from_response_json(
            _client._send_request("GET", path_params, query_params, headers=headers)
        )

    def get_by_time_window(
        self,
        metadata_instance,
        start_time,
        end_time,
        is_event_time=False,
        feature_name=None,
        row_percentage=None,
        transformed_with_version=None,
        computed_at=None,
    ):
        """Gets statistics computed on a specific time window and (optionally) feature name and row percentage.
        If the instance type is Feature Group, the window is based on commit times. Otherwise, if the instance type
        is Feature View, the window can be based either on commit times or event times.

        To fetch Feature View statistics based on event times, the statistics computation time (i.e., computed_at parameter) is required.
        """
        _client = client.get_instance()
        path_params = self.get_path(metadata_instance)
        headers = {"content-type": "application/json"}
        query_params = {
            "start_time": start_time,
            "end_time": end_time,
            "is_event_time": is_event_time,
            "fields": "content",
        }
        if computed_at is not None:
            query_params["computed_at"] = computed_at
        if feature_name is not None:
            query_params["feature_name"] = feature_name
        if row_percentage is not None:
            query_params["row_percentage"] = row_percentage
        if transformed_with_version is not None:
            query_params["transformed_with_version"] = transformed_with_version

        # get by feature group + window + [commit_time] + [feature_name]

        return statistics.Statistics.from_response_json(
            _client._send_request("GET", path_params, query_params, headers=headers)
        )

    def compute(self, metadata_instance, training_dataset_version=None):
        _client = client.get_instance()
        path_params = self.get_path(metadata_instance, training_dataset_version) + [
            "compute"
        ]
        return job.Job.from_response_json(_client._send_request("POST", path_params))

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
            if training_dataset_version is not None:
                path += [
                    "trainingdatasets",
                    "version",
                    training_dataset_version,
                ]
            return path + ["statistics"]
        else:
            return [
                "project",
                _client._project_id,
                "featurestores",
                self._feature_store_id,
                self._entity_type,
                metadata_instance.id,
                "statistics",
            ]
