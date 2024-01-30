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

from typing import Optional, List, Union

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

    def post(
        self, metadata_instance, stats, training_dataset_version
    ) -> Optional[statistics.Statistics]:
        _client = client.get_instance()
        path_params = self.get_path(metadata_instance, training_dataset_version)

        headers = {"content-type": "application/json"}
        stats = statistics.Statistics.from_response_json(
            _client._send_request(
                "POST", path_params, headers=headers, data=stats.json()
            )
        )
        return self._extract_single_stats(stats)

    def get(
        self,
        metadata_instance,
        computation_time=None,
        start_commit_time=None,
        end_commit_time=None,
        feature_names=None,
        row_percentage=None,
        before_transformation=None,
        training_dataset_version=None,
    ) -> Optional[statistics.Statistics]:
        """Get single statistics of an entity.

        :param metadata_instance: metadata object of the instance to get statistics of
        :type metadata_instance: TrainingDataset, FeatureGroup, FeatureView
        :param computation_time: Time at which statistics where computed
        :type computation_time: int
        :param start_commit_time: Window start commit time
        :type start_time: int
        :param end_commit_time: Window end commit time
        :type end_time: int
        :param feature_names: List of feature names of which statistics are retrieved
        :type feature_names: List[str]
        :param row_percentage: Percentage of feature values used during statistics computation
        :type row_percentage: float
        :param before_transformation: Whether the statistics were computed before transformations or not
        :type before_transformation: bool
        :param training_dataset_version: Version of the training dataset on which statistics were computed
        :type training_dataset_version: int
        """
        # get statistics by entity + filters + sorts, including the feature descriptive statistics
        _client = client.get_instance()
        path_params = self.get_path(metadata_instance, training_dataset_version)

        # single statistics
        offset, limit = 0, 1

        headers = {"content-type": "application/json"}
        query_params = self._build_get_query_params(
            computation_time=computation_time,
            start_commit_time=start_commit_time,
            end_commit_time=end_commit_time,
            filter_eq_times=True,
            feature_names=feature_names,
            row_percentage=row_percentage,
            before_transformation=before_transformation,
            training_dataset_version=training_dataset_version,
            # retrieve only one entity statistics, including the feature descriptive statistics
            offset=offset,
            limit=limit,
            with_content=True,
        )

        # response is either a single item or not found exception
        stats = statistics.Statistics.from_response_json(
            _client._send_request("GET", path_params, query_params, headers=headers)
        )
        return self._extract_single_stats(stats)

    def get_all(
        self,
        metadata_instance,
        computation_time=None,
        start_commit_time=None,
        end_commit_time=None,
        feature_names=None,
        row_percentage=None,
        before_transformation=None,
        training_dataset_version=None,
    ) -> Optional[List[statistics.Statistics]]:
        """Get all statistics of an entity.

        :param metadata_instance: metadata object of the instance to get statistics of
        :type metadata_instance: TrainingDataset, FeatureGroup, FeatureView
        :param computation_time: Time at which statistics where computed
        :type computation_time: int
        :param start_commit_time: Window start commit time
        :type start_commit_time: int
        :param end_commit_time: Window end commit time
        :type end_commit_time: int
        :param feature_names: List of feature names of which statistics are retrieved
        :type feature_names: List[str]
        :param row_percentage: Percentage of feature values used during statistics computation
        :type row_percentage: float
        :param before_transformation: Whether the statistics were computed before transformations or not
        :type before_transformation: bool
        :param training_dataset_version: Version of the training dataset on which statistics were computed
        :type training_dataset_version: int
        """
        # get all statistics by entity + filters + sorts, without the feature descriptive statistics
        _client = client.get_instance()
        path_params = self.get_path(metadata_instance, training_dataset_version)

        # multiple statistics
        offset, limit = 0, None

        headers = {"content-type": "application/json"}
        query_params = self._build_get_query_params(
            computation_time=computation_time,
            start_commit_time=start_commit_time,
            end_commit_time=end_commit_time,
            filter_eq_times=False,
            feature_names=feature_names,
            row_percentage=row_percentage,
            before_transformation=before_transformation,
            training_dataset_version=training_dataset_version,
            # retrieve all entity statistics, excluding feature descriptive statistics
            offset=offset,
            limit=limit,
            with_content=False,
        )

        return statistics.Statistics.from_response_json(
            _client._send_request("GET", path_params, query_params, headers=headers)
        )

    def compute(self, metadata_instance, training_dataset_version=None) -> job.Job:
        """Compute statistics for an entity.

        :param metadata_instance: metadata object of the instance to compute statistics for
        :type metadata_instance: TrainingDataset, FeatureGroup, FeatureView
        :param training_dataset_version: version of the training dataset metadata object
        :type training_dataset_version: int
        """
        _client = client.get_instance()
        path_params = self.get_path(metadata_instance, training_dataset_version) + [
            "compute"
        ]
        return job.Job.from_response_json(_client._send_request("POST", path_params))

    def get_path(self, metadata_instance, training_dataset_version=None) -> list:
        """Get statistics path.

        :param metadata_instance: metadata object of the instance to compute statistics for
        :type metadata_instance: TrainingDataset, FeatureGroup
        :param training_dataset_version: version of the training dataset metadata object
        :type training_dataset_version: int
        """
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

    def _extract_single_stats(self, stats) -> Optional[statistics.Statistics]:
        return stats[0] if isinstance(stats, list) else stats

    def _build_get_query_params(
        self,
        computation_time=None,
        start_commit_time=None,
        end_commit_time=None,
        filter_eq_times=None,
        feature_names=None,
        row_percentage=None,
        before_transformation=None,
        training_dataset_version=None,
        offset=0,
        limit=None,
        with_content=False,
    ) -> dict:
        """Build query parameters for statistics requests.

        :param computation_time: Time at which statistics where computed
        :type computation_time: int
        :param start_commit_time: Window start commit time
        :type start_commit_time: int
        :param end_commit_time: Window end commit time
        :type end_commit_time: int
        :param feature_names: List of feature names of which statistics are retrieved
        :type feature_names: List[str]
        :param row_percentage: Percentage of feature values used during statistics computation
        :type row_percentage: float
        :param before_transformation: Whether the statistics were computed for transformations or not
        :type before_transformation: bool
        :param training_dataset_version: Version of the training dataset on which statistics were computed
        :type training_dataset_version: int
        :param offset: Offset for pagination queries
        :type offset: int
        :param limit: Limit for pagination queries
        :type limit: int
        :param with_content: Whether include feature descriptive statistics in the response or not
        :type with_content: bool
        """
        query_params: dict[str, Union[int, str, List[str]]] = {"offset": offset}
        if limit is not None:
            query_params["limit"] = limit
        if with_content:
            query_params["fields"] = "content"

        sorts: List[str] = []
        filters: List[str] = []

        # filters and sorts

        # window times
        if end_commit_time is not None:
            col_name = "window_end_commit_time"
            sorts.append(col_name + ":desc")  # first sort
            filter_name = col_name + ("_eq" if filter_eq_times else "_ltoeq")
            filters.append(filter_name + ":" + str(end_commit_time))
        if start_commit_time is not None:
            col_name = "window_start_commit_time"
            sorts.append(col_name + ":asc")  # second sort
            filter_name = col_name + ("_eq" if filter_eq_times else "_gtoeq")
            filters.append(filter_name + ":" + str(start_commit_time))

        # computation time -- order is important, this should be after the other sorts
        if start_commit_time is None and end_commit_time is None:
            sorts.append("computation_time:desc")  # third sort
        if computation_time is not None:
            filters.append("computation_time_ltoeq:" + str(computation_time))

        # row percentage
        if row_percentage is not None:
            filters.append("row_percentage_eq:" + str(row_percentage))

        # for transformation
        if before_transformation is not None:
            filters.append("before_transformation_eq:" + str(before_transformation))

        # feature names
        if feature_names is not None:
            query_params["feature_names"] = feature_names

        # others
        if training_dataset_version is not None:
            query_params["training_dataset_version"] = training_dataset_version

        if sorts:
            query_params["sort_by"] = sorts
        if filters:
            query_params["filter_by"] = filters

        return query_params
