#
#   Copyright 2022 Logical Clocks AB
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

from typing import List, Optional, Union

from hsfs import (
    client,
    feature_view,
    training_dataset,
    transformation_function_attached,
)
from hsfs.client.exceptions import RestAPIError
from hsfs.constructor import query, serving_prepared_statement
from hsfs.core import explicit_provenance, job, training_dataset_job_conf


class FeatureViewApi:
    _POST = "POST"
    _GET = "GET"
    _PUT = "PUT"
    _DELETE = "DELETE"
    _VERSION = "version"
    _QUERY = "query"
    _BATCH = "batch"
    _DATA = "data"
    _PREPARED_STATEMENT = "preparedstatement"
    _TRANSFORMATION = "transformation"
    _TRAINING_DATASET = "trainingdatasets"
    _COMPUTE = "compute"
    _PROVENANCE = "provenance"
    _LINKS = "links"

    def __init__(self, feature_store_id: int) -> None:
        self._feature_store_id = feature_store_id
        self._client = client.get_instance()
        self._base_path = [
            "project",
            self._client._project_id,
            "featurestores",
            self._feature_store_id,
            "featureview",
        ]

    def post(
        self, feature_view_obj: feature_view.FeatureView
    ) -> feature_view.FeatureView:
        headers = {"content-type": "application/json"}
        return feature_view_obj.update_from_response_json(
            self._client._send_request(
                self._POST,
                self._base_path,
                headers=headers,
                data=feature_view_obj.json(),
            )
        )

    def update(self, feature_view_obj: feature_view.FeatureView) -> None:
        headers = {"content-type": "application/json"}
        self._client._send_request(
            self._PUT,
            self._base_path
            + [feature_view_obj.name, self._VERSION, feature_view_obj.version],
            headers=headers,
            data=feature_view_obj.json(),
        )

    def get_by_name(self, name: str) -> feature_view.FeatureView:
        path = self._base_path + [name]
        try:
            return [
                feature_view.FeatureView.from_response_json(fv)
                for fv in self._client._send_request(
                    self._GET, path, {"expand": ["query", "features"]}
                )["items"]
            ]
        except RestAPIError as e:
            if e.response.json().get("errorCode", "") == 270009:
                raise ValueError(
                    "Cannot get back the feature view because the query defined is no longer valid."
                    " Some feature groups used in the query may have been deleted. You can clean up this feature view on the UI"
                    " or `FeatureView.clean`."
                ) from e
            else:
                raise e

    def get_by_name_version(self, name: str, version: int) -> feature_view.FeatureView:
        path = self._base_path + [name, self._VERSION, version]
        try:
            return feature_view.FeatureView.from_response_json(
                self._client._send_request(
                    self._GET, path, {"expand": ["query", "features"]}
                )
            )
        except RestAPIError as e:
            if e.response.json().get("errorCode", "") == 270009:
                raise ValueError(
                    "Cannot get back the feature view because the query defined is no longer valid."
                    " Some feature groups used in the query may have been deleted. You can clean up this feature view on the UI"
                    " or `FeatureView.clean`."
                ) from e
            else:
                raise e

    def delete_by_name(self, name: str) -> None:
        path = self._base_path + [name]
        self._client._send_request(self._DELETE, path)

    def delete_by_name_version(self, name: str, version: int) -> None:
        path = self._base_path + [name, self._VERSION, version]
        self._client._send_request(self._DELETE, path)

    def get_batch_query(
        self,
        name: str,
        version: int,
        start_time: Optional[int],
        end_time: Optional[int],
        training_dataset_version: Optional[int] = None,
        with_label: bool = False,
        primary_keys: bool = False,
        event_time: bool = False,
        inference_helper_columns: bool = False,
        training_helper_columns: bool = False,
        is_python_engine: bool = False,
    ) -> "query.Query":
        path = self._base_path + [
            name,
            self._VERSION,
            version,
            self._QUERY,
            self._BATCH,
        ]
        return query.Query.from_response_json(
            self._client._send_request(
                self._GET,
                path,
                {
                    "start_time": start_time,
                    "end_time": end_time,
                    "with_label": with_label,
                    "with_primary_keys": primary_keys,
                    "with_event_time": event_time,
                    "inference_helper_columns": inference_helper_columns,
                    "training_helper_columns": training_helper_columns,
                    "is_hive_engine": is_python_engine,
                    "td_version": training_dataset_version,
                },
            )
        )

    def get_serving_prepared_statement(
        self, name: str, version: int, batch: bool, inference_helper_columns: bool
    ) -> List["serving_prepared_statement.ServingPreparedStatement"]:
        path = self._base_path + [
            name,
            self._VERSION,
            version,
            self._PREPARED_STATEMENT,
        ]
        headers = {"content-type": "application/json"}
        query_params = {
            "batch": batch,
            "inference_helper_columns": inference_helper_columns,
        }
        return serving_prepared_statement.ServingPreparedStatement.from_response_json(
            self._client._send_request("GET", path, query_params, headers=headers)
        )

    def get_attached_transformation_fn(
        self, name: str, version: int
    ) -> Union[
        "transformation_function_attached.TransformationFunctionAttached",
        List["transformation_function_attached.TransformationFunctionAttached"],
    ]:
        path = self._base_path + [name, self._VERSION, version, self._TRANSFORMATION]
        return transformation_function_attached.TransformationFunctionAttached.from_response_json(
            self._client._send_request("GET", path)
        )

    def create_training_dataset(
        self,
        name: str,
        version: int,
        training_dataset_obj: "training_dataset.TrainingDataset",
    ) -> "training_dataset.TrainingDataset":
        path = self.get_training_data_base_path(name, version)
        headers = {"content-type": "application/json"}
        return training_dataset_obj.update_from_response_json(
            self._client._send_request(
                "POST", path, headers=headers, data=training_dataset_obj.json()
            )
        )

    def get_training_dataset_by_version(
        self, name: str, version: int, training_dataset_version: int
    ) -> "training_dataset.TrainingDataset":
        path = self.get_training_data_base_path(name, version, training_dataset_version)
        return training_dataset.TrainingDataset.from_response_json_single(
            self._client._send_request("GET", path)
        )

    def get_training_datasets(
        self, name: str, version: int
    ) -> List["training_dataset.TrainingDataset"]:
        path = self.get_training_data_base_path(name, version)
        return training_dataset.TrainingDataset.from_response_json(
            self._client._send_request("GET", path)
        )

    def compute_training_dataset(
        self,
        name: str,
        version: int,
        training_dataset_version: int,
        td_app_conf: "training_dataset_job_conf.TrainingDatasetJobConf",
    ) -> job.Job:
        path = self.get_training_data_base_path(
            name, version, training_dataset_version
        ) + [self._COMPUTE]
        headers = {"content-type": "application/json"}
        return job.Job.from_response_json(
            self._client._send_request(
                "POST", path, headers=headers, data=td_app_conf.json()
            )
        )

    def delete_training_data(self, name: str, version: int) -> None:
        path = self.get_training_data_base_path(name, version)
        return self._client._send_request("DELETE", path)

    def delete_training_data_version(
        self, name: str, version: int, training_dataset_version: int
    ) -> None:
        path = self.get_training_data_base_path(name, version, training_dataset_version)
        return self._client._send_request("DELETE", path)

    def delete_training_dataset_only(self, name: str, version: int) -> None:
        path = self.get_training_data_base_path(name, version) + [self._DATA]
        return self._client._send_request("DELETE", path)

    def delete_training_dataset_only_version(
        self, name: str, version: int, training_dataset_version: int
    ) -> None:
        path = self.get_training_data_base_path(
            name, version, training_dataset_version
        ) + [self._DATA]

        return self._client._send_request("DELETE", path)

    def get_training_data_base_path(
        self, name: str, version: int, training_data_version: Optional[int] = None
    ) -> List[Union[str, int]]:
        if training_data_version:
            return self._base_path + [
                name,
                self._VERSION,
                version,
                self._TRAINING_DATASET,
                self._VERSION,
                training_data_version,
            ]
        else:
            return self._base_path + [
                name,
                self._VERSION,
                version,
                self._TRAINING_DATASET,
            ]

    def get_parent_feature_groups(
        self, name: str, version: int
    ) -> explicit_provenance.Links:
        """Get the parents of this feature view, based on explicit provenance.
        Parents are feature groups or external feature groups. These feature
        groups can be accessible, deleted or inaccessible.
        For deleted and inaccessible feature groups, only a minimal information is
        returned.

        # Arguments
            feature_view_instance: Metadata object of feature view.

        # Returns
            `ExplicitProvenance.Links`: the feature groups used to generated this
            feature view
        """
        _client = client.get_instance()
        path_params = self._base_path + [
            name,
            self._VERSION,
            version,
            self._PROVENANCE,
            self._LINKS,
        ]
        query_params = {
            "expand": "provenance_artifacts",
            "upstreamLvls": 1,
            "downstreamLvls": 0,
        }
        links_json = _client._send_request("GET", path_params, query_params)
        return explicit_provenance.Links.from_response_json(
            links_json,
            explicit_provenance.Links.Direction.UPSTREAM,
            explicit_provenance.Links.Type.FEATURE_GROUP,
        )

    def get_models_provenance(
        self,
        feature_view_name: str,
        feature_view_version: int,
        training_dataset_version: Optional[int] = None,
    ) -> "explicit_provenance.Links":
        """Get the generated models using this feature view, based on explicit
        provenance. These models can be accessible or inaccessible. Explicit
        provenance does not track deleted generated model links, so deleted
        will always be empty.
        For inaccessible models, only a minimal information is returned.

        # Arguments
            feature_view_name: Filter generated models based on feature view name.
            feature_view_version: Filter generated models based on feature view version.
            training_dataset_version: Filter generated models based on the used training dataset version.

        # Returns
            `ExplicitProvenance.Links`: the models generated using this feature
            group
        """
        _client = client.get_instance()
        path_params = self._base_path + [
            feature_view_name,
            self._VERSION,
            feature_view_version,
            self._PROVENANCE,
            self._LINKS,
        ]
        query_params = {
            "expand": "provenance_artifacts",
            "upstreamLvls": 0,
            "downstreamLvls": 2,
        }
        links_json = _client._send_request("GET", path_params, query_params)
        return explicit_provenance.Links.from_response_json(
            links_json,
            explicit_provenance.Links.Direction.DOWNSTREAM,
            explicit_provenance.Links.Type.MODEL,
            training_dataset_version=training_dataset_version,
        )
