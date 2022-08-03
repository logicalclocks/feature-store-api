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

from typing import TypeVar, Optional, Dict, Any
from abc import ABC, abstractmethod


class EngineWriteBase(ABC):
    @abstractmethod
    def write_training_dataset(
        self,
        training_dataset,
        dataset,
        user_write_options,
        save_mode,
        feature_view_obj=None,
        to_df=False,
    ):
        pass

    @abstractmethod
    def add_file(self, file):
        pass

    @abstractmethod
    def register_external_temporary_table(self, external_fg, alias):
        pass

    @abstractmethod
    def register_hudi_temporary_table(
        self, hudi_fg_alias, feature_store_id, feature_store_name, read_options
    ):
        pass

    @abstractmethod
    def save_dataframe(
        self,
        feature_group,
        dataframe,
        operation,
        online_enabled,
        storage,
        offline_write_options,
        online_write_options,
        validation_id=None,
    ):
        pass

    @abstractmethod
    def save_stream_dataframe(
        self,
        feature_group,
        dataframe,
        query_name,
        output_mode,
        await_termination,
        timeout,
        checkpoint_dir,
        write_options,
    ):
        pass

    @abstractmethod
    def save_empty_dataframe(self, feature_group, dataframe):
        pass

    @abstractmethod
    def profile_by_spark(self, metadata_instance):
        pass


class EngineReadBase(ABC):
    @abstractmethod
    def read(self, storage_connector, data_format, read_options, location):
        pass

    @abstractmethod
    def read_options(self, data_format, provided_options):
        pass

    @abstractmethod
    def read_stream(
        self,
        storage_connector,
        message_format,
        schema,
        options,
        include_metadata,
    ):
        pass

    @abstractmethod
    def show(self, sql_query, feature_store, n, online_conn):
        pass

    @staticmethod
    @abstractmethod
    def get_unique_values(feature_dataframe, feature_name):
        pass

    @abstractmethod
    def get_training_data(
        self, training_dataset_obj, feature_view_obj, query_obj, read_options
    ):
        pass

    @abstractmethod
    def get_empty_appended_dataframe(self, dataframe, new_features):
        pass


class EngineUtilBase(ABC):
    @abstractmethod
    def set_job_group(self, group_id, description):
        pass

    @abstractmethod
    def sql(self, sql_query, feature_store, online_conn, dataframe_type, read_options):
        pass

    @abstractmethod
    def profile(
        self,
        dataframe,
        relevant_columns,
        correlations,
        histograms,
        exact_uniqueness=True,
    ):
        pass

    @abstractmethod
    def validate_with_great_expectations(
        self,
        dataframe,
        expectation_suite: TypeVar("ge.core.ExpectationSuite"),
        ge_validate_kwargs: Optional[Dict[Any, Any]] = {},
    ):
        pass

    @abstractmethod
    def convert_to_default_dataframe(self, dataframe):
        pass

    @abstractmethod
    def parse_schema_feature_group(self, dataframe, time_travel_format=None):
        pass

    @abstractmethod
    def parse_schema_training_dataset(self, dataframe):
        pass

    @abstractmethod
    def split_labels(self, df, labels):
        pass

    @abstractmethod
    def is_spark_dataframe(self, dataframe):
        pass

    @abstractmethod
    def create_empty_df(self, dataframe):
        pass

    @abstractmethod
    def setup_storage_connector(self, storage_connector, path=None):
        pass
