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

from typing import Optional, Union, Any, Dict, List, TypeVar
from datetime import datetime
from hsfs import constructor, tag

class FeatureView:

    def delete(self):
        pass

    def update_description(self, description):
        return self

    def get_query(self):
        return constructor.query.Query

    def get_batch_query(
        self,
        start_time: Optional[datetime],
        end_time: Optional[datetime]):
        return ""

    def get_online_vector(
        self,
        entry: Dict[str, Any],
        replace: Optional[Dict],
        external: Optional[bool] = False
    ):
        return list()

    def get_online_vectors(
        self,
        entry: Dict[str, Any],
        replace: Optional[Dict],
        external: Optional[bool] = False
    ):
        return list(list())

    def preview_online_vector(self):
        return list()

    def preview_online_vectors(self, n: int):
        return list(list())

    def add_tag(self, name: str, value):
        pass

    def get_tag(self, name: str):
        return tag.Tag()

    def get_tags(self):
        return list(list(tag.Tag()))

    def delete_tag(self, name: str):
        pass

    def register_transformation_statistics(self, version: int):
        pass

    def add_training_dataset_tag(
        self,
        version: int,
        name: str,
        value
    ):
        pass

    def get_training_dataset_tag(
        self,
        version: int,
        name: str
    ):
        return tag.Tag()

    def get_training_dataset_tags(self, version: int):
        return list(list(tag.Tag()))

    def delete_training_dataset_tag(
        self,
        version: int,
        name: str
    ):
        pass

    def get_training_data(
        self,
        version: int,
        split: str,

    ):
        pass

    def purge_training_data(self, version: int):
        pass

    def purge_all_training_data(self):
        pass

    def delete_training_dataset(self, version: int):
        pass

    def delete_all_training_datasets(self):
        pass

