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


class TrainingDatasetBundle:
    def __init__(
        self,
        version,
        training_dataset=None,
        training_dataset_splits=None,
        train_split=None,
        job=None,
    ):
        self._version = version
        self._training_dataset = training_dataset
        self._training_dataset_splits = training_dataset_splits
        self._train_split = train_split
        self._job = job

    def get_dataset(self, split=None):
        if not self._job:
            if split:
                return self._training_dataset_splits.get(split)
            else:
                if self._train_split and self._training_dataset_splits:
                    return self._training_dataset_splits.get(self._train_split)
                else:
                    return self._training_dataset

    def get_split_names(self):
        if not self._job:
            return list(self._training_dataset_splits.keys())

    def get_train_split_name(self):
        return self._train_split

    def get_job(self):
        return self._job

    @property
    def version(self):
        return self._version
