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
        self, version, training_dataset=None, training_dataset_splits=None,
        train_split=None, job=None
    ):
        self._version = version
        self._training_dataset = training_dataset
        self._training_dataset_splits = training_dataset_splits
        self._train_split = train_split
        self._job = job

    def get_dataset(self, split=None):
        pass

    def get_split_names(self):
        pass

    def get_train_split(self):
        pass

    def get_job(self):
        pass
