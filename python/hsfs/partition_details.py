#
#   Copyright 2023 Hopsworks AB
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

import humps


class PartitionDetails:
    def __init__(
        self,
        id,
        leader,
        replicas,
        in_sync_replicas,
    ):
        self._id = id
        self._leader = leader
        self._replicas = replicas
        self._in_sync_replicas = in_sync_replicas

    def to_dict(self):
        return {
            "id": self._id,
            "leader": self._leader,
            "replicas": self._replicas,
            "inSyncReplicas": self._in_sync_replicas,
        }

    @classmethod
    def from_response_json(cls, json_dict):
        if json_dict is None:
            return None

        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    @property
    def id(self):
        return self._id

    @property
    def leader(self):
        return self._leader

    @property
    def replicas(self):
        return self._replicas

    @property
    def in_sync_replicas(self):
        return self._in_sync_replicas
