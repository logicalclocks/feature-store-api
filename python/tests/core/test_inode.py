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


from hsfs.core import inode


class TestInode:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["inode"]["get"]["response"]

        # Act
        node_list = inode.Inode.from_response_json(json)

        # Assert
        assert len(node_list) == 1
        node = node_list[0]
        assert node.path == "test_path"

    def test_from_response_json_empty(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["inode"]["get_empty"]["response"]

        # Act
        node_list = inode.Inode.from_response_json(json)

        # Assert
        assert len(node_list) == 0
