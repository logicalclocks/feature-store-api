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


from hsfs import feature_view, training_dataset_feature
from hsfs.constructor import query


class TestFeatureView:
    def test_from_response_json(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.client.get_instance")
        json = backend_fixtures["get_feature_view"]["response"]

        # Act
        fv = feature_view.FeatureView.from_response_json(json)

        # Assert
        assert fv.name == 'test_name'
        assert fv.id == 11
        assert isinstance(fv.query, query.Query)
        assert fv.featurestore_id == 5
        assert fv.version == 1
        assert fv.description == 'test_description'
        assert fv.labels == ['intt']
        assert fv.transformation_functions == {}
        assert len(fv.schema) == 2
        assert isinstance(fv.schema[0], training_dataset_feature.TrainingDatasetFeature)

    def test_from_response_json_basic_info(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.client.get_instance")
        json = backend_fixtures["get_feature_view_basic_info"]["response"]

        # Act
        fv = feature_view.FeatureView.from_response_json(json)

        # Assert
        assert fv.name == 'test_name'
        assert fv.id == None
        assert isinstance(fv.query, query.Query)
        assert fv.featurestore_id == 5
        assert fv.version == None
        assert fv.description == None
        assert fv.labels == []
        assert fv.transformation_functions == {}
        assert len(fv.schema) == 0
