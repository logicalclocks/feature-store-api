#
#   Copyright 2024 Hopsworks AB
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
from hsfs.core import rondb_engine


class TestRondbEngine:
    def test_build_base_payload_response_json(self, backend_fixtures):
        # Arrange
        rondb_engine_instance = rondb_engine.RondbEngine()
        kwargs = {
            "feature_store_name": "test_store_featurestore",
            "feature_view_name": "test_feature_view",
            "feature_view_version": 2,
            "return_type": rondb_engine.RondbEngine.RETURN_TYPE_RESPONSE_JSON,
        }
        metadata_options_missing_name = {"featureType": False}
        metadata_options_missing_type = {"featureName": False}

        # Act
        kwargs["metadata_options"] = metadata_options_missing_name
        missing_name_payload = rondb_engine_instance._build_base_payload(**kwargs)
        kwargs["metadata_options"] = metadata_options_missing_type
        missing_type_payload = rondb_engine_instance._build_base_payload(**kwargs)

        # Assert
        for (key, value) in missing_name_payload.items():
            if key != "metadataOptions":
                assert (
                    backend_fixtures["rondb_server"]["get_single_vector_payload"][key]
                    == value
                )
            else:
                # featureName is missing, insert key with default value True
                assert "featureName" in value.keys() and value["featureName"] is True
                # does not enforce featureType to be true if return type is response json
                assert value["featureType"] is False

        for (key, value) in missing_type_payload.items():
            if key != "metadataOptions":
                assert (
                    backend_fixtures["rondb_server"]["get_single_vector_payload"][key]
                    == value
                )
            else:
                # featureType is missing, insert key with default value True
                assert "featureType" in value.keys() and value["featureType"] is True
                # does not enforce featureName to be true if return type is response json
                assert value["featureName"] is False

    def test_build_base_payload_feature_vector(self, backend_fixtures):
        # Arrange
        rondb_engine_instance = rondb_engine.RondbEngine()
        kwargs = {
            "feature_store_name": "test_store_featurestore",
            "feature_view_name": "test_feature_view",
            "feature_view_version": 2,
            "return_type": rondb_engine.RondbEngine.RETURN_TYPE_FEATURE_VECTOR,
        }
        metadata_options_missing_name = {"featureType": False}
        metadata_options_missing_type = {"featureName": False}

        # Act
        kwargs["metadata_options"] = metadata_options_missing_name
        missing_name_payload = rondb_engine_instance._build_base_payload(**kwargs)
        kwargs["metadata_options"] = metadata_options_missing_type
        missing_type_payload = rondb_engine_instance._build_base_payload(**kwargs)

        # Assert
        for (key, value) in missing_name_payload.items():
            if key != "metadataOptions":
                assert (
                    backend_fixtures["rondb_server"]["get_single_vector_payload"][key]
                    == value
                )
            else:
                # featureName is missing, insert key with default value True
                assert "featureName" in value.keys() and value["featureName"] is True
                # enforce featureType to be true if return type is feature vector
                assert value["featureType"] is True

        for (key, value) in missing_type_payload.items():
            if key != "metadataOptions":
                assert (
                    backend_fixtures["rondb_server"]["get_single_vector_payload"][key]
                    == value
                )
            else:
                # featureType is missing, insert key with default value True
                assert "featureType" in value.keys() and value["featureType"] is True
                # enforce featureName to be true if return type is feature vector
                assert value["featureName"] is True

    def test_convert_rdrs_response_to_feature_vector_dict(self, backend_fixtures):
        # Arrange
        rondb_engine_instance = rondb_engine.RondbEngine()
        response = backend_fixtures["rondb_server"][
            "get_single_vector_response_json_complete"
        ]
        reference_feature_vector = {
            "ticker": "APPL",
            "when": "2022-01-01 00:00:00",
            "price": 21.3,
            "volume": 10,
        }

        # Act
        feature_vector_dict = (
            rondb_engine_instance.convert_rdrs_response_to_dict_feature_vector(
                row_feature_values=response["features"], metadatas=response["metadata"]
            )
        )

        # Assert
        assert feature_vector_dict == reference_feature_vector
