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

import warnings

from hsfs import feature_group as fg_mod
from hsfs.core import feature_group_api
from mock import Mock


class TestFeatureGroupApi:
    def test_get_smart_with_infer_type(self, mocker, backend_fixtures):
        # Arrange
        feature_store_id = 99
        fg_api = feature_group_api.FeatureGroupApi()
        side_effects = [
            [backend_fixtures["feature_group"]["get"]["response"]],
            [backend_fixtures["external_feature_group"]["get"]["response"]],
            [backend_fixtures["spine_group"]["get"]["response"]],
        ]
        client_mock = Mock()
        client_mock.configure_mock(**{"_send_request.side_effect": side_effects})
        mocker.patch(
            "hsfs.client.get_instance",
            return_value=client_mock,
        )
        mocker.patch("hsfs.engine.get_instance")

        print(client_mock.side_effect)

        # Act
        stream_fg = fg_api.get(feature_store_id, "stream_fg", version=1)
        external_fg = fg_api.get(feature_store_id, "external_fg", version=1)
        spine_fg = fg_api.get(feature_store_id, "spine_fg", version=1)

        # Assert
        assert isinstance(stream_fg, fg_mod.FeatureGroup)
        assert isinstance(external_fg, fg_mod.ExternalFeatureGroup)
        assert isinstance(spine_fg, fg_mod.SpineGroup)

    def test_check_features(self, mocker, backend_fixtures):
        # Arrange
        fg_api = feature_group_api.FeatureGroupApi()
        json = backend_fixtures["feature_group"]["get_basic_info"][
            "response"
        ]
        fg = fg_mod.FeatureGroup.from_response_json(json)

        # Act
        with warnings.catch_warnings(record=True) as warning_record:
            fg_api._check_features(fg)

        # Assert
        assert len(warning_record) == 0

    def test_check_features_no_features(self, mocker, backend_fixtures):
        # Arrange
        fg_api = feature_group_api.FeatureGroupApi()
        json = backend_fixtures["feature_group"]["get_basic_info_no_features"][
            "response"
        ]
        fg = fg_mod.FeatureGroup.from_response_json(json)

        # Act
        with warnings.catch_warnings(record=True) as warning_record:
            fg_api._check_features(fg)

        # Assert
        assert len(warning_record) == 1
