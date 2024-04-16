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

from hsfs.core import validation_report_engine


class TestValidationReportEngine:
    def test_save(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_group_id = 10
        validation_report_url = "test_url"

        mock_vr_api = mocker.patch(
            "hsfs.core.validation_report_api.ValidationReportApi"
        )
        mock_vr_engine_get_validation_report_url = mocker.patch(
            "hsfs.core.validation_report_engine.ValidationReportEngine._get_validation_report_url"
        )
        mock_print = mocker.patch("builtins.print")

        vr_engine = validation_report_engine.ValidationReportEngine(
            feature_store_id=feature_store_id, feature_group_id=feature_group_id
        )

        mock_vr_engine_get_validation_report_url.return_value = validation_report_url

        # Act
        vr_engine.save(validation_report=None)

        # Assert
        assert mock_vr_api.return_value.create.call_count == 1
        assert mock_print.call_count == 1
        assert mock_print.call_args[0][
            0
        ] == "Validation Report saved successfully, explore a summary at {}".format(
            validation_report_url
        )

    def test_get_last(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_group_id = 10
        validation_report_url = "test_url"

        mock_vr_api = mocker.patch(
            "hsfs.core.validation_report_api.ValidationReportApi"
        )
        mock_vr_engine_get_validation_report_url = mocker.patch(
            "hsfs.core.validation_report_engine.ValidationReportEngine._get_validation_report_url"
        )
        mock_print = mocker.patch("builtins.print")

        vr_engine = validation_report_engine.ValidationReportEngine(
            feature_store_id=feature_store_id, feature_group_id=feature_group_id
        )

        mock_vr_engine_get_validation_report_url.return_value = validation_report_url

        # Act
        vr_engine.get_last()

        # Assert
        assert mock_vr_api.return_value.get_last.call_count == 1
        assert mock_print.call_count == 1
        assert (
            mock_print.call_args[0][0]
            == "Long reports can be truncated when fetching from Hopsworks.\n"
            "        \nYou can download the full report at {}".format(
                validation_report_url
            )
        )

    def test_get_all(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_group_id = 10
        validation_report_url = "test_url"

        mock_vr_api = mocker.patch(
            "hsfs.core.validation_report_api.ValidationReportApi"
        )
        mock_vr_engine_get_validation_report_url = mocker.patch(
            "hsfs.core.validation_report_engine.ValidationReportEngine._get_validation_report_url"
        )
        mock_print = mocker.patch("builtins.print")

        vr_engine = validation_report_engine.ValidationReportEngine(
            feature_store_id=feature_store_id, feature_group_id=feature_group_id
        )

        mock_vr_engine_get_validation_report_url.return_value = validation_report_url

        # Act
        vr_engine.get_all()

        # Assert
        assert mock_vr_api.return_value.get_all.call_count == 1
        assert mock_print.call_count == 1
        assert (
            mock_print.call_args[0][0]
            == "Long reports can be truncated when fetching from Hopsworks.\n"
            "        \nYou can download full reports at {}".format(
                validation_report_url
            )
        )

    def test_delete(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_group_id = 10
        validation_report_id = 43

        mock_vr_api = mocker.patch(
            "hsfs.core.validation_report_api.ValidationReportApi"
        )

        vr_engine = validation_report_engine.ValidationReportEngine(
            feature_store_id=feature_store_id, feature_group_id=feature_group_id
        )

        # Act
        vr_engine.delete(validation_report_id=validation_report_id)

        # Assert
        assert mock_vr_api.return_value.delete.call_count == 1

    def test_get_validation_report_url(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_group_id = 10

        mock_client_get_instance = mocker.patch("hsfs.client.get_instance")
        mock_util_get_hostname_replaced_url = mocker.patch(
            "hsfs.util.get_hostname_replaced_url"
        )

        vr_engine = validation_report_engine.ValidationReportEngine(
            feature_store_id=feature_store_id, feature_group_id=feature_group_id
        )

        mock_client_get_instance.return_value._project_id = 50

        # Act
        vr_engine._get_validation_report_url()

        # Assert
        assert mock_util_get_hostname_replaced_url.call_count == 1
        assert (
            mock_util_get_hostname_replaced_url.call_args[0][0] == "/p/50/fs/99/fg/10"
        )
