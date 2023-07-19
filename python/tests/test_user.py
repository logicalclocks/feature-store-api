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


from hsfs import user


class TestUser:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["user"]["get"]["response"]

        # Act
        u = user.User.from_response_json(json)

        # Assert
        assert u.username == "test_username"
        assert u.email == "test_email"
        assert u.first_name == "test_first_name"
        assert u.last_name == "test_last_name"
        assert u.status == "test_status"
        assert u.secret == "test_secret"
        assert u.chosen_password == "test_chosen_password"
        assert u.repeated_password == "test_repeated_password"
        assert u.tos == "test_tos"
        assert u.two_factor == "test_two_factor"
        assert u.tours_state == "test_tours_state"
        assert u.max_num_projects == "test_max_num_projects"
        assert u.test_user == "test_test_user"
        assert u.user_account_type == "test_user_account_type"
        assert u.num_active_projects == "test_num_active_projects"
        assert u.num_remaining_projects == "test_num_remaining_projects"

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["user"]["get_basic_info"]["response"]

        # Act
        u = user.User.from_response_json(json)

        # Assert
        assert u is None
