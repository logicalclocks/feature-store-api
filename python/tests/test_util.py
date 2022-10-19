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

from hsfs import util
from datetime import datetime


class TestUtil:
    def test_get_hudi_datestr_from_timestamp(self):
        dt = util.get_hudi_datestr_from_timestamp(1640995200000)
        dt_test = datetime(2022, 1, 1, 0, 0, 0)

        assert dt == "20220101000000000"
