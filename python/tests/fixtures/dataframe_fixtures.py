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

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest


@pytest.fixture
def dataframe_fixture_basic():
    data = {
        "primary_key": [1, 2, 3, 4],
        "event_date": [
            datetime(2022, 7, 3).date(),
            datetime(2022, 1, 5).date(),
            datetime(2022, 1, 6).date(),
            datetime(2022, 1, 7).date(),
        ],
        "state": ["nevada", None, "nevada", None],
        "measurement": [12.4, 32.5, 342.6, 43.7],
    }

    return pd.DataFrame(data)


@pytest.fixture
def dataframe_fixture_times():
    data = {
        "primary_key": [1],
        "event_date": [datetime(2022, 7, 3).date()],
        "event_datetime_notz": [datetime(2022, 7, 3, 0, 0, 0)],
        "event_datetime_utc": [
            datetime(2022, 7, 3, 0, 0, 0).replace(tzinfo=timezone.utc)
        ],
        "event_datetime_utc_3": [
            datetime(2022, 7, 3, 0, 0, 0).replace(tzinfo=timezone(timedelta(hours=3)))
        ],
        "event_timestamp": [pd.Timestamp("2022-07-03T00")],
        "event_timestamp_pacific": [pd.Timestamp("2022-07-03T00", tz="US/Pacific")],
        "state": ["nevada"],
        "measurement": [12.4],
    }

    return pd.DataFrame(data)


@pytest.fixture
def dataframe_fixtures_column_spaced():
    data = {
        "Primary Key": [1, 2, 3, 4],
        "Event date": [
            datetime(2022, 7, 3).date(),
            datetime(2022, 1, 5).date(),
            datetime(2022, 1, 6).date(),
            datetime(2022, 1, 7).date(),
        ],
        "staTe 1": ["nevada", None, "nevada", None],
        "Measure ment taken": [12.4, 32.5, 342.6, 43.7],
    }

    return pd.DataFrame(data)
