import pytest
import pandas as pd
from datetime import datetime


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
