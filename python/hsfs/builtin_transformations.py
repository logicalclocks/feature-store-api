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

import numpy as np
import pandas as pd
from hsfs.hopsworks_udf import udf
from hsfs.transformation_statistics import TransformationStatistics


feature_statistics = TransformationStatistics("feature")


@udf(float, drop=["feature"])
def min_max_scaler(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    return (feature - statistics.feature.min) / (
        statistics.feature.max - statistics.feature.min
    )


@udf(float, drop=["feature"])
def standard_scaler(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    return (feature - statistics.feature.mean) / statistics.feature.stddev


@udf(float, drop=["feature"])
def robust_scaler(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    return (feature - statistics.feature.percentiles[49]) / (
        statistics.feature.percentiles[74] - statistics.feature.percentiles[24]
    )


@udf(int, drop=["feature"])
def label_encoder(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    unique_data = sorted(
        [value for value in statistics.feature.extended_statistics["unique_values"]]
    )
    value_to_index = {value: index for index, value in enumerate(unique_data)}
    return pd.Series(
        [value_to_index[data] if not pd.isna(data) else np.nan for data in feature]
    )


@udf(bool, drop=["feature"])
def one_hot_encoder(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    unique_data = [
        value for value in statistics.feature.extended_statistics["unique_values"]
    ]
    one_hot = pd.get_dummies(feature, dtype="bool")
    for data in unique_data:
        if data not in one_hot:
            one_hot[data] = False
    # Sorting by columns so as to maintain consistency in column order.
    return one_hot.reindex(sorted(one_hot.columns), axis=1)
