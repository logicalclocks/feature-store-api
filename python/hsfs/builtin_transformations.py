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

import pandas as pd
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.hopsworks_udf import hopsworks_udf


@hopsworks_udf(float)
def min_max_scaler(feature: pd.Series, statistics_feature) -> pd.Series:
    return (feature - statistics_feature.min) / (
        statistics_feature.max - statistics_feature.min
    )


@hopsworks_udf(float)
def standard_scaler(
    feature: pd.Series, statistics_feature: FeatureDescriptiveStatistics
) -> pd.Series:
    return (feature - statistics_feature.mean) / statistics_feature.stddev


@hopsworks_udf(float)
def robust_scaler(
    feature: pd.Series, statistics_feature: FeatureDescriptiveStatistics
) -> pd.Series:
    return (feature - statistics_feature.percentiles[49]) / (
        statistics_feature.percentiles[74] - statistics_feature.percentiles[24]
    )


# @hopsworks_udf(int)
def label_encoder(
    feature: pd.Series, statistics_feature: FeatureDescriptiveStatistics
) -> pd.Series:
    unique_data = [
        value for value in statistics_feature.extended_statistics["unique_values"]
    ]
    value_to_index = {value: index for index, value in enumerate(unique_data)}
    return pd.Series([value_to_index[data] for data in feature])


def one_hot_encoder(
    feature: pd.Series, statistics_feature: FeatureDescriptiveStatistics
) -> pd.Series:
    unique_data = [
        value for value in statistics_feature.extended_statistics["unique_values"]
    ]
    print(statistics_feature.extended_statistics["unique_values"])
    one_hot = pd.get_dummies(feature, dtype="bool")
    for data in unique_data:
        if data not in one_hot:
            one_hot[data] = False
    return one_hot
