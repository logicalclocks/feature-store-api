#
#   Copyright 2021 Logical Clocks AB
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

from typing import List
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics


class BuiltInTransformationFunction:
    def __init__(self, method):
        self._method = method.lower()

    @staticmethod
    def min_max_scaler_stats(
        feature_descriptive_stats: List[FeatureDescriptiveStatistics], feature_name: str
    ):
        min_value = None
        max_value = None
        for stats in feature_descriptive_stats:
            if stats.feature_name == feature_name:
                if stats.feature_type not in ["Integral", "Fractional", "Decimal"]:
                    raise ValueError("Can't compute min_max_scaler for this type")
                min_value = stats.min
                max_value = stats.max

        if min_value is None or max_value is None:
            raise FeatureStoreException(
                "Feature {feature_name:} doesn't have minimum and/or maximum values computed. Thus can't use "
                "min_max_scaler method".format(feature_name=feature_name)
            )
        return min_value, max_value

    @staticmethod
    def standard_scaler_stats(
        feature_descriptive_stats: List[FeatureDescriptiveStatistics], feature_name: str
    ):
        mean = None
        std_dev = None
        for stats in feature_descriptive_stats:
            if stats.feature_name == feature_name:
                if stats.feature_type not in ["Integral", "Fractional", "Decimal"]:
                    raise ValueError("Can't compute standard_scaler for this type")
                mean = stats.mean
                std_dev = stats.stddev

        if mean is None or std_dev is None:
            raise FeatureStoreException(
                "Feature {feature_name:} doesn't have mean and/or standard deviation computed. Thus can't use "
                "standard_scaler method".format(feature_name=feature_name)
            )
        return mean, std_dev

    @staticmethod
    def robust_scaler_stats(
        feature_descriptive_stats: List[FeatureDescriptiveStatistics], feature_name: str
    ):
        percentiles = None
        for stats in feature_descriptive_stats:
            if stats.feature_name == feature_name:
                if stats.feature_type not in ["Integral", "Fractional", "Decimal"]:
                    raise ValueError("Can't compute robust_scaler for this type")
                if stats.percentiles is not None and len(stats.percentiles) > 0:
                    percentiles = stats.percentiles

        if percentiles is None:
            raise FeatureStoreException(
                "Feature {feature_name:} doesn't have mean and/or standard deviation computed. Thus can't use "
                "standard_scaler method".format(feature_name=feature_name)
            )
        return percentiles

    @staticmethod
    def encoder_stats(
        feature_descriptive_stats: List[FeatureDescriptiveStatistics], feature_name: str
    ):
        for stats in feature_descriptive_stats:
            if (
                stats.feature_name == feature_name
                and stats.extended_statistics is not None
                and "unique_values" in stats.extended_statistics
            ):
                unique_data = [
                    value for value in stats.extended_statistics["unique_values"]
                ]
                value_to_index = dict(
                    (value, index) for index, value in enumerate(unique_data)
                )
                return value_to_index
