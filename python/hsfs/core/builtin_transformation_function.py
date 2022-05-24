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

from hsfs.client.exceptions import FeatureStoreException


class BuiltInTransformationFunction:
    def __init__(self, method):
        self._method = method.lower()

    @staticmethod
    def min_max_scaler_stats(content, feature_name):
        min_value = None
        max_value = None
        for col in content["columns"]:
            if col["column"] == feature_name:
                if col["dataType"] not in ["Integral", "Fractional", "Decimal"]:
                    raise ValueError("Can't compute min_max_scaler for this type")
                if "minimum" in col:
                    min_value = col["minimum"]
                if "maximum" in col:
                    max_value = col["maximum"]

        if min_value is None or max_value is None:
            raise FeatureStoreException(
                "Feature {feature_name:} doesn't have minimum and/or maximum values computed. Thus can't use "
                "min_max_scaler method".format(feature_name=feature_name)
            )
        return min_value, max_value

    @staticmethod
    def standard_scaler_stats(content, feature_name):
        mean = None
        std_dev = None
        for col in content["columns"]:
            if col["column"] == feature_name:
                if col["dataType"] not in ["Integral", "Fractional", "Decimal"]:
                    raise ValueError("Can't compute standard_scaler for this type")
                if "mean" in col:
                    mean = col["mean"]
                if "stdDev" in col:
                    std_dev = col["stdDev"]

        if mean is None or std_dev is None:
            raise FeatureStoreException(
                "Feature {feature_name:} doesn't have mean and/or standard deviation computed. Thus can't use "
                "standard_scaler method".format(feature_name=feature_name)
            )
        return mean, std_dev

    @staticmethod
    def robust_scaler_stats(content, feature_name):
        percentiles = None
        for col in content["columns"]:
            if col["column"] == feature_name:
                if col["dataType"] not in ["Integral", "Fractional", "Decimal"]:
                    raise ValueError("Can't compute robust_scaler for this type")
                if "approxPercentiles" in col and len(col["approxPercentiles"]) > 0:
                    percentiles = col["approxPercentiles"]

        if percentiles is None:
            raise FeatureStoreException(
                "Feature {feature_name:} doesn't have mean and/or standard deviation computed. Thus can't use "
                "standard_scaler method".format(feature_name=feature_name)
            )
        return percentiles

    @staticmethod
    def encoder_stats(content, feature_name):
        for col in content["columns"]:
            if col["column"] == feature_name and "unique_values" in col:
                unique_data = [value for value in col["unique_values"]]
                value_to_index = dict(
                    (value, index) for index, value in enumerate(unique_data)
                )
                return value_to_index
