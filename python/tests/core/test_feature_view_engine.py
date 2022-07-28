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

from hsfs import feature_view, transformation_function
from hsfs.core import feature_view_engine


class TestFeatureViewEngine:
    def test_save(self, mocker):
        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mocker.patch(
            "hsfs.transformation_function.TransformationFunction._extract_source_code"
        )
        mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_feature_view_url",
            return_value="url",
        )
        target = feature_view_engine.FeatureViewEngine(1)

        def plus_one(a):
            return a + 1

        tf = transformation_function.TransformationFunction(
            1, plus_one, 1, "plus_one", output_type=str
        )
        fv = feature_view.FeatureView(
            name="fv_name",
            query="fv_query",
            featurestore_id=1,
            labels=["label1", "label2"],
            transformation_functions={"ft1": tf},
        )

        target.save(fv)

        assert len(fv._features) == 3
        assert fv._features[0].name == "label1" and fv._features[0].label
        assert fv._features[1].name == "label2" and fv._features[1].label
        assert fv._features[2].name == "ft1" and fv._features[2].type == "StringType()"
