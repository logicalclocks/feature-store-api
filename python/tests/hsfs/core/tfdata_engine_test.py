#
#   Copyright 2020 Logical Clocks AB
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

try:
    import tensorflow as tf
except ImportError:
    pass

from hsfs.core.tfdata_engine import TFDataEngine


class TFDataEngineTest(tf.test.TestCase):
    def setUp(self):
        super(TFDataEngineTest, self).setUp()

    @staticmethod
    def _bytes_feature(value):
        """Returns a bytes_list from a string / byte."""
        if isinstance(value, type(tf.constant(0))):
            value = (
                value.numpy()
            )  # BytesList won't unpack a string from an EagerTensor.
        return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))

    @staticmethod
    def _float_feature(value):
        """Returns a float_list from a float / double."""
        return tf.train.Feature(float_list=tf.train.FloatList(value=[value]))

    @staticmethod
    def _int64_feature(value):
        """Returns an int64_list from a bool / enum / int / uint."""
        return tf.train.Feature(int64_list=tf.train.Int64List(value=[value]))

    @staticmethod
    def _serialize_example(example):
        return tf.train.Example.FromString(example)

    def _example_with_string(self, feature0, feature1, feature2, feature3):
        feature = {
            "feature0": self._int64_feature(feature0),
            "feature1": self._int64_feature(feature1),
            "feature2": self._bytes_feature(feature2),
            "feature3": self._float_feature(feature3),
        }

        return tf.train.Example(features=tf.train.Features(feature=feature))

    def _example_no_string(self, feature0, feature1):
        feature = {
            "feature0": self._int64_feature(feature0),
            "feature1": self._float_feature(feature1),
        }

        return tf.train.Example(features=tf.train.Features(feature=feature))

    def test_infer_tf_dtype(self):
        example = self._example_with_string(True, 5, b"hsfs", 1.2836)

        feature_description = {}
        for k, v in sorted(example.features.feature.items()):
            f_name, f_description = TFDataEngine._infer_tf_dtype(k, v, [])
            feature_description[f_name] = f_description

        expected = {
            "feature0": tf.io.FixedLenFeature(
                shape=[], dtype=tf.int64, default_value=None
            ),
            "feature1": tf.io.FixedLenFeature(
                shape=[], dtype=tf.int64, default_value=None
            ),
            "feature2": tf.io.FixedLenFeature(
                shape=[], dtype=tf.string, default_value=None
            ),
            "feature3": tf.io.FixedLenFeature(
                shape=[], dtype=tf.float32, default_value=None
            ),
        }
        self.assertEqual(feature_description, expected)

    def test_convert2tfdtype(self):
        feture_types = ["float", "int", "int", "float", "float"]
        expected = [tf.float32, tf.int32, tf.int32, tf.float32, tf.float32]
        record_defaults = [
            TFDataEngine._convert_to_tf_dtype(feat_type) for feat_type in feture_types
        ]
        self.assertEqual(record_defaults, expected)
        self.assertRaises(ValueError, TFDataEngine._convert_to_tf_dtype, "list<string>")
