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

import re
import itertools

try:
    import tensorflow as tf
except ModuleNotFoundError:
    pass

try:
    from pydoop import hdfs
except ModuleNotFoundError:
    pass

try:
    import boto3
except ModuleNotFoundError:
    pass


class FeedModelEngine:
    def __init__(
        self,
        training_dataset,
        split,
        target_name,
        feature_names,
        is_training,
        cycle_length,
    ):
        """FeedModelEngine object that has utility methods for efficient tf.data.TFRecordDataset reading.

        :param training_dataset: training dataset name
        :type name: str, required
        :param split: name of training dataset split. train, test or eval
        :type split: str, required
        :param target_name: name of the target variable
        :type target_name: str, required
        :param feature_names: name of training variables
        :type feature_names: 1d array, required
        :param is_training: whether it is for training, testing or eval
        :type is_training: boolean, required
        :param cycle_length: number of files to be read and deserialized in parallel.
        :type cycle_length: int, required
        :type engine: str, required

        :return: feed model object
        :rtype: FeedModelEngine
        """

        self._training_dataset = training_dataset
        self._split = split
        self._target_name = target_name
        self._feature_names = feature_names
        self._is_training = is_training
        self._cycle_length = cycle_length

        self._training_dataset_schema = self._training_dataset.schema

        self._input_files = _get_training_dataset_files(
            self._training_dataset.location, self._split
        )

        # TODO (davit): get feat.types as well. This will be important for csv dataset
        if self._feature_names is None:
            self._feature_names = [feat.name for feat in self._training_dataset_schema]

            if target_name in self._feature_names:
                self._feature_names.remove(target_name)

    def get_serialized_example_schema(self):
        """
        returns schema for parsing serialized example of ff record file

        :return: schema for tfrecord file
        :rtype: dict
        """
        _, tfrecord_feature_description = _get_tfdataset(
            self._input_files, self._cycle_length
        )
        return tfrecord_feature_description

    def tf_record_dataset(
        self,
        batch_size=None,
        num_epochs=None,
        one_hot_encode_labels=False,
        num_classes=None,
        optimize=True,
        serialized_ndarray_fname=[],
    ):
        """
        Reads and returns tf.data.TFRecordDataset object ready to feed tf keras models

        :param batch_size: size of batch, defaults to None
        :type batch_size: int, optional
        :param num_epochs: number of epochs to train, defaults to None
        :type num_epochs: int, optional
        :param one_hot_encode_labels: if set true then one hot encode lables, defaults to False
        :type one_hot_encode_labels: boolean, optional
        :param num_classes: if above true then provide number of target classes, defaults to None
        :type num_classes: int, optional
        :param optimize: if set true  api will optimise tf data read operation, and return feature vector for model
        with single input, defaults to True
        :type  optimize: int, optional
        :param serialized_ndarray_fname: names of features that contain serialised multi dimentional arrays,
        defaults to []
        :type  serialized_ndarray_fname: 1d array, optional
        :return: tf dataset
        :rtype: tf.data.TFRecordDataset
        """

        if optimize and batch_size is None and num_epochs is None:
            raise ValueError(
                "if optimize is set to True you also need to provide batch_size and num_epochs"
            )

        if one_hot_encode_labels and (num_classes is None or num_classes <= 1):
            raise ValueError(
                "if one_hot_encode_labels is set to True you also need to provide num_classes > 1"
            )

        dataset, tfrecord_feature_description = _get_tfdataset(
            self._input_files, self._cycle_length
        )

        def _de_serialize(serialized_example):
            example = tf.io.parse_single_example(
                serialized_example, tfrecord_feature_description
            )
            return example

        def _process_example(example):
            x = []
            for feature_name in self._feature_names:
                if feature_name in serialized_ndarray_fname:
                    x.append(
                        tf.io.parse_tensor(example[feature_name], out_type=tf.float32)
                    )
                else:
                    if example[feature_name].dtype == tf.string:
                        raise ValueError(
                            "tf.string feature is not allowed here. please provide optimize=False and preprocess "
                            "dataset accordingly"
                        )
                    elif example[feature_name].dtype in (tf.float64, tf.float16,  tf.int64, tf.int32):
                        example[feature_name] = tf.cast(example[feature_name], tf.float32)
                    x.append(example[feature_name])

            if len(x) == 1:
                x = x[0]
            else:
                x = tf.stack(x)

            y = example[self._target_name]
            if one_hot_encode_labels:
                y = tf.one_hot(y, num_classes)
            else:
                y = [tf.cast(y, tf.float32)]
            return x, y

        dataset = dataset.map(
            lambda value: _de_serialize(value),
            num_parallel_calls=tf.data.experimental.AUTOTUNE
        )

        if optimize:
            dataset = dataset.map(
                lambda value: _process_example(value),
                num_parallel_calls=tf.data.experimental.AUTOTUNE
            )
            dataset = _optimize_dataset(
                dataset, batch_size, num_epochs, self._is_training
            )

        return dataset

    def tf_csv_dataset(
            self,
            batch_size=None,
            num_epochs=None,
            one_hot_encode_labels=False,
            num_classes=None,
            optimize=True
    ):
        """
        Reads and returns tf.data.TFRecordDataset object ready to feed tf keras models

        :param batch_size: size of batch, defaults to None
        :type batch_size: int, optional
        :param num_epochs: number of epochs to train, defaults to None
        :type num_epochs: int, optional
        :param one_hot_encode_labels: if set true then one hot encode lables, defaults to False
        :type one_hot_encode_labels: boolean, optional
        :param num_classes: if above true then provide number of target classes, defaults to None
        :type num_classes: int, optional
        :param optimize: if set true  api will optimise tf data read operation, and return feature vector for model
        with single input, defaults to True
        :type  optimize: int, optional
        :return: tf dataset
        :rtype: tf.data.TFRecordDataset
        """

        if optimize and batch_size is None and num_epochs is None:
            raise ValueError(
                "if optimize is set to True you also need to provide batch_size and num_epochs"
            )

        if one_hot_encode_labels and (num_classes is None or num_classes <= 1):
            raise ValueError(
                "if one_hot_encode_labels is set to True you also need to provide num_classes > 1"
            )

        self._input_files
        csv_dataset = tf.data.experimental.CsvDataset(
            self._input_files,
            header=False,
            record_defaults=[tf.float32,
                               tf.float32,
                               tf.float32,
                               tf.float32,
                               tf.float32
                               ]
        )

        def _process_csv_dataset(csv_record):
            csv_record_list = list(csv_record)
            y = csv_record_list.pop(self._feature_names.index(self._target_name))
            y = tf.convert_to_tensor(y)
            x = tf.convert_to_tensor(tuple(csv_record_list))
            return x,y

        if optimize:
            csv_dataset = csv_dataset.map(lambda *value: _process_csv_dataset(value))
            csv_dataset = _optimize_dataset(
                csv_dataset, batch_size, num_epochs, self._is_training
            )
        return csv_dataset

def _optimize_dataset(dataset, batch_size, num_epochs, is_training):
    if is_training:
        dataset = dataset.shuffle(num_epochs * batch_size)
        dataset = dataset.repeat(num_epochs * batch_size)
    dataset = dataset.cache()
    dataset = dataset.batch(batch_size, drop_remainder=True)
    dataset = dataset.prefetch(tf.data.experimental.AUTOTUNE)

    return dataset


def _return_example_tf1(train_filenames):
    sample = 1
    record_iterator = tf.compat.v1.io.tf_record_iterator(path=train_filenames[0])
    for string_record in itertools.islice(record_iterator, sample):
        example = tf.train.Example()
        example.ParseFromString(string_record)
    return example


def _return_example_tf2(dataset):
    for raw_record in dataset.take(1):
        example = tf.train.Example()
        example.ParseFromString(raw_record.numpy())
    return example


def _create_tfrecord_feature_description(dataset, train_filenames):
    if tf.__version__ >= "2.0":
        example = _return_example_tf2(dataset)
    else:
        example = _return_example_tf1(train_filenames)

    feature_description = {}
    for k, v in sorted(example.features.feature.items()):
        f_name, f_description = _infer_tf_dtype(k, v)
        feature_description[f_name] = f_description
    return feature_description


def _infer_tf_dtype(k, v):
    if v.int64_list.value:
        result = v.int64_list.value
        feature_length = len(result)
        if feature_length > 1:
            feature_type = tf.io.FixedLenFeature([feature_length], tf.int64)
        else:
            feature_type = tf.io.FixedLenFeature([], tf.int64)

    elif v.float_list.value:
        result = v.float_list.value
        feature_length = len(result)
        if feature_length > 1:
            feature_type = tf.io.FixedLenFeature([feature_length], tf.float32)
        else:
            feature_type = tf.io.FixedLenFeature([], tf.float32)

    elif v.bytes_list.value:
        feature_type = tf.io.FixedLenFeature([], tf.string)

    return k, feature_type


def _get_tfdataset(input_files, cycle_length):
    dataset = tf.data.Dataset.from_tensor_slices(input_files)

    dataset = dataset.interleave(
        tf.data.TFRecordDataset,
        cycle_length=cycle_length,
        num_parallel_calls=tf.data.experimental.AUTOTUNE,
    )

    tfrecord_feature_description = _create_tfrecord_feature_description(
        dataset, input_files
    )

    return dataset, tfrecord_feature_description


def _get_training_dataset_files(training_dataset_location, split):
    """
    returns list of absolute path of training input files
    :param training_dataset_location: training_dataset_location
    :type training_dataset_location: str
    :param split: name of training dataset split. train, test or eval
    :type split: str
    :return: absolute path of input files
    :rtype: 1d array
    """

    if training_dataset_location.startswith("hopsfs"):
        input_files = _get_hopsfs_dataset_files(training_dataset_location, split)
    elif training_dataset_location.startswith("s3"):
        input_files = _get_s3_dataset_files(training_dataset_location, split)
    else:
        raise Exception("Couldn't find execution engine.")

    return input_files


def _get_hopsfs_dataset_files(training_dataset_location, split):
    path = training_dataset_location.replace("hopsfs", "hdfs")
    if split is None:
        path = hdfs.path.abspath(path)
    else:
        path = hdfs.path.abspath(path + "/" + str(split))

    input_files = []

    all_list = hdfs.ls(path, recursive=True)

    # Remove directories and spark '_SUCCESS' file if any
    for file in all_list:
        if not hdfs.path.isdir(file) and not file.endswith("_SUCCESS"):
            input_files.append(file)

    return input_files


def _get_s3_dataset_files(training_dataset_location, split):
    """
    returns list of absolute path of training input files
    :param training_dataset_location: training_dataset_location
    :type training_dataset_location: str
    :param split: name of training dataset split. train, test or eval
    :type split: str
    :return: absolute path of input files
    :rtype: 1d array
    """

    if split is None:
        path = training_dataset_location
    else:
        path = training_dataset_location + "/" + str(split)

    match = re.match(r"s3:\/\/(.+?)\/(.+)", path)
    bucketname = match.group(1)

    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucketname)

    input_files = []
    for s3_obj_summary in bucket.objects.all():
        if s3_obj_summary.get()["ContentType"] != "application/x-directory":
            input_files.append(path + "/" + s3_obj_summary.key)

    return input_files
