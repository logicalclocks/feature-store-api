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


class TFDataEngine:
    SUPPORTED_TFDTYPES = [
        tf.float16,
        tf.float32,
        tf.float64,
        tf.int16,
        tf.int64,
        tf.int32,
    ]
    SPARK_TO_TFDTYPES_MAPPINGS = {
        "string": tf.string,
        "short": tf.int16,
        "int": tf.int32,
        "long": tf.int64,
        "float": tf.float32,
        "double": tf.float64,
    }

    def __init__(
        self,
        training_dataset,
        split,
        target_name,
        feature_names,
        is_training,
        cycle_length,
    ):
        """TFDataEngine object that has utility methods for tf.data

        # Arguments
            training_dataset: str. Name of the training dataset.
            training dataset name
        split: split: str, required
            name of training dataset split. train, test or eval
        target_name: str, required
            name of the target variable
        feature_names: 1d array, required
            name of training variables
        is_training: boolean, required
            whether it is for training, testing or eval
        cycle_length: int, required
            number of files to be read and deserialized in parallel.

        # Returns
            TFDataEngine object
        """

        self._training_dataset = training_dataset
        self._split = split
        self._target_name = target_name
        self._feature_names = feature_names
        self._is_training = is_training
        self._cycle_length = cycle_length

        self._features = self._training_dataset._features
        self._training_dataset_format = self._training_dataset.data_format

        self._input_files = _get_training_dataset_files(
            self._training_dataset.location,
            self._split,
            filter_empty=True if self._training_dataset_format == "csv" else False,
        )

        if self._feature_names is None:
            self._feature_names = [feat.name for feat in self._features]

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
        process=False,
        serialized_ndarray_fname=[],
    ):
        """
        Reads tfrecord files and returns ParallelMapDataset or PrefetchDataset object ready to feed tf keras models

        Example:
            import hsfs
            connection = hsfs.connection()
            fs = connection.get_feature_store();
            td = fs.get_training_dataset("sample_model", 3)
            td.tf_data(target_name = "id").tf_record_dataset(batch_size=1, num_epochs=1, process=True)

        # Arguments
            batch_size: int, optional
                size of batch, defaults to `None`.
            num_epochs: int, optional
                number of epochs to train, defaults to `None`.
            one_hot_encode_labels: boolean, optional
                if set true then one hot encode labels, defaults to `False`.
            num_classes: int, optional
                if above true then provide number of target classes, defaults to  `None`
            process: boolean, optional
                if set true  api will optimise tf data read operation, and return feature vector for model
                with single input, defaults to `False`.
            serialized_ndarray_fname: string array, optional
                names of features that contain serialised multi dimentional arrays, defaults to `[]`

           # Returns
                PrefetchDataset if process is set to True
                ParallelMapDataset if process is set to False
        """

        if self._training_dataset_format.lower() not in ["tfrecords", "tfrecord"]:
            raise Exception(
                "tf_record_dataset function works only for training datasets that have tfrecord or tfrecords format"
            )

        if process and (batch_size is None or num_epochs is None):
            raise ValueError(
                "if process is set to True you also need to provide batch_size and num_epochs"
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
            # get target variable 1st
            y = example[self._target_name]
            if one_hot_encode_labels:
                y = tf.one_hot(y, num_classes)
            else:
                y = tf.cast(y, tf.float32)

            # if there is only 1 feature we return it
            if len(self._feature_names) == 1:
                _feature_name = self._feature_names[0]
                # here it is assumed that if user provides serialized_ndarray, it is only one feature
                if self._feature_names in serialized_ndarray_fname:
                    x = tf.io.parse_tensor(example[_feature_name], out_type=tf.float32)
                else:
                    x = example[_feature_name]
                return x, y
            # Otherwise we need to have features in the same type, thus tf.float32
            else:
                x = []
                for _feature_name in self._feature_names:
                    x.append(_convert2float32(example[_feature_name]))
                x = tf.stack(x)
            return x, y

        dataset = dataset.map(
            lambda value: _de_serialize(value),
            num_parallel_calls=tf.data.experimental.AUTOTUNE,
        )

        if process:
            dataset = dataset.map(
                lambda value: _process_example(value),
                num_parallel_calls=tf.data.experimental.AUTOTUNE,
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
        process=False,
    ):
        """
        Reads csv files and returns CsvDatasetV2 or PrefetchDataset object ready to feed tf keras models

        Example:
            import hsfs
            connection = hsfs.connection()
            fs = connection.get_feature_store();
            td = fs.get_training_dataset("sample_model", 3)
            td.tf_data(target_name = "id").tf_csv_dataset(batch_size=1, num_epochs=1, process=True)

        # Arguments
            batch_size: int, optional
                size of batch, defaults to `None`.
            num_epochs: int, optional
                number of epochs to train, defaults to `None`.
            one_hot_encode_labels: boolean, optional
                if set true then one hot encode labels, defaults to `False`.
            num_classes: int, optional
                if above true then provide number of target classes, defaults to  `None`
            process: boolean, optional
                if set true  api will optimise tf data read operation, and return feature vector for model
                with single input, defaults to `False`.
            serialized_ndarray_fname: string array, optional
                names of features that contain serialised multi dimentional arrays, defaults to `[]`

           # Returns
                PrefetchDataset if process is set to True
                ParallelMapDataset if process is set to False
            """

        if self._training_dataset_format != "csv":
            raise Exception(
                "tf_csv_dataset function works only for training datasets that have csv format"
            )

        if process and (batch_size is None or num_epochs is None):
            raise ValueError(
                "if process is set to True you also need to provide batch_size and num_epochs"
            )

        if one_hot_encode_labels and (num_classes is None or num_classes <= 1):
            raise ValueError(
                "if one_hot_encode_labels is set to True you also need to provide num_classes > 1"
            )

        csv_header = dict((feat.name, feat.index) for feat in self._features)
        record_defaults = [_convert2tfdtype(feat.type) for feat in self._features]

        csv_dataset = tf.data.experimental.CsvDataset(
            self._input_files, header=True, record_defaults=record_defaults,
        )

        def _process_csv_dataset(csv_record):
            csv_record_list = list(csv_record)
            # get target variable 1st
            y = csv_record_list.pop(csv_header[self._target_name])
            y = tf.convert_to_tensor(y)
            if one_hot_encode_labels:
                y = tf.one_hot(y, num_classes)
            else:
                y = tf.cast(y, tf.float32)

            # now get the feature vector
            x = []
            for feat in csv_record_list:
                x.append(_convert2float32(tf.convert_to_tensor(feat)))
            x = tf.stack(x)
            return x, y

        if process:
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


def _get_training_dataset_files(training_dataset_location, split, filter_empty=False):
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
        input_files = _get_hopsfs_dataset_files(
            training_dataset_location, split, filter_empty
        )
    elif training_dataset_location.startswith("s3"):
        input_files = _get_s3_dataset_files(training_dataset_location, split)
    else:
        raise Exception("Couldn't find execution engine.")

    return input_files


def _get_hopsfs_dataset_files(training_dataset_location, split, filter_empty):
    path = training_dataset_location.replace("hopsfs", "hdfs")
    if split is None:
        path = hdfs.path.abspath(path)
    else:
        path = hdfs.path.abspath(path + "/" + str(split))

    input_files = []

    all_list = hdfs.ls(path, recursive=True)

    # Remove directories and spark '_SUCCESS'
    include_file = True
    for file in all_list:
        # remove empty file if any
        if filter_empty:
            _file_size = hdfs.hdfs("default", 0).get_path_info(file)["size"]
            if _file_size == 0:
                include_file = False
            else:
                include_file = True
        if not hdfs.path.isdir(file) and not file.endswith("_SUCCESS") and include_file:
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


def _convert2tfdtype(input_type):
    try:
        tf_type = TFDataEngine.SPARK_TO_TFDTYPES_MAPPINGS[input_type]
    except KeyError:
        raise ValueError("Unknown type of value, please report to hsfs maintainers")
    return tf_type


def _convert2float32(input):
    if input.dtype == tf.string:
        raise ValueError(
            "tf.string feature is not allowed here. please provide process=False and preprocess "
            "dataset accordingly"
        )
    elif input.dtype in TFDataEngine.SUPPORTED_TFDTYPES:
        input = tf.cast(input, tf.float32)
    else:
        raise ValueError("Unknown type of value, please report to hsfs maintainers")
    return input
