try:
    import tensorflow as tf
except ModuleNotFoundError:
    pass


import itertools


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


def create_tfrecord_feature_description(dataset, train_filenames):
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
        feature_type = tf.io.FixedLenFeature([feature_length], tf.int64)
    elif v.float_list.value:
        result = v.float_list.value
        feature_length = len(result)
        feature_type = tf.io.FixedLenFeature([feature_length], tf.float32)
    elif v.bytes_list.value:
        feature_type = tf.io.FixedLenFeature([], tf.string)

    return k, feature_type
