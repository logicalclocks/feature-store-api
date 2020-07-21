try:
    import tensorflow as tf
    from pydoop import hdfs
except ModuleNotFoundError:
    pass

from hsfs.tf_utils import create_tfrecord_feature_description


class FeedModelEngine:
    def __init__(
        self,
        training_dataset,
        split,
        target_name=None,
        feature_names=None,
        is_training=True,
        cycle_length=2,
    ):
        """
        :param training_dataset:
        :param split:
        :param target_name:
        :param feature_names:
        :param is_training:
        """

        self.training_dataset = training_dataset
        self.split = split
        self.target_name = target_name
        self.feature_names = feature_names
        self.is_training = is_training
        self.cycle_length = cycle_length

        self.training_dataset_schema = self.training_dataset.schema

        if self.split is None:
            self.path = hdfs.path.abspath(self.training_dataset.location + "/" + "**")
        else:
            self.path = hdfs.path.abspath(
                self.training_dataset.location + "/" + str(split)
            )

        if self.feature_names is None:
            self.feature_names = [feat.name for feat in self.training_dataset_schema]

            for target_name in self.target_names:
                if target_name in self.feature_names:
                    self.feature_names.remove(target_name)

    def tf_record_dataset(
        self,
        batch_size=None,
        num_epochs=None,
        one_hot_encode_labels=False,
        num_classes=None,
        optimize=False,
        serialized_ndarray_fname=[],
    ):
        """
        :param batch_size:
        :param num_epochs:
        :param one_hot_encode_labels:
        :param num_classes:
        :param optimize:
        :param serialized_ndarray_fname:
        :return:
        """

        if optimize and batch_size is None and num_epochs is None:
            raise ValueError(
                "if optimize is set to True you also need to provide batch_size and num_epochs"
            )

        if one_hot_encode_labels and (num_classes is None or num_classes <= 1):
            raise ValueError(
                "if one_hot_encode_labels is set to True you also need to provide num_classes > 1"
            )

        # TODO (davit): this will be valid if spark is engine
        input_files = tf.io.gfile.glob(self.path + "/part-r-*")

        dataset = tf.data.Dataset.from_tensor_slices(input_files)

        # Convert to individual records.
        # cycle_length = 2 means that up to 2 files will be read and deserialized in
        # parallel. You may want to increase this number if you have a large number of
        # CPU cores.
        dataset = dataset.interleave(
            tf.data.TFRecordDataset,
            cycle_length=self.cycle_length,
            num_parallel_calls=tf.data.experimental.AUTOTUNE,
        )

        tfrecord_feature_description = create_tfrecord_feature_description(
            dataset, input_files
        )

        def _de_serialize(serialized_example):
            example = tf.io.parse_single_example(
                serialized_example, tfrecord_feature_description
            )
            x = []
            for feature_name in self.feature_names:
                if feature_name in serialized_ndarray_fname:
                    x.append(
                        tf.io.parse_tensor(example[feature_name], out_type=tf.float64)
                    )
                else:
                    x.append(example[feature_name])

            y = example[self.target_name]
            if one_hot_encode_labels:
                y = tf.one_hot(y, num_classes)
            return x, y

        dataset = dataset.map(
            lambda value: _de_serialize(value),
            num_parallel_calls=tf.data.experimental.AUTOTUNE,
        )

        if optimize:
            dataset = _optimize_dataset(
                dataset, batch_size, num_epochs, self.is_training
            )

        return dataset


def _optimize_dataset(dataset, batch_size, num_epochs, is_training):
    """

    :param dataset:
    :param batch_size:
    :param num_epochs:
    :return:
    """
    # tf data optimization
    if is_training:
        dataset = dataset.shuffle(num_epochs * batch_size)
        dataset = dataset.repeat()  # num_epochs * steps_per_epoch
    dataset = dataset.cache()
    dataset = dataset.batch(batch_size, drop_remainder=True)
    dataset = dataset.prefetch(tf.data.experimental.AUTOTUNE)

    return dataset
