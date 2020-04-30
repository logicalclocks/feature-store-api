try:
    import numpy as np
    import tensorflow as tf
    from petastorm import make_reader
    from petastorm.tf_utils import make_petastorm_dataset
    from pydoop import hdfs
except ModuleNotFoundError:
    pass

# from hopsworks.tf_utils import read_training_dataset_tf_record_schema
from hsfs.tf_utils import create_tf_record_schema


class FeedModelEngine:
    def __init__(
        self,
        training_dataset,
        split,
        target_name,
        feature_names=None,
        one_hot_encode_labels=False,
        num_classes=None,
    ):
        """

        :param training_dataset:
        :param split:
        :param target_name:
        :param feature_names:
        :param one_hot_encode_labels:
        :param num_classes:
        """

        # TODO (davit): check tf version and use compat functions accordingly

        self.training_dataset = training_dataset
        self.split = split
        self.target_name = target_name
        self.feature_names = feature_names
        self.one_hot_encode_labels = one_hot_encode_labels
        self.num_classes = num_classes

        self.training_dataset_schema = self.training_dataset.schema

        if self.split is None:
            self.path = hdfs.path.abspath(self.training_dataset.location + "/" + "**")
        else:
            self.path = hdfs.path.abspath(
                self.training_dataset.location + "/" + str(split)
            )

        if self.feature_names is None:
            self.feature_names = [feat.name for feat in self.training_dataset_schema]
            if self.target_name in self.feature_names:
                self.feature_names.remove(self.target_name)

        # TODO (davit): save tfrecords schema properly when creating training dataset
        # self.tf_record_schema = read_training_dataset_tf_record_schema(self.path)
        self.tf_record_schema = create_tf_record_schema(self.training_dataset_schema)

        if self.one_hot_encode_labels and self.num_classes is None:
            raise ValueError(
                "if one_hot_encode_labels is set to True you also need to provide num_classes"
            )

    def tf_record_dataset(self, batch_size=None, num_epochs=None, optimize=False):
        """

        :param batch_size:
        :param num_epochs:
        :param optimize:
        :return:
        """

        if optimize and batch_size is None and num_epochs is None:
            raise ValueError(
                "if optimize is set to True you also need to provide batch_size and num_epochs"
            )

        input_files = tf.compat.v1.io.gfile.glob(self.path + "/part-r-*")
        dataset = tf.data.TFRecordDataset(input_files)

        def _decode(sample):
            example = tf.compat.v1.io.parse_single_example(
                sample, self.tf_record_schema
            )
            x = []
            for feature_name in self.feature_names:
                x.append(example[feature_name])

            # TODO (davit): provide user ability to enter tf.dtypes.DType
            y = [tf.cast(example[self.target_name], tf.float32)]
            return x, y

        dataset = dataset.map(_decode)

        if self.one_hot_encode_labels:
            dataset = dataset.map(
                lambda *x: _one_hot_encode(
                    x[0], tf.dtypes.cast(x[1], tf.int32), self.num_classes
                )
            )

        if optimize:
            dataset = _optimize_dataset(dataset, batch_size, num_epochs)

        return dataset

    def numpy(self):
        """

        :return:
        """
        tfdata = self.tf_record_dataset()

        # TODO (davit): make also for tf2
        iterator = tfdata.make_one_shot_iterator().get_next()
        features = []
        target = []
        with tf.compat.v1.Session() as sess:
            while True:
                try:
                    batch_array = sess.run(iterator)
                    features.append(batch_array[0])
                    target.append(batch_array[1])
                except tf.errors.OutOfRangeError:
                    break
        features = np.array(features)
        target = np.array(target).flatten()
        return features, target

    # TODO  (davit): this function work in progress
    def tf_petastorm_dataset(
        self,
        workers_count,
        shuffle_row_groups,
        batch_size=None,
        num_epochs=None,
        optimize=False,
    ):
        """

        :param workers_count:
        :param shuffle_row_groups:
        :param batch_size:
        :param num_epochs:
        :param optimize:
        :return:
        """

        with make_reader(
            self.path,
            num_epochs=None,
            hdfs_driver="libhdfs",
            workers_count=workers_count,
            shuffle_row_groups=shuffle_row_groups,
        ) as reader:
            dataset = make_petastorm_dataset(reader)

            def _row_mapper(sample):
                out_dict = dict()

                for feature in self.feature_names:
                    if hasattr(sample, feature):
                        out_dict[feature] = getattr(sample, feature)
                    else:
                        print("{} is not known".format(feature))

                label = getattr(sample, self.target_name)

                return out_dict, label

            dataset = dataset.map(_row_mapper)

        if self.one_hot_encode_labels:
            dataset = dataset.map(
                lambda *x: _one_hot_encode(
                    x[0], tf.dtypes.cast(x[1], tf.int32), self.num_classes
                )
            )

        if optimize:
            dataset = _optimize_dataset(dataset, batch_size, num_epochs)

        return dataset


def _optimize_dataset(dataset, batch_size, num_epochs):
    """

    :param dataset:
    :param batch_size:
    :param num_epochs:
    :return:
    """
    # tf data optimization
    dataset = dataset.repeat()  # num_epochs * steps_per_epoch
    dataset = dataset.cache()
    dataset = dataset.shuffle(num_epochs * batch_size)
    dataset = dataset.batch(batch_size, drop_remainder=True)
    dataset = dataset.prefetch(tf.data.experimental.AUTOTUNE)

    options = tf.data.Options()

    options.experimental_stats.latency_all_edges = True

    options.experimental_optimization.noop_elimination = True
    options.experimental_optimization.map_vectorization.enabled = True
    options.experimental_optimization.apply_default_optimizations = False

    # options.experimental_threading.private_threadpool_size = 10

    dataset = dataset.with_options(options)

    return dataset


def _one_hot_encode(features, target, num_classes):
    return features, tf.one_hot(target, num_classes)
