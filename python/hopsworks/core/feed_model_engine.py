try:
    import tensorflow as tf
    from petastorm import make_reader
    from petastorm.tf_utils import make_petastorm_dataset
    from pydoop import hdfs
except ModuleNotFoundError:
    pass

from hopsworks.tf_utils import read_training_dataset_tf_record_schema


class FeedModelEngine:
    def __init__(
        self,
        training_dataset,
        split,
        batch_size,
        num_epochs,
        label_name,
        feature_names=None,
        one_hot_encode_labels=False,
        num_classes=None,
        optimize=False,
    ):

        self.training_dataset = training_dataset
        self.split = split
        self.batch_size = batch_size
        self.num_epochs = num_epochs
        self.label_name = label_name
        self.feature_names = feature_names
        self.one_hot_encode_labels = one_hot_encode_labels
        self.num_classes = num_classes
        self.optimize = optimize

        self.training_dataset_schema = self.training_dataset.schema

        if self.split is None:
            self.path = hdfs.path.abspath(self.training_dataset.location + "/" + "**")
        else:
            self.path = hdfs.path.abspath(
                self.training_dataset.location + "/" + str(split)
            )

        if self.feature_names is None:
            self.feature_names = self.training_dataset_schema
            if self.label_name in self.feature_names:
                self.feature_names.remove(self.label_name)

        self.tf_record_schema = read_training_dataset_tf_record_schema(self.path)

    def TFRecordDataset(self):
        # TODO (davit): check tf version and use compat functions accordingly

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
            y = [tf.cast(example[self.label_name], tf.float32)]
            return x, y

        dataset = dataset.map(_decode)

        if self.optimize:
            dataset = _optimize_dataset(dataset, self.batch_size, self.num_epochs)

        return dataset

    def TFPetastormDataset(self, workers_count, shuffle_row_groups):

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

                label = getattr(sample, self.label_name)

                return out_dict, label

            dataset = dataset.map(_row_mapper)

        if self.optimize:
            dataset = _optimize_dataset(dataset, self.batch_size, self.num_epochs)

        return dataset


def _optimize_dataset(dataset, batch_size, num_epochs):
    # tf data optimization
    dataset = dataset.repeat()  # num_epochs * steps_per_epoch
    dataset = dataset.cache()
    dataset = dataset.shuffle(num_epochs * batch_size)
    dataset = dataset.batch(batch_size, drop_remainder=True)
    dataset = dataset.prefetch(tf.data.experimental.AUTOTUNE)

    # https://www.tensorflow.org/versions/r1.15/api_docs/python/tf/data/Options
    options = tf.data.Options()

    options.experimental_stats.latency_all_edges = True

    options.experimental_optimization.noop_elimination = True
    options.experimental_optimization.map_vectorization.enabled = True
    options.experimental_optimization.apply_default_optimizations = False

    options.experimental_threading.private_threadpool_size = (
        10  # TODO (davit): AUTOTUNE
    )

    # options.experimental_slack = True # TODO (davit):

    dataset = dataset.with_options(options)

    return dataset
