from hopsworks import engine
from hopsworks.core import training_dataset_api


class TrainingDatasetEngine:
    OVERWRITE = "overwrite"
    APPEND = "append"

    def __init__(self, feature_store_id):
        self._training_dataset_api = training_dataset_api.TrainingDatasetApi(
            feature_store_id
        )

    def create(self, training_dataset, feature_dataframe, user_write_options):
        # TODO: remove when backend saves the splits, just for now for testing
        splits = training_dataset.splits
        self._training_dataset_api.post(training_dataset)
        training_dataset.splits = splits

        write_options = engine.get_instance().write_options(
            training_dataset.data_format, user_write_options
        )

        self._write(training_dataset, feature_dataframe, write_options, self.OVERWRITE)

    def insert(
        self, training_dataset, feature_dataframe, user_write_options, overwrite
    ):
        # validate matching schema
        engine.get_instance().schema_matches(feature_dataframe, training_dataset.schema)

        write_options = engine.get_instance().write_options(
            training_dataset.data_format, user_write_options
        )

        self._write(
            training_dataset,
            feature_dataframe,
            write_options,
            self.OVERWRITE if overwrite else self.APPEND,
        )

    def read(self, training_dataset, split, user_read_options):
        if split is None:
            path = training_dataset.location + "/" + "**"
        else:
            path = training_dataset.location + "/" + str(split)

        read_options = engine.get_instance().read_options(
            training_dataset.data_format, user_read_options
        )

        return engine.get_instance().read(
            training_dataset.data_format, read_options, path
        )

    def _write(self, training_dataset, dataset, write_options, save_mode):
        if training_dataset.splits is None:
            path = training_dataset.location + "/" + training_dataset.name
            self._write_single(
                dataset,
                training_dataset.storage_connector,
                training_dataset.data_format,
                write_options,
                save_mode,
                path,
            )
        else:
            split_names = sorted([*training_dataset.splits])
            split_weights = [training_dataset.splits[i] for i in split_names]
            self._write_splits(
                dataset.randomSplit(split_weights, training_dataset.seed),
                training_dataset.storage_connector,
                training_dataset.data_format,
                write_options,
                save_mode,
                training_dataset.location,
                split_names,
            )

    def _write_splits(
        self,
        feature_dataframe_list,
        storage_connector,
        data_format,
        write_options,
        save_mode,
        path,
        split_names,
    ):
        for i in range(len(feature_dataframe_list)):
            split_path = path + "/" + str(split_names[i])
            self._write_single(
                feature_dataframe_list[i],
                storage_connector,
                data_format,
                write_options,
                save_mode,
                split_path,
            )

    def _write_single(
        self,
        feature_dataframe,
        storage_connector,
        data_format,
        write_options,
        save_mode,
        path,
    ):
        # TODO: currently not supported petastorm, hdf5 and npy file formats
        engine.get_instance().write(
            feature_dataframe,
            storage_connector,
            data_format,
            save_mode,
            write_options,
            path,
        )
