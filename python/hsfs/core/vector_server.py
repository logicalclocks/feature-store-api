#
#   Copyright 2022 Logical Clocks AB
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
import io
import avro.schema
import avro.io
from sqlalchemy import sql, bindparam, exc, text
from hsfs import util
from hsfs import training_dataset, feature_view
from hsfs.core import (
    training_dataset_api,
    storage_connector_api,
    transformation_function_engine,
    feature_view_api,
    feature_view_engine,
)


class VectorServer:
    def __init__(self, feature_store_id, training_dataset_version=None):
        self._training_dataset_version = training_dataset_version
        self._prepared_statement_engine = None
        self._prepared_statements = None
        self._serving_keys = None
        self._pkname_by_serving_index = None
        self._feature_store_id = feature_store_id
        self._training_dataset_api = training_dataset_api.TrainingDatasetApi(
            feature_store_id
        )
        self._feature_view_api = feature_view_api.FeatureViewApi(feature_store_id)
        self._storage_connector_api = storage_connector_api.StorageConnectorApi(
            feature_store_id
        )
        self._transformation_function_engine = (
            transformation_function_engine.TransformationFunctionEngine(
                feature_store_id
            )
        )
        self._feature_view_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id
        )

    def init_serving(self, vector_server, batch, external):
        # `init_prepared_statement` should be the last because other initialisations
        # has to be done successfully before it is able to fetch feature vectors.
        self.init_transformation(vector_server)
        self.init_prepared_statement(vector_server, batch, external)

    def init_transformation(self, vector_server):
        # attach transformation functions
        vector_server.transformation_functions = self._get_transformation_fns(
            vector_server
        )

    def init_prepared_statement(self, vector_server, batch, external):
        if isinstance(vector_server, feature_view.FeatureView):
            prepared_statements = self._feature_view_api.get_serving_prepared_statement(
                vector_server.name, vector_server.version, batch
            )
        elif isinstance(vector_server, training_dataset.TrainingDataset):
            prepared_statements = (
                self._training_dataset_api.get_serving_prepared_statement(
                    vector_server, batch
                )
            )
        else:
            raise ValueError(
                "Object type needs to be `feature_view.FeatureView` or `training_dataset.TrainingDataset`."
            )
        # reset values to default, as user may be re-initialising with different parameters
        self.prepared_statement_engine = None
        self.prepared_statements = None
        self.serving_keys = None

        self._set_mysql_connection(external)

        prepared_statements_dict = {}
        pkname_by_serving_index = {}
        serving_vector_keys = set()
        for prepared_statement in prepared_statements:
            query_online = str(prepared_statement.query_online).replace("\n", " ")

            # In java prepared statement `?` is used for parametrization.
            # In sqlalchemy `:feature_name` is used instead of `?`
            pk_names = [
                psp.name
                for psp in sorted(
                    prepared_statement.prepared_statement_parameters,
                    key=lambda psp: psp.index,
                )
            ]

            for pk_name in pk_names:
                serving_vector_keys.add(pk_name)

            if not batch:
                for pk_name in pk_names:
                    query_online = self._parametrize_query(pk_name, query_online)
                query_online = sql.text(query_online)
            else:
                query_online = self._parametrize_query("batch_ids", query_online)
                query_online = sql.text(query_online)
                query_online = query_online.bindparams(
                    batch_ids=bindparam("batch_ids", expanding=True)
                )
                pkname_by_serving_index[
                    prepared_statement.prepared_statement_index
                ] = pk_names

            prepared_statements_dict[
                prepared_statement.prepared_statement_index
            ] = query_online

        self.prepared_statements = prepared_statements_dict
        self.serving_keys = serving_vector_keys

        if batch:
            self._pkname_by_serving_index = pkname_by_serving_index

    def get_preview_vectors(self, vector_server, external, n):
        batch = n > 1
        if self.prepared_statements is None:
            self.init_serving(vector_server, batch, external)
        entry = dict([(key, None) for key in self.serving_keys])
        if batch:
            return self.get_feature_vectors(vector_server, entry, external, n)
        else:
            return self.get_feature_vector(vector_server, entry, external, n)

    def get_feature_vector(self, vector_server, entry, external, preview_sample=0):
        """Assembles serving vector from online feature store."""

        if not preview_sample and all(
            [isinstance(val, list) for val in entry.values()]
        ):
            raise ValueError(
                "Entry is expected to be single value per primary key. "
                "If you have already initialised prepared statements for single vector and now want to retrieve "
                "batch vector please reinitialise prepared statements with  "
                "`training_dataset.init_prepared_statement()` "
                "or `feature_view.init_serving()`"
            )

        serving_vector = []

        if self.prepared_statements is None:
            self.init_serving(vector_server, False, external)

        # check if primary key map correspond to serving_keys.
        if not entry.keys() == self.serving_keys:
            raise ValueError(
                "Provided primary key map doesn't correspond to serving_keys"
            )

        # get schemas for complex features once
        complex_features = self.get_complex_feature_schemas(vector_server)

        self.refresh_mysql_connection(external)
        for prepared_statement_index in self.prepared_statements:
            prepared_statement = self.prepared_statements[prepared_statement_index]
            if preview_sample:
                prepared_statement = self._make_preview_statement(
                    prepared_statement, preview_sample
                )
            with self.prepared_statement_engine.connect() as mysql_conn:
                result_proxy = mysql_conn.execute(prepared_statement, entry).fetchall()
            result_dict = {}
            for row in result_proxy:
                result_dict = self.deserialize_complex_features(
                    complex_features, dict(row.items())
                )
                if not result_dict:
                    raise Exception(
                        "No data was retrieved from online feature store using input "
                        + entry
                    )
                # apply transformation functions
                result_dict = self._apply_transformation(
                    vector_server.transformation_functions, result_dict
                )
            serving_vector += list(result_dict.values())

        return serving_vector

    def get_feature_vectors(self, vector_server, entry, external, preview_sample=0):
        """Assembles serving vector from online feature store."""

        # initialize prepared statements
        if self.prepared_statements is None:
            self.init_serving(vector_server, True, external)

        if not preview_sample and not all(
            [isinstance(val, list) for val in entry.values()]
        ):
            raise ValueError(
                "Entry is expected to be list of primary key values. "
                "If you have already initialised for batch serving and now want to retrieve single vector "
                "please reinitialise prepared statements with `training_dataset.init_prepared_statement()` "
                "or `feature_view.init_serving()`"
            )

        # create dict object that will have of order of the vector as key and values as
        # vector itself to stitch them correctly if there are multiple feature groups involved. At this point we
        # expect that backend will return correctly ordered vectors.
        batch_dicts = {}

        # check if primary key map correspond to serving_keys.
        if not entry.keys() == self.serving_keys:
            raise ValueError(
                "Provided primary key map doesn't correspond to serving_keys"
            )
        if not len({len(i) for i in entry.keys()}) == 1:
            raise Exception(
                "Not all primary keys have the same length. Please make sure that all primary key have "
                "the same length."
            )

        # get schemas for complex features once
        complex_features = self.get_complex_feature_schemas(vector_server)

        self.refresh_mysql_connection(external)
        for prepared_statement_index in self.prepared_statements:
            order_in_batch = 0
            prepared_statement = self.prepared_statements[prepared_statement_index]
            if preview_sample:
                prepared_statement = self._make_preview_statement(
                    prepared_statement, preview_sample
                )
            with self.prepared_statement_engine.connect() as mysql_conn:
                result_proxy = mysql_conn.execute(
                    prepared_statement,
                    batch_ids=None
                    if preview_sample
                    else tuple(
                        zip(
                            *[
                                entry.get(key)
                                for key in self._pkname_by_serving_index[
                                    prepared_statement_index
                                ]
                            ]
                        )
                    ),
                ).fetchall()
            for row in result_proxy:
                result_dict = self.deserialize_complex_features(
                    complex_features, dict(row.items())
                )
                if not result_dict:
                    raise Exception(
                        "No data was retrieved from online feature store using input "
                        + entry
                    )
                # apply transformation functions
                result_dict = self._apply_transformation(
                    vector_server.transformation_functions, result_dict
                )
                if order_in_batch in batch_dicts:
                    batch_dicts[order_in_batch] += list(result_dict.values())
                else:
                    batch_dicts[order_in_batch] = list(result_dict.values())
                order_in_batch += 1

        if not len({len(i) for i in list(batch_dicts.values())}) == 1:
            raise Exception(
                "Not all feature vectors have the same length. Please make sure that all primary key values "
                "are correct."
            )

        return list(batch_dicts.values())

    def get_complex_feature_schemas(self, vector_server):
        return {
            f.name: avro.io.DatumReader(
                avro.schema.parse(f._feature_group._get_feature_avro_schema(f.name))
            )
            for f in vector_server.schema
            if f.is_complex()
        }

    def deserialize_complex_features(self, feature_schemas, row_dict):
        for feature_name, schema in feature_schemas.items():
            bytes_reader = io.BytesIO(row_dict[feature_name])
            decoder = avro.io.BinaryDecoder(bytes_reader)
            row_dict[feature_name] = schema.read(decoder)
        return row_dict

    def refresh_mysql_connection(self, external):
        try:
            with self.prepared_statement_engine.connect():
                pass
        except exc.OperationalError:
            self._set_mysql_connection(external)

    def _make_preview_statement(self, statement, n):
        return text(statement.text[: statement.text.find(" WHERE ")] + f" LIMIT {n}")

    def _set_mysql_connection(self, external):
        online_conn = self._storage_connector_api.get_online_connector()
        mysql_engine = util.create_mysql_engine(online_conn, external)
        self.prepared_statement_engine = mysql_engine

    @staticmethod
    def _apply_transformation(transformation_fns, row_dict):
        for feature_name in transformation_fns:
            if feature_name in row_dict:
                transformation_fn = transformation_fns[feature_name].transformation_fn
                row_dict[feature_name] = transformation_fn(row_dict[feature_name])
        return row_dict

    @staticmethod
    def _parametrize_query(name, query_online):
        # Now we have ordered pk_names, iterate over it and replace `?` with `:feature_name` one by one.
        # Regex `"^(.*?)\?"` will identify 1st occurrence of `?` in the sql string and replace it with provided
        # feature name, e.g. `:feature_name`:. As we iteratively update `query_online` we are always aiming to
        # replace 1st occurrence of `?`. This approach can only work if primary key names are sorted properly.
        # Regex `"^(.*?)\?"`:
        # `^` - asserts position at start of a line
        # `.*?` - matches any character (except for line terminators). `*?` Quantifier —
        # Matches between zero and unlimited times, expanding until needed, i.e 1st occurrence of `\?`
        # character.
        return re.sub(
            r"^(.*?)\?",
            r"\1:" + name,
            query_online,
        )

    def _get_transformation_fns(self, vector_server):
        # get attached transformation functions
        transformation_functions = (
            self._transformation_function_engine.get_td_transformation_fn(vector_server)
            if isinstance(vector_server, training_dataset.TrainingDataset)
            else (
                self._feature_view_engine.get_attached_transformation_fn(
                    vector_server.name, vector_server.version
                )
            )
        )
        is_stat_required = (
            len(
                set(self._transformation_function_engine.BUILTIN_FN_NAMES).intersection(
                    set([tf.name for tf in transformation_functions.values()])
                )
            )
            > 0
        )
        is_feat_view = isinstance(vector_server, feature_view.FeatureView)
        if not is_stat_required:
            td_tffn_stats = None
        else:
            # if there are any built-in transformation functions get related statistics and
            # populate with relevant arguments
            # there should be only one statistics object with for_transformation=true
            if is_feat_view and self._training_dataset_version is None:
                raise ValueError(
                    "Training data version is required for transformation. Call `feature_view.init_serving(version)` to pass the training dataset version."
                    "Training data can be created by `feature_view.create_training_data` or `feature_view.get_training_data`."
                )
            td_tffn_stats = self._feature_view_engine._statistics_engine.get_last(
                vector_server,
                for_transformation=True,
                training_dataset_version=self._training_dataset_version,
            )

        if is_stat_required and td_tffn_stats is None:
            raise ValueError(
                "No statistics available for initializing transformation functions."
            )

        transformation_fns = (
            self._transformation_function_engine.populate_builtin_attached_fns(
                transformation_functions,
                td_tffn_stats.content if td_tffn_stats is not None else None,
            )
        )
        return transformation_fns

    @property
    def prepared_statement_engine(self):
        """JDBC connection engine to retrieve connections to online features store from."""
        return self._prepared_statement_engine

    @prepared_statement_engine.setter
    def prepared_statement_engine(self, prepared_statement_engine):
        self._prepared_statement_engine = prepared_statement_engine

    @property
    def prepared_statements(self):
        """The dict object of prepared_statements as values and kes as indices of positions in the query for
        selecting features from feature groups of the training dataset.
        """
        return self._prepared_statements

    @prepared_statements.setter
    def prepared_statements(self, prepared_statements):
        self._prepared_statements = prepared_statements

    @property
    def serving_keys(self):
        """Set of primary key names that is used as keys in input dict object for `get_serving_vector` method."""
        return self._serving_keys

    @serving_keys.setter
    def serving_keys(self, serving_vector_keys):
        self._serving_keys = serving_vector_keys

    @property
    def training_dataset_version(self):
        return self._training_dataset_version

    @training_dataset_version.setter
    def training_dataset_version(self, training_dataset_version):
        self._training_dataset_version = training_dataset_version
