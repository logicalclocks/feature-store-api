import json

try:
    import tensorflow as tf
    from pydoop import hdfs
except ModuleNotFoundError:
    pass


def store_tf_record_schema_spark_hdfs(spark_df, hdfs_path):
    """
    Stores a tfrecord json schema to HDFS

    Args:
        :spark_df: spark data frame
        :hdfs_path: the hdfs path to store it

    Returns:
        None
    """

    _, tfrecord_schema = _get_dataframe_tf_record_schema_json(spark_df, fixed=True)
    json_str = json.dumps(tfrecord_schema)
    path = hdfs.path.abspath(hdfs_path)
    hdfs.dump(
        json_str, path + "/" + TF_CONSTANTS.TRAINING_DATASET_TF_RECORD_SCHEMA_FILE_NAME
    )


def read_training_dataset_tf_record_schema(hdfs_path):
    tf_record_json_schema = json.loads(
        hdfs.load(
            hdfs_path + "/" + TF_CONSTANTS.TRAINING_DATASET_TF_RECORD_SCHEMA_FILE_NAME
        )
    )
    return _convert_tf_record_schema_json_to_dict(tf_record_json_schema)


def _get_spark_array_size(spark_df, array_col_name):
    """
    Gets the size of an array column in the dataframe

    Args:
        :spark_df: the spark dataframe that contains the array column
        :array_col_name: the name of the array column in the spark dataframe

    Returns:
        The length of the the array column (assuming fixed size)
    """
    return len(getattr(spark_df.select(array_col_name).first(), array_col_name))


def _get_dataframe_tf_record_schema_json(spark_df, fixed=True):
    """
    Infers the tf-record schema from a spark dataframe
    Note: this method is just for convenience, it should work in 99% of cases but it is not guaranteed,
    if spark or tensorflow introduces new datatypes this will break. The user can always fallback to encoding the
    tf-example-schema manually.

    Can only handle one level of nesting, e.g arrays inside the dataframe is okay but having a schema of
     array<array<float>> will not work.

    Args:
        :spark_df: the spark dataframe to infer the tensorflow example record from
        :fixed: boolean flag indicating whether array columns should be treated with fixed size or variable size

    Returns:
        a dict with the tensorflow example as well as a json friendly version of the schema

    Raises:
        :InferTFRecordSchemaError: if a tf record schema could not be inferred from the dataframe
    """
    example = {}
    example_json = {}
    for col in spark_df.dtypes:
        if col[1] in TF_CONSTANTS.TF_RECORD_INT_SPARK_TYPES:
            example[str(col[0])] = tf.io.FixedLenFeature([], tf.int64)
            example_json[str(col[0])] = {
                TF_CONSTANTS.TF_RECORD_SCHEMA_FEATURE: TF_CONSTANTS.TF_RECORD_SCHEMA_FEATURE_FIXED,
                TF_CONSTANTS.TF_RECORD_SCHEMA_TYPE: TF_CONSTANTS.TF_RECORD_INT_TYPE,
            }
            tf.io.FixedLenFeature([], tf.int64)
        if col[1] in TF_CONSTANTS.TF_RECORD_FLOAT_SPARK_TYPES:
            example[str(col[0])] = tf.io.FixedLenFeature([], tf.float32)
            example_json[str(col[0])] = {
                TF_CONSTANTS.TF_RECORD_SCHEMA_FEATURE: TF_CONSTANTS.TF_RECORD_SCHEMA_FEATURE_FIXED,
                TF_CONSTANTS.TF_RECORD_SCHEMA_TYPE: TF_CONSTANTS.TF_RECORD_FLOAT_TYPE,
            }
        if col[1] in TF_CONSTANTS.TF_RECORD_INT_ARRAY_SPARK_TYPES:
            if fixed:
                array_len = _get_spark_array_size(spark_df, str(col[0]))
                example[str(col[0])] = tf.io.FixedLenFeature(
                    shape=[array_len], dtype=tf.int64
                )
                example_json[str(col[0])] = {
                    TF_CONSTANTS.TF_RECORD_SCHEMA_FEATURE: TF_CONSTANTS.TF_RECORD_SCHEMA_FEATURE_FIXED,
                    TF_CONSTANTS.TF_RECORD_SCHEMA_TYPE: TF_CONSTANTS.TF_RECORD_INT_TYPE,
                    TF_CONSTANTS.TF_RECORD_SCHEMA_SHAPE: [array_len],
                }
            else:
                example[str(col[0])] = tf.VarLenFeature(tf.int64)
                example_json[str(col[0])] = {
                    TF_CONSTANTS.TF_RECORD_SCHEMA_FEATURE: TF_CONSTANTS.TF_RECORD_SCHEMA_FEATURE_VAR,
                    TF_CONSTANTS.TF_RECORD_SCHEMA_TYPE: TF_CONSTANTS.TF_RECORD_INT_TYPE,
                }
        if col[1] in TF_CONSTANTS.TF_RECORD_FLOAT_ARRAY_SPARK_TYPES:
            if fixed:
                array_len = _get_spark_array_size(spark_df, str(col[0]))
                example[str(col[0])] = tf.io.FixedLenFeature(
                    shape=[array_len], dtype=tf.float32
                )
                example_json[str(col[0])] = {
                    TF_CONSTANTS.TF_RECORD_SCHEMA_FEATURE: TF_CONSTANTS.TF_RECORD_SCHEMA_FEATURE_FIXED,
                    TF_CONSTANTS.TF_RECORD_SCHEMA_TYPE: TF_CONSTANTS.TF_RECORD_FLOAT_TYPE,
                    TF_CONSTANTS.TF_RECORD_SCHEMA_SHAPE: [array_len],
                }
            else:
                example[str(col[0])] = tf.io.VarLenFeature(tf.float32)
                example_json[str(col[0])] = {
                    TF_CONSTANTS.TF_RECORD_SCHEMA_FEATURE: TF_CONSTANTS.TF_RECORD_SCHEMA_FEATURE_VAR,
                    TF_CONSTANTS.TF_RECORD_SCHEMA_TYPE: TF_CONSTANTS.TF_RECORD_FLOAT_TYPE,
                }
        if (
            col[1] in TF_CONSTANTS.TF_RECORD_STRING_ARRAY_SPARK_TYPES
            or col[1] in TF_CONSTANTS.TF_RECORD_STRING_SPARK_TYPES
        ):
            example[str(col[0])] = tf.io.VarLenFeature(tf.string)
            example_json[str(col[0])] = {
                TF_CONSTANTS.TF_RECORD_SCHEMA_FEATURE: TF_CONSTANTS.TF_RECORD_SCHEMA_FEATURE_VAR,
                TF_CONSTANTS.TF_RECORD_SCHEMA_TYPE: TF_CONSTANTS.TF_RECORD_STRING_TYPE,
            }

        if col[1] not in TF_CONSTANTS.RECOGNIZED_TF_RECORD_TYPES:
            raise InferTFRecordSchemaError(
                "Could not recognize the spark type: {} for inferring the tf-records schema."
                "Recognized types are: {}".format(
                    col[1], TF_CONSTANTS.RECOGNIZED_TF_RECORD_TYPES
                )
            )
    return example, example_json


def _convert_tf_record_schema_json_to_dict(tf_record_json_schema):
    """
    Converts a JSON version of a tf record schema into a dict that is a valid tf example schema

    Args:
        :tf_record_json_schema: the json version to convert

    Returns:
        the converted schema
    """
    example = {}
    for key, value in tf_record_json_schema.items():
        if (
            value[TF_CONSTANTS.TF_RECORD_SCHEMA_FEATURE]
            == TF_CONSTANTS.TF_RECORD_SCHEMA_FEATURE_FIXED
            and value[TF_CONSTANTS.TF_RECORD_SCHEMA_TYPE]
            == TF_CONSTANTS.TF_RECORD_INT_TYPE
        ):
            if TF_CONSTANTS.TF_RECORD_SCHEMA_SHAPE in value:
                example[str(key)] = tf.io.FixedLenFeature(
                    shape=value[TF_CONSTANTS.TF_RECORD_SCHEMA_SHAPE], dtype=tf.int64
                )
            else:
                example[str(key)] = tf.io.FixedLenFeature([], tf.int64)
        if (
            value[TF_CONSTANTS.TF_RECORD_SCHEMA_FEATURE]
            == TF_CONSTANTS.TF_RECORD_SCHEMA_FEATURE_FIXED
            and value[TF_CONSTANTS.TF_RECORD_SCHEMA_TYPE]
            == TF_CONSTANTS.TF_RECORD_FLOAT_TYPE
        ):
            if TF_CONSTANTS.TF_RECORD_SCHEMA_SHAPE in value:
                example[str(key)] = tf.io.FixedLenFeature(
                    shape=value[TF_CONSTANTS.TF_RECORD_SCHEMA_SHAPE], dtype=tf.float32
                )
            else:
                example[str(key)] = tf.io.FixedLenFeature([], tf.float32)
        if (
            value[TF_CONSTANTS.TF_RECORD_SCHEMA_FEATURE]
            == TF_CONSTANTS.TF_RECORD_SCHEMA_FEATURE_VAR
            and value[TF_CONSTANTS.TF_RECORD_SCHEMA_TYPE]
            == TF_CONSTANTS.TF_RECORD_INT_TYPE
        ):
            example[str(key)] = tf.VarLenFeature(tf.int64)
        if (
            value[TF_CONSTANTS.TF_RECORD_SCHEMA_FEATURE]
            == TF_CONSTANTS.TF_RECORD_SCHEMA_FEATURE_VAR
            and value[TF_CONSTANTS.TF_RECORD_SCHEMA_TYPE]
            == TF_CONSTANTS.TF_RECORD_FLOAT_TYPE
        ):
            example[str(key)] = tf.VarLenFeature(tf.float32)
        if (
            value[TF_CONSTANTS.TF_RECORD_SCHEMA_FEATURE]
            == TF_CONSTANTS.TF_RECORD_SCHEMA_FEATURE_VAR
            and value[TF_CONSTANTS.TF_RECORD_SCHEMA_TYPE]
            == TF_CONSTANTS.TF_RECORD_STRING_TYPE
        ):
            example[str(key)] = tf.VarLenFeature(tf.string)
    return example


class InferTFRecordSchemaError(Exception):
    """This exception will be raised if there is an error in inferring the tfrecord schema of a dataframe"""


class SPARK_CONFIG:
    """
    Spark string constants
    """

    SPARK_SCHEMA_FIELD_METADATA = "metadata"
    SPARK_SCHEMA_FIELDS = "fields"
    SPARK_SCHEMA_FIELD_NAME = "name"
    SPARK_SCHEMA_FIELD_TYPE = "type"
    SPARK_SCHEMA_ELEMENT_TYPE = "elementType"
    SPARK_OVERWRITE_MODE = "overwrite"
    SPARK_APPEND_MODE = "append"
    SPARK_WRITE_DELIMITER = "delimiter"
    SPARK_WRITE_HEADER = "header"
    SPARK_TF_CONNECTOR_RECORD_TYPE = "recordType"
    SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE = "Example"
    SPARK_LONG_TYPE = "long"
    SPARK_SHORT_TYPE = "short"
    SPARK_BYTE_TYPE = "byte"
    SPARK_INTEGER_TYPE = "integer"
    SPARK_INT_TYPE = "int"
    SPARK_FLOAT_TYPE = "float"
    SPARK_DOUBLE_TYPE = "double"
    SPARK_DECIMAL_TYPE = "decimal"
    SPARK_BIGINT_TYPE = "bigint"
    SPARK_SMALLINT_TYPE = "smallint"
    SPARK_STRING_TYPE = "string"
    SPARK_BINARY_TYPE = "binary"
    SPARK_NUMERIC_TYPES = [
        SPARK_BIGINT_TYPE,
        SPARK_DECIMAL_TYPE,
        SPARK_INTEGER_TYPE,
        SPARK_INT_TYPE,
        SPARK_DOUBLE_TYPE,
        SPARK_LONG_TYPE,
        SPARK_FLOAT_TYPE,
        SPARK_SHORT_TYPE,
    ]
    SPARK_STRUCT = "struct"
    SPARK_ARRAY = "array"
    SPARK_ARRAY_DOUBLE = "array<double>"
    SPARK_ARRAY_INTEGER = "array<integer>"
    SPARK_ARRAY_INT = "array<int>"
    SPARK_ARRAY_BIGINT = "array<bigint>"
    SPARK_ARRAY_FLOAT = "array<float>"
    SPARK_ARRAY_DECIMAL = "array<decimal>"
    SPARK_ARRAY_STRING = "array<string>"
    SPARK_ARRAY_LONG = "array<long>"
    SPARK_ARRAY_BINARY = "array<binary>"
    SPARK_VECTOR = "vector"
    SPARK_SQL_CATALOG_IMPLEMENTATION = "spark.sql.catalogImplementation"
    SPARK_SQL_CATALOG_HIVE = "hive"
    SPARK_JDBC_FORMAT = "jdbc"
    SPARK_JDBC_URL = "url"
    SPARK_JDBC_DBTABLE = "dbtable"
    SPARK_JDBC_USER = "user"
    SPARK_JDBC_PW = "password"


# class MODEL_SERVING:
#     MODELS_DATASET = "Models"
#     SERVING_TYPE_TENSORFLOW = "TENSORFLOW"
#     SERVING_TYPE_SKLEARN = "SKLEARN"
#     SERVING_TYPES = [SERVING_TYPE_TENSORFLOW, SERVING_TYPE_SKLEARN]
#     SERVING_ACTION_START = "START"
#     SERVING_ACTION_STOP = "STOP"
#     SERVING_ACTIONS = [SERVING_ACTION_STOP, SERVING_ACTION_STOP]
#     SERVING_START_OR_STOP_PATH_PARAM = "?action="
#
class TF_CONSTANTS:
    """
     Featurestore constants
    """

    # TRAINING_DATASET_PROVENANCE_FEATUREGROUP = "featuregroup"
    # TRAINING_DATASET_PROVENANCE_VERSION = "version"
    # MAX_CORRELATION_MATRIX_COLUMNS = 50
    # TRAINING_DATASET_CSV_FORMAT = "csv"
    # TRAINING_DATASET_TSV_FORMAT = "tsv"
    # TRAINING_DATASET_PARQUET_FORMAT = "parquet"
    # TRAINING_DATASET_TFRECORDS_FORMAT = "tfrecords"
    # TRAINING_DATASET_AVRO_FORMAT = "avro"
    # TRAINING_DATASET_ORC_FORMAT = "orc"
    # TRAINING_DATASET_NPY_FORMAT = "npy"
    # TRAINING_DATASET_IMAGE_FORMAT = "image"
    # TRAINING_DATASET_HDF5_FORMAT = "hdf5"
    # TRAINING_DATASET_PETASTORM_FORMAT = "petastorm"
    # TRAINING_DATASET_NPY_SUFFIX = ".npy"
    # TRAINING_DATASET_HDF5_SUFFIX = ".hdf5"
    # TRAINING_DATASET_CSV_SUFFIX = ".csv"
    # TRAINING_DATASET_TSV_SUFFIX = ".tsv"
    # TRAINING_DATASET_PARQUET_SUFFIX = ".parquet"
    # TRAINING_DATASET_AVRO_SUFFIX = ".avro"
    # TRAINING_DATASET_ORC_SUFFIX = ".orc"
    # TRAINING_DATASET_IMAGE_SUFFIX = ".image"
    # TRAINING_DATASET_TFRECORDS_SUFFIX = ".tfrecords"
    # TRAINING_DATASET_PETASTORM_SUFFIX = ".petastorm"
    # TRAINING_DATASET_SUPPORTED_FORMATS = [
    #     TRAINING_DATASET_TSV_FORMAT,
    #     TRAINING_DATASET_CSV_FORMAT,
    #     TRAINING_DATASET_PARQUET_FORMAT,
    #     TRAINING_DATASET_TFRECORDS_FORMAT,
    #     TRAINING_DATASET_NPY_FORMAT,
    #     TRAINING_DATASET_HDF5_FORMAT,
    #     TRAINING_DATASET_AVRO_FORMAT,
    #     TRAINING_DATASET_ORC_FORMAT,
    #     TRAINING_DATASET_IMAGE_FORMAT,
    #     TRAINING_DATASET_PETASTORM_FORMAT
    # ]
    # CLUSTERING_ANALYSIS_INPUT_COLUMN = "featurestore_feature_clustering_analysis_input_col"
    # CLUSTERING_ANALYSIS_OUTPUT_COLUMN = "featurestore_feature_clustering_analysis_output_col"
    # CLUSTERING_ANALYSIS_PCA_COLUMN = "featurestore_feature_clustering_analysis_pca_col"
    # CLUSTERING_ANALYSIS_FEATURES_COLUMN = "features"
    # CLUSTERING_ANALYSIS_CLUSTERS_OUTPUT_COLUMN = "clusters"
    # CLUSTERING_ANALYSIS_CLUSTERS_COLUMN = "featurestore_feature_clustering_analysis_pca_col"
    # CLUSTERING_ANALYSIS_ARRAY_COLUMN = "array"
    # CLUSTERING_ANALYSIS_SAMPLE_SIZE = 50
    # FEATURE_GROUP_INSERT_APPEND_MODE = "append"
    # FEATURE_GROUP_INSERT_OVERWRITE_MODE = "overwrite"
    # DESCRIPTIVE_STATS_SUMMARY_COL= "summary"
    # DESCRIPTIVE_STATS_METRIC_NAME_COL= "metricName"
    # DESCRIPTIVE_STATS_VALUE_COL= "value"
    # HISTOGRAM_FREQUENCY = "frequency"
    # HISTOGRAM_FEATURE = "feature"
    # FEATURESTORE_SUFFIX =  "_featurestore"
    # TRAINING_DATASETS_SUFFIX =  "_Training_Datasets"
    TRAINING_DATASET_TF_RECORD_SCHEMA_FILE_NAME = "tf_record_schema.txt"
    TF_RECORD_SCHEMA_FEATURE = "feature"
    TF_RECORD_SCHEMA_FEATURE_FIXED = "fixed_len"
    TF_RECORD_SCHEMA_FEATURE_VAR = "var_len"
    TF_RECORD_SCHEMA_TYPE = "type"
    TF_RECORD_SCHEMA_SHAPE = "shape"
    TF_RECORD_INT_TYPE = "int"
    TF_RECORD_FLOAT_TYPE = "float"
    TF_RECORD_STRING_TYPE = "string"
    TF_RECORD_INT_ARRAY_SPARK_TYPES = [
        SPARK_CONFIG.SPARK_ARRAY_INTEGER,
        SPARK_CONFIG.SPARK_ARRAY_BIGINT,
        SPARK_CONFIG.SPARK_ARRAY_INT,
        SPARK_CONFIG.SPARK_ARRAY_LONG,
    ]
    TF_RECORD_INT_SPARK_TYPES = [
        SPARK_CONFIG.SPARK_INTEGER_TYPE,
        SPARK_CONFIG.SPARK_BIGINT_TYPE,
        SPARK_CONFIG.SPARK_INT_TYPE,
        SPARK_CONFIG.SPARK_LONG_TYPE,
    ]
    TF_RECORD_STRING_SPARK_TYPES = [
        SPARK_CONFIG.SPARK_STRING_TYPE,
        SPARK_CONFIG.SPARK_BINARY_TYPE,
    ]
    TF_RECORD_STRING_ARRAY_SPARK_TYPES = [
        SPARK_CONFIG.SPARK_ARRAY_STRING,
        SPARK_CONFIG.SPARK_ARRAY_BINARY,
    ]
    TF_RECORD_FLOAT_SPARK_TYPES = [
        SPARK_CONFIG.SPARK_FLOAT_TYPE,
        SPARK_CONFIG.SPARK_DECIMAL_TYPE,
        SPARK_CONFIG.SPARK_DOUBLE_TYPE,
    ]
    TF_RECORD_FLOAT_ARRAY_SPARK_TYPES = [
        SPARK_CONFIG.SPARK_ARRAY_FLOAT,
        SPARK_CONFIG.SPARK_ARRAY_DECIMAL,
        SPARK_CONFIG.SPARK_ARRAY_DOUBLE,
        SPARK_CONFIG.SPARK_VECTOR,
    ]
    RECOGNIZED_TF_RECORD_TYPES = [
        SPARK_CONFIG.SPARK_VECTOR,
        SPARK_CONFIG.SPARK_ARRAY_BINARY,
        SPARK_CONFIG.SPARK_ARRAY_STRING,
        SPARK_CONFIG.SPARK_ARRAY_DECIMAL,
        SPARK_CONFIG.SPARK_ARRAY_DOUBLE,
        SPARK_CONFIG.SPARK_ARRAY_FLOAT,
        SPARK_CONFIG.SPARK_ARRAY_LONG,
        SPARK_CONFIG.SPARK_ARRAY_INTEGER,
        SPARK_CONFIG.SPARK_BINARY_TYPE,
        SPARK_CONFIG.SPARK_STRING_TYPE,
        SPARK_CONFIG.SPARK_DECIMAL_TYPE,
        SPARK_CONFIG.SPARK_DOUBLE_TYPE,
        SPARK_CONFIG.SPARK_FLOAT_TYPE,
        SPARK_CONFIG.SPARK_LONG_TYPE,
        SPARK_CONFIG.SPARK_INT_TYPE,
        SPARK_CONFIG.SPARK_INTEGER_TYPE,
        SPARK_CONFIG.SPARK_ARRAY_BIGINT,
        SPARK_CONFIG.SPARK_BIGINT_TYPE,
        SPARK_CONFIG.SPARK_ARRAY_INT,
    ]
    # DATAFRAME_TYPE_SPARK = "spark"
    # DATAFRAME_TYPE_NUMPY = "numpy"
    # DATAFRAME_TYPE_PYTHON = "python"
    # DATAFRAME_TYPE_PANDAS = "pandas"
    # JDBC_TRUSTSTORE_ARG = "sslTrustStore"
    # JDBC_TRUSTSTORE_PW_ARG = "trustStorePassword"
    # JDBC_KEYSTORE_ARG = "sslKeyStore"
    # JDBC_KEYSTORE_PW_ARG = "keyStorePassword"
    # IMPORT_HOPS_UTIL_FEATURESTORE_HELPER = "import io.hops.util.featurestore.FeaturestoreHelper"


class PETASTORM_CONFIG:
    """
    Petastorm String constants
    """

    FILESYSTEM_FACTORY = "pyarrow_filesystem"
    SCHEMA = "schema"
    LIBHDFS = "libhdfs"


# class DELIMITERS:
#     """
#     String delimiters constants
#     """
#     SLASH_DELIMITER = "/"
#     COMMA_DELIMITER = ","
#     TAB_DELIMITER = "\t"
#     COLON_DELIMITER = ":"
#     DOT_DELIMITER = "."
#     AMPERSAND_DELIMITER = "&"
#     SEMI_COLON_DELIMITER = ";"
#     JDBC_CONNECTION_STRING_VALUE_DELIMITER = "="
#     JDBC_CONNECTION_STRING_DELIMITER = ";"
#     QUESTION_MARK_DELIMITER = "?"
