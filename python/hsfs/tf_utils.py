try:
    import tensorflow as tf
except ModuleNotFoundError:
    pass


def create_tfrecord_feature_description(df_schema, fixed=True):

    """
    :param df_schema: dataframe schema to infer the tensorflow example record from
    :param fixed: if feature is fixed or not
    :return: tfrecord feature description
    """

    # supported type mappings between DataFrame.dtypes and tf.train.Feature types
    float_dtypes = ["float", "double"]
    int64_dtypes = ["integer", "long"]
    bytes_dtypes = ["binary", "string"]

    bytes_list_dtypes = ["array<binary>", "array<string>"]
    float_list_dtypes = ["array<float>", "array<double>"]
    int64_list_dtypes = ["array<integer>", "array<long>", "array<float>"]

    feature_description = {}

    for col in df_schema:
        if col.type in float_dtypes or col.type in float_list_dtypes:
            feature_description[col.name] = tf.io.FixedLenFeature([], tf.float32)
        elif col.type in int64_dtypes or col.type in int64_list_dtypes:
            feature_description[col.name] = tf.io.FixedLenFeature([], tf.int64)
        elif col.type in bytes_dtypes or col.type in bytes_list_dtypes:
            feature_description[col.name] = tf.io.FixedLenFeature([], tf.string)
        else:
            raise InferTFRecordSchemaError(
                "Could not recognize type {} for inferring the tf-records schema.".format(
                    col.type
                )
            )
    return feature_description


class InferTFRecordSchemaError(Exception):
    """This exception will be raised if there is an error in inferring the tfrecord schema of a dataframe"""
