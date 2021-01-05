# Feature

*Features* are the most granular entity in the [feature store](feature_store.md) and are
logically grouped by [feature groups](feature_group.md).

The storage location of a single feature is determined by the
[feature group](feature_group.md). Hence, enabling a [feature group](feature_group.md)
for online storage will make a feature available as an online feature.

New features can be appended to [feature groups](feature_group.md), however, to drop
features, a new [feature group](feature_group.md) version has to be created. When
appending features it is possible to specify a default value which is used for existing
feature vectors in the [feature group](feature_group.md) for the new feature.

{{feature}}

## Feature Types

Each features requires at least an offline type to be specified for the creation of the
meta data of the [feature group](feature_group.md) in the offline storage, even if the
[feature group](feature_group.md) is going to be a purely online
[feature group](feature_group.md) with no data in the offline storage.

### Offline Storage

The offline storage is based on Apache Hive and hence, any
[Hive Data Type](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types)
can be leveraged.

!!! note "Type Inference"
    When creating a [feature group](feature_group.md) from a Spark DataFrame, without
    providing a schema manually, the feature store will infer the schema with feature
    types from the DataFrame.

Potential *offline* types are:

```SQL
"None","TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE",
"DECIMAL", "TIMESTAMP", "DATE", "STRING",
"BOOLEAN", "BINARY",
"ARRAY <TINYINT>", "ARRAY <SMALLINT>", "ARRAY <INT>", "ARRAY <BIGINT>",
"ARRAY <FLOAT>", "ARRAY <DOUBLE>", "ARRAY <DECIMAL>", "ARRAY <TIMESTAMP>",
"ARRAY <DATE>", "ARRAY <STRING>",
"ARRAY <BOOLEAN>", "ARRAY <BINARY>", "ARRAY <ARRAY <FLOAT> >",
"ARRAY <ARRAY <INT> >", "ARRAY <ARRAY <STRING> >",
"MAP <FLOAT, FLOAT>", "MAP <FLOAT, STRING>", "MAP <FLOAT, INT>",
"MAP <FLOAT, BINARY>", "MAP <INT, INT>", "MAP <INT, STRING>",
"MAP <INT, BINARY>", "MAP <INT, FLOAT>", "MAP <INT, ARRAY <FLOAT> >",
"STRUCT < label: STRING, index: INT >", "UNIONTYPE < STRING, INT>"
```

### Online Storage

The online storage is based on MySQL Cluster (NDB) and hence, any
[MySQL Data Type](https://dev.mysql.com/doc/refman/8.0/en/data-types.html)
can be leveraged.

!!! note "Type Inference"
    When creating a [feature group](feature_group.md) from a Spark DataFrame, without
    providing a schema manually, the feature store will infer the schema with feature
    types from the DataFrame.

Potential *online* types are:

```SQL
"None", "INT(11)", "TINYINT(1)", "SMALLINT(5)", "MEDIUMINT(7)", "BIGINT(20)",
"FLOAT", "DOUBLE", "DECIMAL", "DATE", "DATETIME", "TIMESTAMP", "TIME", "YEAR",
"CHAR", "VARCHAR(25)", "VARCHAR(125)", "VARCHAR(225)", "VARCHAR(500)",
"VARCHAR(1000)", "VARCHAR(2000)", "VARCHAR(5000)", "VARCHAR(10000)", "BINARY",
"VARBINARY(100)", "VARBINARY(500)", "VARBINARY(1000)", "BLOB", "TEXT",
"TINYBLOB", "TINYTEXT", "MEDIUMBLOB", "MEDIUMTEXT", "LONGBLOB", "LONGTEXT",
"JSON"
```

## Properties

{{feature_properties}}

## Methods

{{feature_methods}}
