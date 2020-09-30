## The connection module

!!! example "Connection on Hopsworks"
    Example of using `hsfs.connection()` on Hopsworks:

    ```python
    import hsfs
    conn = hsfs.connection()
    fs = conn.get_feature_store("production_fs")
    ```

{{hsfs.connection.Connection.connection}}

{{hsfs.connection.Connection.setup_databricks}}
