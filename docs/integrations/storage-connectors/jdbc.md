Most databases can be connected to using our generic JDBC Storage Connector (such as MySQL, Postgres, Oracle, DB2, and MongoDB). Typically, you will need to add username and password in your JDBC URL or as key/value parameters. Consult the documentation of your target database to determine the correct JDBC URL and parameters.

<p align="center">
  <figure>
    <img src="../../../assets/images/storage-connectors/jdbc-setup.png" alt="Define a JDBC storage connector using a JDBC connection string and parameters">
    <figcaption>You can define a storage connector to a JDBC enabled source using a JDBC connection string and optional parameters supplied at runtime.</figcaption>
  </figure>
</p>

