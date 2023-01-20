# Kafka Connect JDBC Connector

*This project is a fork of the official confluent `kafka-connect-jdbc` project that extends the latter by the Filemaker JDBC dialect (v16 - v19)*

kafka-connect-jdbc is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect)
for loading data to and from any JDBC-compatible database.

Documentation for this connector can be found [here](http://docs.confluent.io/current/connect/connect-jdbc/docs/index.html).

# Development

To build a development version you'll need a recent version of Kafka as well as a set of upstream Confluent projects, which you'll have to build from their appropriate snapshot branch. See the [FAQ](https://github.com/confluentinc/kafka-connect-jdbc/wiki/FAQ)
for guidance on this process.

You can build kafka-connect-jdbc with Maven using the standard lifecycle phases.

# FilemakerDialect

## Running the FilemakerDialect integration tests

There is no way to use FileMaker Server for testing without purchasing a license or applying for a 
test license at the customer service. So running integration tests which require a these FileMaker Server 
can not be fully automated.

At current it is required that you have a Filemaker Server to which the tests can connect. 
This server needs to offer two databases which can be used for the tests.   

Please copy the `src/test/resources/FilemakerJdbcConnect.properties.template` to `src/test/resources/FilemakerJdbcConnect.properties` 
and configure it accordingly.

In future this will be simplified by making use of a docker image. Running the Filemaker related integration tests this ways requires the docker image `filemakerServer19:latest` being installed locally.

Building the FileMaker Server 19 Docker image:

~~~
./scripts/make-fm19-docker-image.sh
~~~

**TODO**: 

* document how to supply the `LicenseCert.fmcert`
* create two FM test databases in `src/test/resources/filemaker19_dbs/`
* un-comment `public static final FixedHostPortGenericContainer fmServer = .... ` in `FilemakerDialectIT_FM`
* adapt the test class to use the server running in the docker image

# FAQ

Refer frequently asked questions on Kafka Connect JDBC here -
https://github.com/confluentinc/kafka-connect-jdbc/wiki/FAQ

# Contribute

Contributions can only be accepted if they contain appropriate testing. For example, adding a new dialect of JDBC will require an integration test.

- Source Code: https://github.com/confluentinc/kafka-connect-jdbc
- Issue Tracker: https://github.com/confluentinc/kafka-connect-jdbc/issues
- Learn how to work with the connector's source code by reading our [Development and Contribution guidelines](CONTRIBUTING.md).

# Information

For more information, check the documentation for the JDBC connector on the [confluent.io](https://docs.confluent.io/current/connect/kafka-connect-jdbc/index.html) website. Questions related to the connector can be asked on [Community Slack](https://launchpass.com/confluentcommunity) or the [Confluent Platform Google Group](https://groups.google.com/forum/#!topic/confluent-platform/).

# License

This project is licensed under the [Confluent Community License](LICENSE).

