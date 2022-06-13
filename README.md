# Schema to Flink table definition converter and Avro format

This project contains two modules as below

* **converter** - The schema conversion RESTful service which can convert Avro, Protobuf and JSON 
  schemas into equivalent Flink table definitions. There is built in support for Confluent Schema 
  registry.
* **enum-enabled-avro-confluent** - A table format which is able to handle Avro `enum` fields
  correctly. There is a known issue [FLINK-24544](https://issues.apache.org/jira/browse/FLINK-24544)
  where the `avro-confluent` table format throws `AvroTypeException` while trying to parse an enum
  field. This module provides an alternative format `enum-enabled-avro-confluent` which
  allows schema specification at table definition time circumventing this issue.
  Schema can be specified with `value.enum-enabled-avro-confluent.schema` option.

### How to build

```shell
mvn clean package
```

### APIs

The service exposes the following 4 APIs which

| Method | Path                                      | Parameters                                               |
|--------|-------------------------------------------|----------------------------------------------------------|
| `POST` | `/fromavro`                               | Avro schema in the body                                  |
| `POST` | `/fromjson`                               | JSON schema in the body                                  |
| `POST` | `/fromprotobuf`                           | Protobuf schema in the body                              |
| `PUT`  | `/fromregistry/<subjectname>/[<version>]` | Create table based on specific schema from the Registry. |

### Configuration properties

#### Common
| Property                            | Purpose                                                                           | Default       |
|-------------------------------------|-----------------------------------------------------------------------------------|---------------|
| `app.hadoop.config-dir`             | Hadoop configuration directory                                                    | (None)        |
| `app.hive.config-dir`               | Hive configuration directory                                                      | (None)        |
| `app.hive.catalog-name`             | Hive catalog to use for table creation                                            | hivecatalog   |
| `app.hive.database-name`            | Hive database to use for table creation                                           | default       |
| `app.kafka.auto-offset-reset`       | How to reset Kafka offset                                                         | latest        |
| `app.kafka.bootstrap-servers`       | Kafka bootstrap servers                                                           | (None)        |
| `app.kafka.scan-startup-mode`       | Kafka scan startup mode                                                           | group-offsets |
| `app.kafka.ssl.enabled`             | Enable SSL                                                                        | false         |
| `app.kafka.ssl.keystore.location`   | Kafka SSL keystore location                                                       | (None)        |
| `app.kafka.ssl.keystore.password`   | Kafka SSL keystore password                                                       | (None)        |
| `app.kafka.ssl.truststore.location` | Kafka SSL truststore location                                                     | (None)        |
| `app.kafka.ssl.truststore.password` | Kafka SSL truststore password                                                     | (None)        |
| `app.kafka.ssl.key.password `       | Kafka SSL key password                                                            | (None)        |
| `app.schema-registry.url`           | Confluent schema registry location. When set, enables schema registry integration | (None)        |
| `app.schema-registry.cache-size`    | Client side cache size for schema registry.                                       | 20            |

#### Avro Confluent
| Property                                                    | Purpose                                          | Default            |
|-------------------------------------------------------------|--------------------------------------------------|--------------------|
| `app.format.avro-confluent.advanced-schema-support-enabled` | Enable the advanced schema support in the serde. | true               |
| `app.format.avro-confluent.include-key-field`               | Include a key field when creating a table.       | true               |
| `app.format.avro-confluent.key-format`                      | Format of the key.                               | raw                |
| `app.format.avro-confluent.key-fields`                      | Key fields to include.                           | kafka_key          |
| `app.format.avro-confluent.value-fields-include `           | Which fields to include in values.               | EXCEPT_KEY         |
| `app.format.avro-confluent.basic-auth.credentials-source`   | Basic authentication credential source.          | (None)             |
| `app.format.avro-confluent.basic-auth.user-info`            | Basic authentication user information.           | (None)             |
| `app.format.avro-confluent.bearer-auth.credentials-source`  | Bearer authentication credential source.         | (None)             |
| `app.format.avro-confluent.bearer-auth.token`               | Bearer authentication token.                     | (None)             |
| `app.format.avro-confluent.subject`                         | Subject name to use for the schema.              | <topic name>-value |

#### JSON
| Property                                         | Purpose                                          | Default |
|--------------------------------------------------|--------------------------------------------------|---------|
| `app.format.json.fail-on-missing-field`          | Fail if a mandatory field is missing.            | false   |
| `app.format.json.ignore-parse-errors`            | Ignore parsing errors.                           | false   |
| `app.format.json.timestamp-format.standard`      | Format of the timestamp field when deserializing | SQL     |
| `app.format.json.map-null-key.mode`              | How to map a null key                            | FAIL    |
| `app.format.json.map-null-key.literal`           | Which literal to use for a null key              | null    |
| `app.format.json.encode.decimal-as-plain-number` | Should decimals be represented as plain numbers  | false   |
