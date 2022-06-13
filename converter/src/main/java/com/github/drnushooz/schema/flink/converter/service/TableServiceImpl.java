/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.drnushooz.schema.flink.converter.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.drnushooz.schema.flink.converter.config.ConverterConfiguration;
import com.github.drnushooz.schema.flink.converter.config.ConverterConfiguration.Format.AvroConfluent;
import com.github.drnushooz.schema.flink.converter.core.CommonTableEnvironment;
import com.github.drnushooz.schema.flink.converter.core.TableDefinitionGenerator;
import com.github.drnushooz.schema.flink.converter.model.TableNameSchema;
import com.github.drnushooz.schema.flink.converter.registry.ConfluentSchemaRegistryClient;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.PostConstruct;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.springframework.stereotype.Service;

@RequiredArgsConstructor
@Service
public class TableServiceImpl implements TableService {
    @NonNull
    private final CommonTableEnvironment tableEnv;

    @NonNull
    private final ConverterConfiguration convConf;

    @NonNull
    private final ConfluentSchemaRegistryClient regClient;
    private AvroConfluent avroConfluent;

    @PostConstruct
    public void init() {
        avroConfluent = convConf.getFormat().getAvroConfluent();
    }

    @Override
    public TableNameSchema createFromAvro(String schema) {
        TableNameSchema tns = TableDefinitionGenerator.generateFromAvro(schema);
        String tableFormat = getTableFormat("fromavro", null);
        Schema.Builder tsb = Schema.newBuilder().fromSchema(tns.getSchema());
        if (tableFormat.equals(TableFormat.ENUM_ENABLED_AVRO_CONFLUENT.getName())
            && avroConfluent.isIncludeKeyField()) {
            List<String> keyFields = Arrays.asList(avroConfluent.getKeyFields());
            keyFields.forEach(kf -> tsb.column(kf, DataTypes.STRING()));
        }
        Map<String, String> options = getConfiguration(tns.getName(), tableFormat, schema);
        createTable(tns.getName(), tsb.build(), options);
        return tns;
    }

    @Override
    public TableNameSchema createFromJSON(String schema) throws JsonProcessingException {
        TableNameSchema tns = TableDefinitionGenerator.generateFromJSON(schema);
        String tableFormat = getTableFormat("fromjson", null);
        Map<String, String> options = getConfiguration(tns.getName(), tableFormat, null);
        createTable(tns.getName(), tns.getSchema(), options);
        return tns;
    }

    @Override
    public TableNameSchema createFromProtobuf(String schema) throws DescriptorValidationException {
        TableNameSchema tns = TableDefinitionGenerator.generateFromProtobuf(schema);
        String tableFormat = getTableFormat("fromprotobuf", null);
        Map<String, String> options = getConfiguration(tns.getName(), tableFormat, null);
        createTable(tns.getName(), tns.getSchema(), options);
        return tns;
    }

    @Override
    public TableNameSchema createFromRegistry(String subjectName, Integer version)
        throws RestClientException, IOException, DescriptorValidationException {
        io.confluent.kafka.schemaregistry.client.rest.entities.Schema
            schemaFromRegistry =
            Objects.requireNonNull(regClient.getSchemaFromRegistry(subjectName, version));
        TableNameSchema tns = TableDefinitionGenerator.generateFromRegistry(schemaFromRegistry);
        Schema.Builder tsb = Schema.newBuilder().fromSchema(tns.getSchema());
        String tableFormat = getTableFormat("fromregistry", schemaFromRegistry.getSchemaType());
        if (tableFormat.equals(TableFormat.ENUM_ENABLED_AVRO_CONFLUENT.getName())
            && avroConfluent.isIncludeKeyField()) {
            List<String> keyFields = Arrays.asList(avroConfluent.getKeyFields());
            keyFields.forEach(kf -> tsb.column(kf, DataTypes.STRING()));
        }
        Map<String, String> options =
            getConfiguration(tns.getName(), tableFormat, schemaFromRegistry.getSchema());
        createTable(tns.getName(), tsb.build(), options);
        return tns;
    }

    private void createTable(String tableName, Schema tableSchema,
        final Map<String, String> options) {
        ConverterConfiguration.Hive hive = convConf.getHive();
        String fullyQualifiedTableName =
            hive.getCatalogName() + "." + hive.getDatabaseName() + "." + tableName;
        TableDescriptor.Builder tDesc = TableDescriptor.forConnector("kafka").schema(tableSchema);
        options.forEach(tDesc::option);
        TableEnvironment tEnv = tableEnv.get();
        tEnv.createTable(fullyQualifiedTableName, tDesc.build());
    }

    private Map<String, String> getConfiguration(final String tableName,
        final String tableFormat, final String schema) {
        ConverterConfiguration.Kafka kafka = convConf.getKafka();
        Map<String, String> options = new HashMap<>();
        options.put("topic", tableName);
        options.put("value.format", tableFormat);
        options.put("scan.startup.mode", kafka.getScanStartupMode());
        options.put("properties.bootstrap.servers", kafka.getBootstrapServers());
        options.put("properties.group.id", tableName);
        options.put("properties.auto.offset.reset", kafka.getAutoOffsetReset());

        // Format specific options
        ConverterConfiguration.Format format = convConf.getFormat();
        if (tableFormat.equals(TableFormat.ENUM_ENABLED_AVRO_CONFLUENT.getName())) {
            String valueFormatPrefix = "value." + tableFormat;
            ConverterConfiguration.SchemaRegistry registry = convConf.getSchemaRegistry();
            ConverterConfiguration.Format.AvroConfluent avroConfluent = format.getAvroConfluent();
            Optional.ofNullable(schema)
                .ifPresent(s -> options.put(valueFormatPrefix + ".schema", s));
            if (registry.isEnabled()) {
                options.put(valueFormatPrefix + ".url", registry.getUrl());
                options.put(
                    "properties.value.converter.enhanced." + tableFormat + ".schema.support",
                    String.valueOf(registry.isAdvancedSchemaSupportEnabled()));
                Optional.of(avroConfluent.getSubject())
                    .ifPresent(sub -> {
                        if (!sub.isEmpty()) {
                            options.put(valueFormatPrefix + ".subject", sub);
                        }
                    });
            }

            if (avroConfluent.isIncludeKeyField()) {
                options.put("key.format", avroConfluent.getKeyFormat());
                options.put("key.fields", String.join(",", avroConfluent.getKeyFields()));
                options.put("value.fields-include", avroConfluent.getValueFieldsInclude());
            }

            // SSL options
            ConverterConfiguration.Kafka.SSL ssl = kafka.getSsl();
            if (ssl.isEnabled()) {
                options.put("properties.security.protocol", "SSL");
                Optional.of(ssl.getKeystore().getLocation())
                    .ifPresent(loc -> {
                        if (!loc.isEmpty()) {
                            options.put(valueFormatPrefix + ".ssl.keystore.location", loc);
                        }
                    });
                Optional.of(ssl.getKeystore().getPassword())
                    .ifPresent(pw -> {
                        if (!pw.isEmpty()) {
                            options.put(valueFormatPrefix + ".ssl.keystore.password", pw);
                        }
                    });
                Optional.of(ssl.getTruststore().getLocation())
                    .ifPresent(loc -> {
                        if (!loc.isEmpty()) {
                            options.put(valueFormatPrefix + ".ssl.truststore.location", loc);
                        }
                    });
                Optional.of(ssl.getTruststore().getPassword())
                    .ifPresent(pw -> {
                        if (!pw.isEmpty()) {
                            options.put(valueFormatPrefix + ".ssl.truststore.password", pw);
                        }
                    });
                Optional.of(ssl.getKey().getPassword())
                    .ifPresent(pw -> {
                        if (!pw.isEmpty()) {
                            options.put(valueFormatPrefix + "properties.ssl.key.password", pw);
                        }
                    });
            }
        } else if (tableFormat.equals(TableFormat.JSON.getName())) {
            ConverterConfiguration.Format.JSON json = format.getJson();
            options.put(tableFormat + ".fail-on-missing-field",
                String.valueOf(json.isFailOnMissingField()));
            options.put(tableFormat + ".ignore-parse-errors",
                String.valueOf(json.isIgnoreParseErrors()));
            options.put(tableFormat + ".timestamp-format.standard",
                json.getTimestampFormat().getStandard());
            options.put(tableFormat + ".map-null-key.mode", json.getMapNullKey().getMode());
            options.put(tableFormat + ".map-null-key.literal", json.getMapNullKey().getLiteral());
            options.put(tableFormat + ".encode.decimal-as-plain-number",
                String.valueOf(json.getEncode().isDecimalAsPlainNumber()));
        }

        return Collections.unmodifiableMap(options);
    }

    private String getTableFormat(String requestEntity, String schemaTypeFromRegistry) {
        String tableFormat;
        switch (requestEntity) {
            case "fromavro":
                tableFormat = TableFormat.AVRO.getName();
                break;

            case "fromjson":
                tableFormat = TableFormat.JSON.getName();
                break;

            case "fromprotobuf":
                tableFormat = TableFormat.PROTOBUF.getName();
                break;

            case "fromregistry":
                tableFormat = Optional.ofNullable(schemaTypeFromRegistry).map(schemaType -> {
                        switch (schemaType) {
                            case "AVRO":
                                return TableFormat.ENUM_ENABLED_AVRO_CONFLUENT.getName();

                            case "JSON":
                                return TableFormat.JSON.getName();

                            case "PROTOBUF":
                                return TableFormat.PROTOBUF.getName();

                            default:
                                throw new IllegalArgumentException(
                                    "Found invalid schema type: " + schemaType);
                        }
                    }
                ).orElseThrow(() -> new IllegalArgumentException(
                    "Unknown format received in path " + requestEntity));
                break;

            default:
                throw new IllegalArgumentException(
                    "Unknown format received in path " + requestEntity);
        }

        return tableFormat;
    }
}
