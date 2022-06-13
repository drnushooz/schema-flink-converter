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

package com.github.drnushooz.schema.flink.converter.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.drnushooz.schema.flink.converter.model.TableNameSchema;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import io.apicurio.registry.utils.protobuf.schema.FileDescriptorUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter;

@Slf4j
public class TableDefinitionGenerator {
    static final ObjectMapper objectMapper =
        new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /**
     * Generate Flink SQL table definition based on Avro schema.
     */
    public static TableNameSchema generateFromAvro(String schema) {
        org.apache.avro.Schema.Parser avroSchemaParser = new org.apache.avro.Schema.Parser();
        org.apache.avro.Schema avroSchema = avroSchemaParser.parse(schema);
        if (avroSchema.getType() != org.apache.avro.Schema.Type.RECORD) {
            throw new IllegalArgumentException("Outermost type must be record!");
        }
        DataType flinkDataType = AvroSchemaConverter.convertToDataType(schema);
        Schema.Builder tSchemaBuilder = Schema.newBuilder().fromRowDataType(flinkDataType);
        return new TableNameSchema(avroSchema.getName(), tSchemaBuilder.build());
    }

    /**
     * Generate Flink SQL table definition based on JSON schema.
     */
    @SuppressWarnings("deprecation")
    public static TableNameSchema generateFromJSON(String schema)
        throws JsonProcessingException {
        JsonNode jsonSchema = objectMapper.readTree(schema);
        String title = jsonSchema.get("title").asText();
        DataType flinkDataType =
            LegacyTypeInfoDataTypeConverter.toDataType(JsonRowSchemaConverter.convert(schema));
        Schema tSchema = Schema.newBuilder().fromRowDataType(flinkDataType).build();
        return new TableNameSchema(title, tSchema);
    }

    /**
     * Generate Flink SQL table definition based on Protobuf schema.
     */
    public static TableNameSchema generateFromProtobuf(String schema)
        throws DescriptorValidationException {
        ProtoFileElement fileElement =
            ProtoParser.Companion.parse(FileDescriptorUtils.DEFAULT_LOCATION, schema);
        FileDescriptor fileDescriptor = FileDescriptorUtils.protoFileToFileDescriptor(fileElement);
        int messageDescriptorCount = fileDescriptor.getMessageTypes().size();
        if (messageDescriptorCount > 1) {
            IllegalArgumentException iae = new IllegalArgumentException(
                "There should be only one outermost message type, found " + messageDescriptorCount);
            log.error("Error in parsing protobuf schema", iae);
            throw iae;
        }

        Descriptor outermostMessageType = fileDescriptor.getMessageTypes().get(0);
        DataType flinkDataType = ProtobufUtils.protobufToFlinkDataType(outermostMessageType);
        Schema tSchema = Schema.newBuilder().fromRowDataType(flinkDataType).build();
        return new TableNameSchema(outermostMessageType.getName(), tSchema);
    }

    public static TableNameSchema generateFromRegistry(
        io.confluent.kafka.schemaregistry.client.rest.entities.Schema schemaFromRegistry)
        throws DescriptorValidationException, JsonProcessingException {
        String schemaType = schemaFromRegistry.getSchemaType();
        switch (schemaType) {
            case "AVRO":
                return generateFromAvro(schemaFromRegistry.getSchema());

            case "JSON":
                return generateFromJSON(schemaFromRegistry.getSchema());

            case "PROTOBUF":
                return generateFromProtobuf(schemaFromRegistry.getSchema());

            default:
                throw new IllegalArgumentException("Found invalid schema type: " + schemaType);
        }
    }
}
