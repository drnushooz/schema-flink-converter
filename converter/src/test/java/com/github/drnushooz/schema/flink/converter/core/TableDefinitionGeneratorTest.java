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
import com.github.drnushooz.schema.flink.converter.model.TableNameSchema;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import org.apache.flink.table.api.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TableDefinitionGeneratorTest {
    @Test
    void testGenerateFromAvro() {
        String avroSchema = "{\n"
            + "\t\t\"type\": \"record\",\n"
            + "\t\t\"name\": \"snack\",\n"
            + "\t\t\"fields\": [\n"
            + "\t\t\t{\"name\": \"name\", \"type\": \"string\"},\n"
            + "\t\t\t{\"name\": \"manufacturer\", \"type\": \"string\"},\n"
            + "\t\t\t{\"name\": \"calories\", \"type\": \"float\"},\n"
            + "\t\t\t{\"name\": \"color\", \"type\": [\"null\", \"string\"], \"default\": null}\n"
            + "\t\t]\n"
            + "\t}";
        TableNameSchema result = TableDefinitionGenerator.generateFromAvro(avroSchema);
        Schema tableSchema = result.getSchema();
        assertNotNull(tableSchema);
        assertEquals(result.getName(), "snack");
    }

    @Test
    void testGenerateFromJSON() throws JsonProcessingException {
        String jsonSchema = "{\n"
            + "  \"title\": \"Person\",\n"
            + "  \"type\": \"object\",\n"
            + "  \"properties\": {\n"
            + "    \"firstName\": {\n"
            + "      \"type\": \"string\",\n"
            + "      \"description\": \"The person's first name.\"\n"
            + "    },\n"
            + "    \"lastName\": {\n"
            + "      \"type\": \"string\",\n"
            + "      \"description\": \"The person's last name.\"\n"
            + "    },\n"
            + "    \"age\": {\n"
            + "      \"description\": \"Age in years which must be equal to or greater than zero.\",\n"
            + "      \"type\": \"integer\",\n"
            + "      \"minimum\": 0\n"
            + "    }\n"
            + "  }\n"
            + "}";
        TableNameSchema result = TableDefinitionGenerator.generateFromJSON(jsonSchema);
        Schema tableSchema = result.getSchema();
        assertNotNull(tableSchema);
        assertEquals(result.getName(), "Person");
    }

    @Test
    void testGenerateFromProtobuf() throws DescriptorValidationException {
        String protobufSchema = "message Subscriber {\n"
            + "\t\trequired string first_name = 1;\n"
            + "\t\trequired string last_name = 2;\n"
            + "\t\trequired string address = 3;\n"
            + "\t\toptional string city = 4;\n"
            + "\t\toptional int32 zipcode = 5;\n"
            + "\t\trepeated int32 department_codes = 6;\n"
            + "\t}";
        TableNameSchema result = TableDefinitionGenerator.generateFromProtobuf(protobufSchema);
        Schema tableSchema = result.getSchema();
        assertNotNull(tableSchema);
        assertEquals(result.getName(), "Subscriber");
    }
}
