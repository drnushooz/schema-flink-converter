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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import java.util.List;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.types.DataType;

public class ProtobufUtils {
    static DataType protobufToFlinkDataType(Descriptor descriptor) {
        List<FieldDescriptor> fieldDescriptors = descriptor.getFields();
        final DataTypes.Field[] flinkFields = new Field[fieldDescriptors.size()];
        for (int i = 0; i < flinkFields.length; i++) {
            FieldDescriptor curField = fieldDescriptors.get(i);
            flinkFields[i] = DataTypes.FIELD(curField.getName(), protobufToFlinkDataType(curField));
        }
        return DataTypes.ROW(flinkFields).notNull();
    }

    static DataType protobufToFlinkDataType(FieldDescriptor fieldDescriptor) {
        DataType dataType;
        switch (fieldDescriptor.getType()) {
            case BOOL:
                dataType = DataTypes.BOOLEAN();
                break;

            case INT32:
            case SINT32:
            case UINT32:
            case FIXED32:
            case SFIXED32:
                dataType = DataTypes.INT();
                break;

            case INT64:
            case SINT64:
            case FIXED64:
            case SFIXED64:
                dataType = DataTypes.BIGINT();
                break;

            case FLOAT:
                dataType = DataTypes.FLOAT();
                break;

            case DOUBLE:
                dataType = DataTypes.DOUBLE();
                break;

            case STRING:
            case ENUM:
                dataType = DataTypes.STRING();
                break;

            case BYTES:
                dataType = DataTypes.BYTES();
                break;

            case MESSAGE:
                List<FieldDescriptor> nestedFieldDescriptors =
                    fieldDescriptor.getMessageType().getFields();
                final DataTypes.Field[] flinkFields = new Field[nestedFieldDescriptors.size()];
                for (int i = 0; i < flinkFields.length; i++) {
                    FieldDescriptor curField = nestedFieldDescriptors.get(i);
                    flinkFields[i] =
                        DataTypes.FIELD(curField.getName(), protobufToFlinkDataType(curField));
                }
                dataType = DataTypes.ROW(flinkFields);
                break;

            case GROUP: //Groups are deprecated
            default:
                throw new IllegalArgumentException(
                    "Unsupported Protobuf data type '" + fieldDescriptor.getType() + "'.");
        }

        if (fieldDescriptor.isRepeated()) {
            dataType = DataTypes.ARRAY(dataType);
        }

        if (!fieldDescriptor.isOptional()) {
            dataType = dataType.notNull();
        }

        return dataType;
    }
}
