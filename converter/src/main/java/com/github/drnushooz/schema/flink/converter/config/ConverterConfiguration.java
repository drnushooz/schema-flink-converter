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

package com.github.drnushooz.schema.flink.converter.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;

@ConfigurationProperties("app")
@Component
@Validated
@Getter
@Setter
public class ConverterConfiguration {
    private final Format format = new Format();
    private final Hadoop hadoop = new Hadoop();
    private final Hive hive = new Hive();
    private final Kafka kafka = new Kafka();
    private final SchemaRegistry schemaRegistry = new SchemaRegistry();

    // Format classes
    @Getter
    @Setter
    public static class Format {
        private final AvroConfluent avroConfluent = new AvroConfluent();
        private final JSON json = new JSON();

        @Getter
        @Setter
        public static class AvroConfluent {
            private final BasicAuth basicAuth = new BasicAuth();
            private final BearerAuth bearerAuth = new BearerAuth();

            @NotBlank
            private String keyFormat;

            @NotEmpty
            private String[] keyFields;

            @NotBlank
            private String valueFieldsInclude;
            private boolean includeKeyField;
            private boolean advancedSchemaSupportEnabled;
            private String subject;

            @Getter
            @Setter
            public static class BasicAuth {
                private String credentialsSource;
                private String userInfo;
            }

            @Getter
            @Setter
            public static class BearerAuth {
                private String credentialsSource;
                private String token;
            }
        }

        @Getter
        @Setter
        public static class JSON {
            private final TimestampFormat timestampFormat = new TimestampFormat();
            private final MapNullKey mapNullKey = new MapNullKey();
            private final Encode encode = new Encode();

            private boolean failOnMissingField;
            private boolean ignoreParseErrors;

            @Getter
            @Setter
            public static class TimestampFormat {
                @NotBlank
                private String standard;
            }

            @Getter
            @Setter
            public static class MapNullKey {
                @NotBlank
                private String mode;

                @NotBlank
                private String literal;
            }

            @Getter
            @Setter
            public static class Encode {
                private boolean decimalAsPlainNumber;
            }
        }
    }

    @Getter
    @Setter
    public static class Hadoop {
        @NotBlank
        private String configDir;
    }

    @Getter
    @Setter
    public static class Hive {
        @NotBlank
        private String configDir;
        private String catalogName;
        private String databaseName;
    }

    @Getter
    @Setter
    public static class Kafka {
        private final SSL ssl = new SSL();

        @NotBlank
        private String autoOffsetReset;

        @NotBlank
        private String bootstrapServers;

        @NotBlank
        private String scanStartupMode;

        @Getter
        @Setter
        public static class SSL {
            private final Key key = new Key();
            private final Keystore keystore = new Keystore();
            private final Truststore truststore = new Truststore();

            private boolean enabled;

            @Getter
            @Setter
            public static class Key {
                private String password;
            }

            @Getter
            @Setter
            public static class Keystore {
                private String location;
                private String password;
            }

            @Getter
            @Setter
            public static class Truststore {
                private String location;
                private String password;
            }
        }
    }

    @Getter
    @Setter
    public static class SchemaRegistry {
        private boolean advancedSchemaSupportEnabled;
        private String url;

        @Min(1)
        private int cacheSize;

        public boolean isEnabled() {
            return !url.isEmpty();
        }
    }
}
