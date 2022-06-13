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

package com.github.drnushooz.schema.flink.converter.registry;

import com.github.drnushooz.schema.flink.converter.config.ConverterConfiguration;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import javax.annotation.PostConstruct;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

@Component
@Lazy
@RequiredArgsConstructor
@Slf4j
public class ConfluentSchemaRegistryClient {
    @NonNull
    private final ConverterConfiguration convConf;

    private SchemaRegistryClient registryClient;
    private ConverterConfiguration.SchemaRegistry schemaRegistry;

    @PostConstruct
    public void init() {
        schemaRegistry = convConf.getSchemaRegistry();
        String registryURL = schemaRegistry.getUrl();
        int cacheSize = schemaRegistry.getCacheSize();
        registryClient = new CachedSchemaRegistryClient(registryURL, cacheSize);
        log.info("Initialized schema registry client to {} with cache size {}", registryURL,
            cacheSize);
    }

    public Schema getSchemaFromRegistry(String subject, Integer version)
        throws RestClientException, IOException {
        if (schemaRegistry.isEnabled()) {
            Schema schemaFromRegistry;
            if (version == null || version == 0) {
                log.info("Getting schema for subject: {} version: latest", subject);
                SchemaMetadata metadata = registryClient.getLatestSchemaMetadata(subject);
                schemaFromRegistry =
                    registryClient.getByVersion(subject, metadata.getVersion(), false);
            } else {
                log.info("Getting schema for subject: {} version: {}", subject, version);
                schemaFromRegistry = registryClient.getByVersion(subject, version, false);
            }
            return schemaFromRegistry;
        } else {
            return null;
        }
    }
}
