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

import com.github.drnushooz.schema.flink.converter.config.ConverterConfiguration;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Collections;

/**
 * A factory class to create instance of TableEnvironment
 */
@Lazy
@RequiredArgsConstructor
@Component
public class CommonTableEnvironment {
    @NonNull
    private final ConverterConfiguration convConf;

    private TableEnvironment tableEnv;
    private Catalog hiveCatalog;

    @PostConstruct
    public void init() {
        ConverterConfiguration.Hive hive = convConf.getHive();
        String hiveDatabaseName = hive.getDatabaseName();
        String hiveCatalogName = hive.getCatalogName();
        hiveCatalog =
            new HiveCatalog(hiveCatalogName, hiveDatabaseName, hive.getConfigDir(),
                convConf.getHadoop().getConfigDir(),
                null);
        tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tableEnv.registerCatalog(hiveCatalogName, hiveCatalog);
        try {
            hiveCatalog.createDatabase(hiveDatabaseName,
                new CatalogDatabaseImpl(Collections.emptyMap(), null), true);
        } catch (DatabaseAlreadyExistException e) {
            //no-op
        }
    }

    public TableEnvironment get() {
        return tableEnv;
    }

    @PreDestroy
    public void destroy() {
        hiveCatalog.close();
    }
}
