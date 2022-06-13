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

package com.github.drnushooz.schema.flink.converter.health;

import com.github.drnushooz.schema.flink.converter.core.CommonScheduler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Component
public class ServiceHealthIndicator implements ReactiveHealthIndicator {
    private final WebClient webClient;

    @Autowired
    public ServiceHealthIndicator(final WebClient.Builder webClient) {
        this.webClient = webClient.build();
    }

    @Override
    public Mono<Health> health() {
        return webClient.get()
            .uri("http://localhost:8080/actuator/info").retrieve().bodyToMono(String.class)
            .subscribeOn(CommonScheduler.getInstance())
            .map(s -> new Health.Builder().up().build())
            .onErrorResume(ex -> Mono.just(new Health.Builder().down(ex).build()));
    }
}
