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

package com.github.drnushooz.schema.flink.converter.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.drnushooz.schema.flink.converter.core.CommonScheduler;
import com.github.drnushooz.schema.flink.converter.model.TableNameSchema;
import com.github.drnushooz.schema.flink.converter.service.TableService;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.function.TupleUtils;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@RequiredArgsConstructor
@RestController
@Slf4j
public class TableCreationController {
    private final TableService tableService;

    @PostMapping("/fromavro")
    public Mono<ResponseEntity<ResponseBody>> fromAvro(@RequestBody String requestSchema) {
        Mono<String> requestSchemaMono = Mono.just(requestSchema);
        Mono<TableNameSchema> tnsMono =
            requestSchemaMono.subscribeOn(CommonScheduler.getInstance())
                .map(tableService::createFromAvro);
        return tnsMono.map(tns -> {
            ResponseBody rb = new ResponseBody(RequestMethod.POST.name(), "/fromavro",
                tns.getSchema().toString());
            return ResponseEntity.ok(rb);
        }).onErrorResume(ex -> {
            log.error(
                "Error in processing request " + RequestMethod.POST.name() + " /fromavro" + " "
                    + requestSchema, ex);
            ResponseBody rb = new ResponseBody(RequestMethod.POST.name(), "/fromavro",
                "Error in processing request: " + ex.getLocalizedMessage());
            return Mono.just(ResponseEntity.badRequest().body(rb));
        });
    }

    @PostMapping("/fromjson")
    public Mono<ResponseEntity<ResponseBody>> fromJSON(@RequestBody String requestSchema) {
        Mono<String> requestSchemaMono = Mono.just(requestSchema);
        Mono<TableNameSchema> tnsMono =
            requestSchemaMono.subscribeOn(CommonScheduler.getInstance()).flatMap(rs -> {
                try {
                    return Mono.just(tableService.createFromJSON(rs));
                } catch (JsonProcessingException e) {
                    return Mono.error(e);
                }
            });
        return tnsMono.map(tns -> {
            ResponseBody rb = new ResponseBody(RequestMethod.POST.name(), "/fromjson",
                tns.getSchema().toString());
            return ResponseEntity.ok(rb);
        }).onErrorResume(ex -> {
            log.error(
                "Error in processing request " + RequestMethod.POST.name() + " /fromjson" + " "
                    + requestSchema, ex);
            ResponseBody rb = new ResponseBody(RequestMethod.POST.name(), "/fromjson",
                "Error in processing request: " + ex.getLocalizedMessage());
            return Mono.just(ResponseEntity.badRequest().body(rb));
        });
    }

    @PutMapping(value = {"/fromregistry/{subjectName}", "/fromregistry/{subjectName}/{version}"})
    public Mono<ResponseEntity<ResponseBody>> fromRegistry(@PathVariable String subjectName,
        @PathVariable(required = false) Integer version) {
        int resolvedVersion = Optional.ofNullable(version).orElse(0);
        Mono<Tuple2<String, Integer>> requestSchemaMono =
            Mono.just(Tuples.of(subjectName, resolvedVersion));
        Mono<TableNameSchema> tnsMono =
            requestSchemaMono.subscribeOn(CommonScheduler.getInstance())
                .flatMap(TupleUtils.function((sn, v) -> {
                    try {
                        return Mono.just(tableService.createFromRegistry(sn, v));
                    } catch (Exception e) {
                        return Mono.error(e);
                    }
                }));
        return tnsMono.map(tns -> {
            String requestPath =
                "/fromregistry/" + subjectName + Optional.ofNullable(version).map(String::valueOf)
                    .orElse("");
            ResponseBody rb = new ResponseBody(RequestMethod.POST.name(), requestPath,
                tns.getSchema().toString());
            return ResponseEntity.ok(rb);
        }).onErrorResume(ex -> {
            String logMessage =
                "Error in processing request " + RequestMethod.POST.name() + " /fromregistry/"
                    + subjectName;
            logMessage += Optional.ofNullable(version).map(v -> "/" + v).orElse("");
            log.error(logMessage, ex);
            String requestPath =
                "/fromregistry/" + subjectName + Optional.ofNullable(version).map(String::valueOf)
                    .orElse("");
            ResponseBody rb = new ResponseBody(RequestMethod.POST.name(), requestPath,
                "Error in processing request: " + ex.getLocalizedMessage());
            return Mono.just(ResponseEntity.badRequest().body(rb));
        });
    }

    @AllArgsConstructor
    @Getter
    private static class ResponseBody {
        private final String httpMethod;
        private final String path;
        private final String response;
    }
}
