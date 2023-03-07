/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.github.perftool.paas.proxy.kafka.http.controller;

import com.github.perftool.paas.common.proxy.http.module.ProduceMsgReq;
import com.github.perftool.paas.common.proxy.http.module.ProduceMsgResp;
import com.github.perftool.paas.proxy.kafka.service.KafkaProducerService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;

@Slf4j
@RestController
@RequestMapping(path = "/v1/kafka")
public class ProducerController {
    @Autowired
    private KafkaProducerService kafkaProducerService;

    @PostMapping(path = "/topics/{topic}/produce")
    public Mono<ResponseEntity<ProduceMsgResp>> send(@PathVariable(name = "topic") String topic,
                                                        @RequestBody ProduceMsgReq produceMsgReq) {
        long startTime = System.currentTimeMillis();
        if (StringUtils.isEmpty(produceMsgReq.getMsg())) {
            return Mono.error(new Exception("msg can't be empty"));
        }
        CompletableFuture<ResponseEntity<ProduceMsgResp>> future = new CompletableFuture<>();
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, produceMsgReq.getMsg());
        kafkaProducerService.producer(topic).thenAccept(producer -> producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("send msg to {} failed", topic, exception);
                future.completeExceptionally(exception);
            } else {
                log.debug("send msg to {} success, offset {}", topic, metadata.offset());
                future.complete(new ResponseEntity<>(
                        new ProduceMsgResp(System.currentTimeMillis() - startTime), HttpStatus.OK));
            }
        })).exceptionally(throwable -> {
            log.error("send msg to {} failed", topic, throwable);
            future.completeExceptionally(throwable);
            return null;
        });
        return Mono.fromFuture(future);
    }
}
