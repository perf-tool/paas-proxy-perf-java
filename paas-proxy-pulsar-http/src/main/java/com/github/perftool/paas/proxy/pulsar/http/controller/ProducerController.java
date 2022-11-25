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

package com.github.perftool.paas.proxy.pulsar.http.controller;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.perftool.paas.common.metric.MetricBean;
import com.github.perftool.paas.proxy.pulsar.config.PulsarConfig;
import com.github.perftool.paas.proxy.pulsar.module.TopicKey;
import com.github.perftool.paas.proxy.pulsar.service.PulsarClientService;
import com.github.perftool.paas.common.module.Semantic;
import com.github.perftool.paas.common.proxy.http.module.ProduceMsgReq;
import com.github.perftool.paas.common.proxy.http.module.ProduceMsgResp;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Producer;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@RestController
@RequestMapping(path = "/v1/pulsar")
public class ProducerController {

    @Autowired
    private PulsarClientService pulsarClientService;

    @Autowired
    private PulsarConfig pulsarConfig;

    private AsyncLoadingCache<TopicKey, Producer<byte[]>> producerCache;

    private final AtomicInteger atomicInteger = new AtomicInteger();

    private ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);

    @Autowired
    private MeterRegistry meterRegistry;

    private MetricBean metricBean;

    private Runnable producerSizeReportTask = new Runnable() {
        @Override
        public void run() {
            metricBean.recordCachedProducerNum(producerCache.asMap().size());
        }
    };

    @PostConstruct
    public void init() {
        this.producerCache = Caffeine.newBuilder()
                .expireAfterAccess(pulsarConfig.producerCacheSeconds, TimeUnit.SECONDS)
                .maximumSize(pulsarConfig.producerMaxSize)
                .removalListener((RemovalListener<TopicKey, Producer<byte[]>>) (key, value, cause) -> {
                    log.info("topic {} cache removed, because of {}", key.getTopic(), cause);
                    try {
                        value.close();
                    } catch (Exception e) {
                        log.error("close failed, ", e);
                    }
                })
                .buildAsync(new AsyncCacheLoader<>() {
                    @NotNull
                    @Override
                    public CompletableFuture<Producer<byte[]>> asyncLoad(@NotNull TopicKey key, @NotNull Executor executor) {
                        return acquireFuture(key);
                    }

                    @NotNull
                    @Override
                    public CompletableFuture<Producer<byte[]>> asyncReload(@NotNull TopicKey key,
                                                                           @NotNull Producer<byte[]> oldValue,
                                                                           @NotNull Executor executor) {
                        return acquireFuture(key);
                    }
                });
        this.metricBean = new MetricBean(meterRegistry);
        scheduledExecutorService.scheduleAtFixedRate(producerSizeReportTask, 1, 1, TimeUnit.SECONDS);
    }

    @PostMapping(path = "/tenants/{tenant}/namespaces/{namespace}/topics/{topic}/produce")
    public Mono<ResponseEntity<ProduceMsgResp>> produce(@PathVariable(name = "tenant") String tenant,
                                                        @PathVariable(name = "namespace") String namespace,
                                                        @PathVariable(name = "topic") String topic,
                                                        @RequestBody ProduceMsgReq produceMsgReq) {
        if (StringUtils.isEmpty(produceMsgReq.getMsg())) {
            return Mono.error(new Exception("msg can't be empty"));
        }
        CompletableFuture<ResponseEntity<ProduceMsgResp>> future = new CompletableFuture<>();
        long startTime = System.currentTimeMillis();
        int topicSuffixNum = pulsarConfig.topicSuffixNum;
        if (topicSuffixNum > 0) {
            final int increment = atomicInteger.getAndIncrement();
            int index = increment % topicSuffixNum;
            topic = topic + "_" + index;
        }
        TopicKey topicKey = new TopicKey(tenant, namespace, topic);
        final CompletableFuture<Producer<byte[]>> cacheFuture = producerCache.get(topicKey);
        String finalTopic = topic;
        cacheFuture.whenComplete((producer, e) -> {
            if (e != null) {
                log.error("create pulsar client exception ", e);
                future.complete(new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR));
                return;
            }
            try {
                producer.sendAsync(produceMsgReq.getMsg().getBytes(StandardCharsets.UTF_8))
                        .whenComplete(((messageId, throwable) -> {
                            if (throwable != null) {
                                log.error("send producer msg error ", throwable);
                                future.complete(new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR));
                                return;
                            }
                            log.info("topic {} send success, msg id is {}", finalTopic, messageId);
                            if (pulsarConfig.produceSemantic.equals(Semantic.AT_LEAST_ONCE)) {
                                future.complete(new ResponseEntity<>(new ProduceMsgResp(System.currentTimeMillis() - startTime), HttpStatus.OK));
                            }
                        }));
                if (pulsarConfig.produceSemantic.equals(Semantic.AT_MOST_ONCE)) {
                    future.complete(new ResponseEntity<>(new ProduceMsgResp(System.currentTimeMillis() - startTime), HttpStatus.OK));
                }
            } catch (Exception ex) {
                log.error("send async failed ", ex);
                future.complete(new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR));
            }
        });
        return Mono.fromFuture(future);
    }

    private CompletableFuture<Producer<byte[]>> acquireFuture(TopicKey topicKey) {
        CompletableFuture<Producer<byte[]>> future = new CompletableFuture<>();
        try {
            future.complete(pulsarClientService.createProducer(topicKey));
        } catch (Exception e) {
            log.error("{} create producer exception ", topicKey, e);
            future.completeExceptionally(e);
        }
        return future;
    }


}
