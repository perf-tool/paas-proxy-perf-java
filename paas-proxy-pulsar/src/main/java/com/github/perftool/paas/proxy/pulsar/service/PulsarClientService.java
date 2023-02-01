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

package com.github.perftool.paas.proxy.pulsar.service;

import com.github.perftool.paas.proxy.pulsar.config.PulsarConfig;
import com.github.perftool.paas.proxy.pulsar.module.TopicKey;
import com.github.perftool.paas.proxy.pulsar.producer.EnhancedPulsarProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class PulsarClientService {

    private final PulsarClient pulsarClient;

    private final PulsarConfig pulsarConfig;

    public PulsarClientService(@Autowired PulsarConfig pulsarConfig) {
        try {
            pulsarClient = PulsarClient.builder()
                    .operationTimeout(pulsarConfig.operationTimeoutSeconds, TimeUnit.SECONDS)
                    .ioThreads(pulsarConfig.ioThreads)
                    .serviceUrl(String.format("http://%s:%s", pulsarConfig.host, pulsarConfig.port))
                    .build();
            this.pulsarConfig = pulsarConfig;
        } catch (Exception e) {
            log.error("create pulsar client exception ", e);
            throw new IllegalArgumentException("build pulsar client exception, exit");
        }
    }

    public EnhancedPulsarProducer<byte[]> createProducer(TopicKey topicKey) throws Exception {
        ProducerBuilder<byte[]> builder = pulsarClient.newProducer().enableBatching(true);
        builder = builder.maxPendingMessages(pulsarConfig.producerMaxPendingMessage);
        builder = builder.autoUpdatePartitions(pulsarConfig.autoUpdatePartition);
        if (pulsarConfig.producerBatch) {
            builder = builder.enableBatching(true);
            builder = builder.batchingMaxPublishDelay(pulsarConfig.producerBatchDelayMs, TimeUnit.MILLISECONDS);
        } else {
            builder = builder.enableBatching(false);
        }
        return new EnhancedPulsarProducer<byte[]>(builder.topic(concatTopicFn(topicKey)).create(),
                pulsarConfig.sendRetryLimit, pulsarConfig.sendRetryInterval);
    }

    private String concatTopicFn(TopicKey topicKey) {
        return String.format("persistent://%s/%s/%s",
                topicKey.getTenant(), topicKey.getNamespace(), topicKey.getTopic());
    }

}
