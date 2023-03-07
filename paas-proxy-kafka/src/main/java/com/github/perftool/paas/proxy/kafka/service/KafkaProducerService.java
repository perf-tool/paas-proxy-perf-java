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

package com.github.perftool.paas.proxy.kafka.service;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.perftool.paas.proxy.kafka.config.KafkaConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class KafkaProducerService {

    private final KafkaConfig config;
    private final AsyncLoadingCache<String, KafkaProducer<String, String>> producers;

    public KafkaProducerService(@Autowired KafkaConfig config) {
        this.config = config;
        this.producers = Caffeine.newBuilder()
                .expireAfterAccess(config.producerCacheSeconds, TimeUnit.SECONDS)
                .maximumSize(config.producerMaxSize)
                .removalListener((RemovalListener<String, KafkaProducer<String, String>>) (key, value, cause) -> {
                    log.info("{} producer removed, because of {}", key, cause);
                    try {
                        value.close();
                    } catch (Exception e) {
                        log.error("close failed, ", e);
                    }
                })
                .buildAsync(new AsyncCacheLoader<>() {
                    @NotNull
                    @Override
                    public CompletableFuture<KafkaProducer<String, String>> asyncLoad(
                            @NotNull String key, @NotNull Executor executor) {
                        return acquireFuture(key);
                    }

                    @NotNull
                    @Override
                    public CompletableFuture<KafkaProducer<String, String>> asyncReload(
                            @NotNull String key,
                            @NotNull KafkaProducer<String, String> oldValue,
                            @NotNull Executor executor) {
                        return acquireFuture(key);
                    }
                });
    }

    private CompletableFuture<KafkaProducer<String, String>> acquireFuture(String topic) {
        CompletableFuture<KafkaProducer<String, String>> future = new CompletableFuture<>();
        try {
            future.complete(buildProducer());
        } catch (Exception e) {
            log.error("{} create producer exception ", topic, e);
            future.completeExceptionally(e);
        }
        return future;
    }

    private KafkaProducer<String, String> buildProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaAddress);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, config.idempotence);
        props.put(ProducerConfig.ACKS_CONFIG, config.acks);
        props.put(ProducerConfig.LINGER_MS_CONFIG, config.lingerMS);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.compressionType);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(config.batchSizeKb * 1024));
        if (config.saslMechanism.equals(SecurityProtocol.SASL_PLAINTEXT.name)
                || config.saslMechanism.equals(SecurityProtocol.SASL_SSL.name)) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
            props.put(SaslConfigs.SASL_MECHANISM, config.saslMechanism);
            String saslJaasConfig = String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required %n"
                            + "username=\"%s\" %npassword=\"%s\";",
                    config.saslUsername, config.saslPassword);
            props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
        }
        if (config.saslMechanism.equals(SecurityProtocol.SSL.name)
                || config.saslMechanism.equals(SecurityProtocol.SASL_SSL.name)) {
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, config.truststorePath);
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, config.truststorePassword);
        }
        return new KafkaProducer<>(props);
    }

    public CompletableFuture<KafkaProducer<String, String>> producer(String topic) {
        return producers.get(topic);
    }

}
