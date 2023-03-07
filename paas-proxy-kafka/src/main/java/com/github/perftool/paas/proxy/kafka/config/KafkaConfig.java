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

package com.github.perftool.paas.proxy.kafka.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

@Configuration
@Service
public class KafkaConfig {

    @Value("${KAFKA_ADDRESS:localhost:9092}")
    public String kafkaAddress;

    @Value("${KAFKA_IDEMPOTENCE:false}")
    public boolean idempotence;

    @Value("${KAFKA_ACKS:1}")
    public String acks;

    @Value("${KAFKA_LINGER_MS:0}")
    public int lingerMS;

    @Value("${KAFKA_BATCH_SIZE_KB:16}")
    public int batchSizeKb;

    @Value("${KAFKA_COMPRESSION_TYPE:none}")
    public String compressionType;

    @Value("${KAFKA_SASL_MECHANISM:PLAIN}")
    public String saslMechanism;

    @Value("${KAFKA_SASL_USERNAME:}")
    public String saslUsername;

    @Value("${KAFKA_SASL_PASSWORD:}")
    public String saslPassword;

    @Value("${PULSAR_PRODUCER_CACHE_SECONDS:600}")
    public long producerCacheSeconds;

    @Value("${PULSAR_PRODUCER_MAX_SIZE:3000}")
    public long producerMaxSize;

    @Value("${TRUSTSTORE_PATH:}")
    public String truststorePath;

    @Value("${TRUSTSTORE_PASSWORD:}")
    public String truststorePassword;
}
