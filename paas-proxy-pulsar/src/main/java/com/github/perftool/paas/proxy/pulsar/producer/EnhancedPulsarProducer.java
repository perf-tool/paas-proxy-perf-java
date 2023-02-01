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

package com.github.perftool.paas.proxy.pulsar.producer;

import io.netty.util.HashedWheelTimer;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class EnhancedPulsarProducer<Schema> {

    private final int retryLimit;

    private final int retryIntervalInSecond;

    private final Producer<Schema> producer;

    private static final HashedWheelTimer timer = new HashedWheelTimer(
            new DefaultThreadFactory("producerSendRetry"));

    public EnhancedPulsarProducer(Producer<Schema> producer, int retryLimit, int retryIntervalInSecond) {
        this.producer = producer;
        this.retryLimit = retryLimit;
        this.retryIntervalInSecond = retryIntervalInSecond;
    }

    public CompletableFuture<MessageId> sendAsync(Schema message) {
        CompletableFuture<MessageId> future = new CompletableFuture<>();
        doSendMsg(future, message, 0, 0);
        return future;
    }

    private void doSendMsg(CompletableFuture<MessageId> sendFuture, Schema msg, int retryTime, int preTryInterval) {
        producer.sendAsync(msg)
                .thenAccept(sendFuture::complete)
                .exceptionally(ex -> {
                    if (ex != null && retryLimit > 0) {
                        if (retryTime >= retryLimit) {
                            log.error("[{}] retry time exceeded limit {}/{}",
                                    producer.getTopic(), retryTime, retryLimit);
                            sendFuture.completeExceptionally(ex);
                            return null;
                        }
                        int nextRetryInterval = preTryInterval == 0 ? retryIntervalInSecond : preTryInterval << 1;
                        log.warn("[{}] internal send message failed, error is {}, retried {}/{}, "
                                        + "next retry will process in {} s",
                                producer.getTopic(), ex.getMessage(), retryTime, retryLimit, nextRetryInterval);
                        timer.newTimeout(timeout -> doSendMsg(sendFuture, msg, retryTime + 1,
                                nextRetryInterval), nextRetryInterval, TimeUnit.SECONDS);
                    }
                    sendFuture.completeExceptionally(ex);
                    return null;
                });
    }

    public void close() throws PulsarClientException {
        producer.close();
    }
}
