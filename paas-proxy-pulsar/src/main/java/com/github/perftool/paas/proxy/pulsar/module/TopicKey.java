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

package com.github.perftool.paas.proxy.pulsar.module;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Objects;

@Data
@AllArgsConstructor
public class TopicKey {

    private String tenant;

    private String namespace;

    private String topic;

    public TopicKey() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TopicKey topicKey = (TopicKey) o;

        if (!Objects.equals(tenant, topicKey.tenant)) {
            return false;
        }
        if (!Objects.equals(namespace, topicKey.namespace)) {
            return false;
        }
        return Objects.equals(topic, topicKey.topic);
    }

    @Override
    public int hashCode() {
        int result = tenant != null ? tenant.hashCode() : 0;
        result = 31 * result + (namespace != null ? namespace.hashCode() : 0);
        result = 31 * result + (topic != null ? topic.hashCode() : 0);
        return result;
    }
}
