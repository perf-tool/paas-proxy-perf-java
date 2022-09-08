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

package com.github.perftool.paas.proxy;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.LocalDateTime;

@EnableScheduling
@SpringBootApplication
public class Main {

    public static void main(String[] args) {
        Thread.setDefaultUncaughtExceptionHandler((thread, exception) -> {
            System.err.printf("%s [%s] error Uncaught exception in thread %s: %s%n",
                    LocalDateTime.now(), thread.getContextClassLoader(),
                    thread.getName(), exception.getMessage());
            exception.printStackTrace(System.err);
        });
        SpringApplication.run(Main.class);
    }

}
