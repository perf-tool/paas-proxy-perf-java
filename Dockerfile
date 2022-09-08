#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

FROM perftool/compile:jdk17-mvn AS build
COPY . /opt/perf/compile
WORKDIR /opt/perf/compile
RUN mvn -B clean package -Dmaven.test.skip=true


FROM perftool/base:jdk17

COPY --from=build /opt/perf/compile/paas-proxy/target/paas-proxy-0.0.1.jar /opt/perf/paas-proxy.jar
COPY --from=build /opt/perf/compile/paas-proxy/target/lib/* /opt/perf/lib/
COPY --from=build /opt/perf/compile/paas-proxy/target/conf/* /opt/perf/conf/

COPY docker-build /opt/perf

CMD ["/usr/bin/dumb-init", "bash", "-vx","/opt/perf/scripts/start.sh"]
