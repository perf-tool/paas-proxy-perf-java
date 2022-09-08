# paas-proxy-perf-java
## environment variables
### MAVEN_ADDRESS
example: localhost
### PULSAR_JAR_VERSION
example: 2.10.0-202222
## http test command
### readiness
```bash
curl -H 'content-type: application/json;charset=UTF-8' localhost:20001/v1/readiness
```
### pulsar-http
```bash
curl -XPOST -H 'content-type: application/json;charset=UTF-8' localhost:20001/v1/pulsar/tenants/public/namespaces/default/topics/test/produce -d '{"msg":"xxx"}' -iv
```
