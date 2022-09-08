# pulsar-proxy-http

## test command
```bash
curl -XPOST -H 'content-type: application/json;charset=UTF-8' localhost:20001/v1/pulsar/tenants/public/namespaces/default/topics/test/produce -d '{"msg":"xxx"}' -iv
```