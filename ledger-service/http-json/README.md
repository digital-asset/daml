# HTTP JSON Service

## How to start

### Start sandbox from a DAML Assistant project directory
```
$ daml start
```

### Start HTTP service from the DAML project root
This will build the service first, can take up to 5-10 minutes when running first time.
```
$ bazel run //ledger-service/http-json:http-json-bin -- localhost 6865 7575
```
Where:
 - localhost 6865 -- sandbox host name and port
 - 7575 -- HTTP service port
