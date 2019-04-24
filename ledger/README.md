# ledger

Home of our reference ledger implementation (Sandbox) and various ledger related libraries.

## v1 gRPC API

The v1 gRPC API is described [here](API.md)

## Logging

### Logging Configuration

Ledger Server uses [Logback](https://logback.qos.ch/) for logging configuration.

#### Log Files

By [default our log configuration](https://github.com/DACH-NY/da/blob/master/ledger/platform-deployment/src/main/resources/logback.xml) creates two log files:
- a plaintext file `logs/ledger.log`
- a json file `logs/ledger.json.log` (for Logstash type log processors)

The path the file is stored in can be adjusted by setting `-Dlogging.location=some/other/path`.
The filename used can be adjusted by setting `-Dlogging.file=my-process-logs`.

By default no output is sent to stdout (beyond logs from the logging setup itself).

#### standard streams logging

For development and testing it can be useful to send all logs to stdout & stderr rather than files (for instance to use the IntelliJ console or getting useful output from docker containers).

We [ship a logging configuration for this](https://github.com/DACH-NY/da/blob/master/ledger/platform-deployment/src/main/resources/logback-standard.xml) which can be enabled by using `-Dlogback.configurationFile=classpath:logback-standard.xml -Dlogging.config=classpath:logback-standard.xml`.

INFO level and below goes to stdout. WARN and above goes to stderr.

_Note: always use both `-Dlogback.configurationFile` and `-Dlogging.config`. Logback is first initialized with the configuration file from `logback.configurationFile`. When the Spring framework boots it recreates logback and uses the configuration specified in `logging.config`.

### Log levels

As most Java libraries and frameworks, ledger server uses INFO as the default logging level. This level is for minimal 
and important information (usually only startup and normal shutdown events). INFO level logging should not produce
increasing volume of logging during normal operation.

WARN level should be used for transition between healthy/unhealthy state, or in other close to error scenarios.

DEBUG level should be turned on only when investigating issues in the system, and usually that means we want the trail
loggers. Normal loggers at DEBUG level can be useful sometimes (e.g. DAML interpretation).

## gRPC and back-pressure

### RPC

Standard RPC requests should return with RESOURCE_EXHAUSTED status code to signal back-pressure. Envoy can be configured
to retry on these errors. We have to be careful not to have any persistent changes when returning with such an error as
the same original request can be retried on another service instance.

### Streaming

gRPC's streaming protocol has built-in flow-control, but it's not fully active by default. What it does it controls the
flow between the TCP/HTTP layer and the library so it builds on top of TCP's own flow control. The inbound flow control
is active by default, but the outbound does not signal back-pressure out of the box.

`AutoInboundFlowControl`: The default behaviour for handling incoming items in a stream is to automatically signal demand
after every `onNext` call. This is the correct thing to do if the handler logic is CPU bound and does not depend on other
reactive downstream services. By default it's active on all inbound streams. One can disable this and signal demand by
manually calling `request` to follow demands of downstream services. Disabling this feature is possible by calling
`disableAutoInboundFlowControl` on `CallStreamObserver`.

`ServerCallStreamObserver`: casting an outbound `StreamObserver` manually to `ServerCallStreamObserver` gives us access
to `isReady` and `onReadyHandler`. With these methods we can check if there is available capacity in the channel i.e.
we are safe to push into it. This can be used to signal demand to our upstream flow. Note that gRPC buffers 32Kb data
per channel and `isReady` will return false only when this buffer gets full.
