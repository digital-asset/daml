# ledger

Home of our reference ledger implementation (Sandbox) and various ledger related libraries.

## Logging

### Logging Configuration

The Sandbox and Ledger API Server use [Logback](https://logback.qos.ch/) for logging configuration.

#### Log Files

The Sandbox logs at `INFO` level to standard out and to the file `sandbox.log` in the current working directory.

### Log levels

As most Java libraries and frameworks, the Sandbox and Ledger API Server use INFO as the default logging level. This level is for minimal 
and important information (usually only startup and normal shutdown events). INFO level logging should not produce
increasing volume of logging during normal operation.

WARN level should be used for transition between healthy/unhealthy state, or in other close to error scenarios.

DEBUG level should be turned on only when investigating issues in the system, and usually that means we want the trail
loggers. Normal loggers at DEBUG level can be useful sometimes (e.g. DAML interpretation).

## Metrics

Sandbox and Ledger API Server provide a couple of useful metrics:

### Sandbox and Ledger API Server

The Ledger API Server exposes basic metrics for all gRPC services and some additional ones.
<table>
<thead><tr><td>Metric Name</td><td>Description</td></tr>
<tbody>
<tr><td><pre>LedgerApi.com.digitalasset.ledger.api.v1.$SERVICE.$METHOD</pre></td><td>A <i>meter</i> that tracks the number of calls to the respective service and method.
<tr><td><pre>CommandSubmission.failedCommandInterpretations</pre></td><td>A <i>meter</i> that tracks the failed command interpretations.
<tr><td><pre>CommandSubmission.submittedTransactions</pre></td><td>A <i>timer</i> that tracks the commands submitted to the backing ledger. 
</tbody>
</table>

### Indexer
<table>
<thead><tr><td>Metric Name</td><td>Description</td></tr></thead>
<tbody>
<tr><td><pre>JdbcIndexer.processedStateUpdates</pre></td><td>A <i>timer</i> that tracks duration of state update processing.</td></tr>
<tr><td><pre>JdbcIndexer.lastReceivedRecordTime</pre></td><td>A <i>gauge</i> that returns the last received record time in milliseconds since EPOCH.</td></tr>
<tr><td><pre>JdbcIndexer.lastReceivedOffset</pre></td><td>A <i>gauge</i> that returns that last received offset from the ledger.</td></tr>
<tr><td><pre>JdbcIndexer.currentRecordTimeLag</pre></td><td>A <i>gauge</i> that returns the difference between the Indexer's wallclock time and the last received record time in milliseconds.</td></tr>
</tbody>
</table>

The following JMX domains are used:

* Sandbox: `com.digitalasset.platform.sandbox`
* Ledger API Server: `com.digitalasset.platform.ledger-api-server.$PARTICIPANT_ID`
* Indexer Server: `com.digitalasset.platform.indexer.$PARTICIPANT_ID`

`$PARTICIPANT_ID` is provided via CLI parameters.

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
