// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.metrics

import io.grpc.{Metadata, ServerCall, ServerCallHandler, ServerInterceptor}

/**
  * An interceptor that counts all incoming calls by method.
  * The metrics generated will be of the following schema:
  * <pre>LedgerApi.&lt;service-name&gt;.&lt;method-name&gt;</pre>
  * <br/>
  * For example:
  * <pre>LedgerApi.com.digitalasset.ledger.api.v1.TransactionService.GetTransactionTrees</pre>
  */
class MetricsInterceptor(metricsManager: MetricsManager) extends ServerInterceptor {
  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {
    // The service name and method name are separated by a '/'.
    // Converting it to a '.' makes it easier for downstream tools to
    // put the metrics into a hierarchy.
    val serviceName = call.getMethodDescriptor.getFullMethodName.replace('/', '.')
    metricsManager.metrics.meter(s"LedgerApi.$serviceName").mark()
    next.startCall(call, headers)
  }
}
