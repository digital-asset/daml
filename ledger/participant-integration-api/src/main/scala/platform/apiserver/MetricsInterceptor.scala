// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import com.daml.metrics.MetricHandle.Timer
import com.daml.metrics.MetricHandle.Timer.TimerStop
import com.daml.metrics.Metrics
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall
import io.grpc._

import scala.collection.concurrent.TrieMap

/** An interceptor that counts all incoming calls by method.
  *
  * Given a fully qualified method name 'q' in the form "org.example.SomeService/someMethod" where
  * - "org.example.SomeService" is the fully qualified service name and
  * - "SomeService" is the simplified service name 's'
  * - "someMethod" is the method 'm'
  * the names assigned to metrics measured for the service identified by
  * 'q' is prepended by the concatenation of
  * - the literal string "daml"
  * - the literal string "lapi"
  * - 's' converted to snake_case
  * - 'm' converted to snake_case
  * joined together with the character '.'
  *
  * e.g. "org.example.SomeService/someMethod" becomes "daml.lapi.some_service.some_method"
  */
private[apiserver] final class MetricsInterceptor(metrics: Metrics) extends ServerInterceptor {

  // Cache the result of calling MetricsInterceptor.nameFor, which practically has a
  // limited co-domain and whose cost we don't want to pay every time an endpoint is hit
  private val fullServiceToMetricNameCache = TrieMap.empty[String, Timer]

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {
    val fullMethodName = call.getMethodDescriptor.getFullMethodName
    val timer = fullServiceToMetricNameCache.getOrElseUpdate(
      fullMethodName,
      metrics.daml.lapi.forMethod(MetricsNaming.nameFor(fullMethodName)),
    )
    val timerCtx = timer.startAsync()
    next.startCall(new TimedServerCall(call, timerCtx), headers)
  }

  private final class TimedServerCall[ReqT, RespT](
      delegate: ServerCall[ReqT, RespT],
      timerStop: TimerStop,
  ) extends SimpleForwardingServerCall[ReqT, RespT](delegate) {
    override def close(status: Status, trailers: Metadata): Unit = {
      metrics.daml.lapi.return_status.forCode(status.getCode.toString).inc()
      delegate.close(status, trailers)
      timerStop()
      ()
    }
  }

}
