// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import com.codahale.metrics.{MetricRegistry, Timer}
import com.daml.metrics.MetricName
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall
import io.grpc.{Metadata, ServerCall, ServerCallHandler, ServerInterceptor, Status}

import scala.collection.concurrent.TrieMap

/**
  * An interceptor that counts all incoming calls by method.
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
final class MetricsInterceptor(metrics: MetricRegistry) extends ServerInterceptor {

  // Cache the result of calling MetricsInterceptor.nameFor, which practically has a
  // limited co-domain and whose cost we don't want to pay every time an endpoint is hit
  private val fullServiceToMetricNameCache = TrieMap.empty[String, MetricName]

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {
    val fullMethodName = call.getMethodDescriptor.getFullMethodName
    val metricName = fullServiceToMetricNameCache.getOrElseUpdate(
      fullMethodName,
      MetricsNaming.nameFor(fullMethodName))
    val timer = metrics.timer(metricName).time()
    next.startCall(new TimedServerCall(call, timer), headers)
  }

  private final class TimedServerCall[ReqT, RespT](
      delegate: ServerCall[ReqT, RespT],
      timer: Timer.Context,
  ) extends SimpleForwardingServerCall[ReqT, RespT](delegate) {
    override def close(status: Status, trailers: Metadata): Unit = {
      delegate.close(status, trailers)
      timer.stop()
      ()
    }
  }

}
