// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import java.util.concurrent.atomic.AtomicBoolean

import com.codahale.metrics.{MetricRegistry, Timer}
import com.daml.ledger.participant.state.metrics.MetricName
import io.grpc.{Metadata, ServerCall, ServerCallHandler, ServerInterceptor}

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
    val listener = next.startCall(call, headers)
    new TimedListener(listener, timer)
  }

  class TimedListener[ReqT](listener: ServerCall.Listener[ReqT], timer: Timer.Context)
      extends ServerCall.Listener[ReqT] {
    private val timerStopped = new AtomicBoolean(false)

    override def onReady(): Unit =
      listener.onReady()

    override def onMessage(message: ReqT): Unit =
      listener.onMessage(message)

    override def onHalfClose(): Unit =
      listener.onHalfClose()

    override def onCancel(): Unit = {
      listener.onCancel()
      stopTimer()
    }

    override def onComplete(): Unit = {
      listener.onComplete()
      stopTimer()
    }

    private def stopTimer(): Unit = {
      if (timerStopped.compareAndSet(false, true)) {
        timer.stop()
        ()
      }
    }
  }

}
