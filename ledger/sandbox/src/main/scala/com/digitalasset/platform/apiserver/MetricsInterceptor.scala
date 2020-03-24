// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver

import java.util.concurrent.atomic.AtomicBoolean

import com.codahale.metrics.{MetricRegistry, Timer}
import io.grpc.{Metadata, ServerCall, ServerCallHandler, ServerInterceptor}

import scala.collection.concurrent.TrieMap

object MetricsInterceptor {

  private[this] val capitalization = "[A-Z]+".r
  private[this] val startWordCapitalization = "^[A-Z]+".r
  private[this] val endWordAcronym = "[A-Z]{2,}$".r

  private[this] val snakifyWholeWord = (s: String) => if (s.forall(_.isUpper)) s.toLowerCase else s

  private[this] val snakify = (s: String) =>
    capitalization.findAllMatchIn(s).foldRight(s) { (m, r) =>
      val s = m.toString
      if (s.length == 1) r.patch(m.start, s"_${s.toLowerCase}", 1)
      else r.patch(m.start, s"_${s.init.toLowerCase}_${s.last.toLower}", s.length)
  }

  private[this] val snakifyStart = (s: String) =>
    startWordCapitalization.findFirstIn(s).fold(s) { m =>
      s.patch(
        0,
        if (m.length == 1) m.toLowerCase else m.init.toLowerCase,
        math.max(m.length - 1, 1))
  }

  private[this] val snakifyEnd = (s: String) =>
    endWordAcronym.findFirstIn(s).fold(s) { m =>
      s.patch(s.length - m.length, s"_${m.toLowerCase}", m.length)
  }

  // Turns a camelCased string into a snake_cased one
  private[apiserver] val camelCaseToSnakeCase: String => String =
    snakifyWholeWord andThen snakifyStart andThen snakifyEnd andThen snakify

  // assert(fullServiceName("org.example.SomeService/someMethod") == "daml.lapi.some_service.some_method")
  private[apiserver] def nameFor(fullMethodName: String): String = {
    val serviceAndMethodName = fullMethodName.split('/')
    assert(
      serviceAndMethodName.length == 2,
      s"Expected service and method names separated by '/', got '$fullMethodName'")
    val serviceName = camelCaseToSnakeCase(serviceAndMethodName(0).split('.').last)
    val methodName = camelCaseToSnakeCase(serviceAndMethodName(1))
    s"daml.lapi.$serviceName.$methodName"
  }

}

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
  private val fullServiceToMetricNameCache = TrieMap.empty[String, String]

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {
    val fullMethodName = call.getMethodDescriptor.getFullMethodName
    val metricName = fullServiceToMetricNameCache.getOrElseUpdate(
      fullMethodName,
      MetricsInterceptor.nameFor(fullMethodName))
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
