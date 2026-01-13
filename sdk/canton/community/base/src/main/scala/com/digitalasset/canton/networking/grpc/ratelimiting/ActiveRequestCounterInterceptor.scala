// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc.ratelimiting

import com.daml.metrics.api.MetricHandle.Gauge
import com.daml.metrics.api.MetricsContext
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.ActiveRequestsMetrics
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.ratelimiting.ActiveRequestCounterInterceptor.Limited
import com.digitalasset.canton.networking.grpc.ratelimiting.LimitResult.FullMethodName
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.TryUtil
import com.digitalasset.canton.util.TryUtil.ForFailedOps
import io.grpc.*
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

/** Count and enforce limit for selected endpoints
  *
  * Works for both, streams and requests. Limits the number of pending requests. For each enforced
  * method, the count and limits are reported as metrics.
  *
  * @param api
  *   The API name for metrics labelling and ensuring that the metric counts don't conflict
  * @param warnOnUnconfiguredLimits
  *   if true, then each method without a configured limit will be logged as warning
  */
class ActiveRequestCounterInterceptor(
    api: String,
    initialLimits: Map[FullMethodName, NonNegativeInt],
    warnOnUnconfiguredLimits: Boolean,
    maxLoggingRatePerSecond: NonNegativeInt,
    metrics: ActiveRequestsMetrics,
    val loggerFactory: NamedLoggerFactory,
) extends ServerInterceptor
    with NamedLogging {

  private val notified = new AtomicReference[Set[FullMethodName]](Set.empty)
  private val limits = new AtomicReference[Map[FullMethodName, Limited]](Map.empty)

  initialLimits.foreach { case (method, limit) =>
    updateLimits(method, Some(limit))
  }

  def updateLimits(key: FullMethodName, newLimit: Option[NonNegativeInt]): Unit = {
    logger.info(s"Setting limit of $key to $newLimit")(TraceContext.empty)
    limits
      .updateAndGet(
        _.updatedWith(key)(cur =>
          newLimit.map { limit =>
            cur.fold {
              // reset counters when creating a new limit
              val (active, limitGauge) = metrics.getActiveAndLimitGauge(api, key)
              active.updateValue(0)
              limitGauge.updateValue(limit.value)
              Limited(new AtomicInteger(0), active, limitGauge, metrics.mkContext(api, key))
            } { cur =>
              cur.limitGauge.updateValue(limit.value)
              cur
            }
          }
        )
      )
      .discard
  }

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {

    val fullMethodName = call.getMethodDescriptor.getFullMethodName
    val result = check(fullMethodName)

    result match {
      // accept if not monitored
      case None =>
        next.startCall(call, headers)
      // monitor completion if monitored and under limit
      case Some((true, limit)) =>
        try {
          val delegate = next.startCall(call, headers)
          new OnCloseCallListener(
            delegate,
            runOnceOnTermination = () => limit.adjust(-1).discard,
          )
        } catch {
          case ex: Throwable =>
            // decrement if we failed to construct the listener
            limit.adjust(-1).discard
            throw ex
        }
      // reject if over limit
      case Some((false, limit)) =>
        val ip = call.getAttributes.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR)
        val error = errorFactory(fullMethodName, ip.toString)(
          TraceContextGrpc.fromGrpcContextOrNew("ActiveRequestCounterInterceptor")
        )
        metrics.rejections.inc()(limit.metricsContext)
        val statusRuntimeException = error.asGrpcError
        call.close(statusRuntimeException.getStatus, statusRuntimeException.getTrailers)
        new ServerCall.Listener[ReqT]() {}
    }

  }

  private val logFrequency = new AtomicReference[(Double, Long)]((0.0, 0))
  private val logSuspended = new AtomicBoolean(false)
  private def errorFactory(methodName: FullMethodName, ip: String)(implicit
      traceContext: TraceContext
  ): RpcError = {
    val err = CantonGrpcUtil.GrpcErrors.Overloaded
      .TooManyConcurrentRequests(methodName, ip)
    val (count, _) = logFrequency.updateAndGet { case (count, lastLogged) =>
      val newLastLogged = System.nanoTime()
      val newCount =
        Math.max(
          0.0,
          1 + count - (maxLoggingRatePerSecond.value / 1e9) * (newLastLogged - lastLogged),
        )
      (newCount, newLastLogged)
    }
    val suspendLogs = count > maxLoggingRatePerSecond.value
    if (suspendLogs && !logSuspended.getAndSet(true)) {
      err.log()
      logger.warn(
        s"Suspending detailed overload logging for excessive rejections until rate drops below $maxLoggingRatePerSecond"
      )
    } else if (count < maxLoggingRatePerSecond.value) {
      logSuspended.set(false)
      err.log()
    }
    err.toCantonRpcError
  }

  /** check if limit has been reached for the given method
    *
    * @return
    *   returns None if the method is not monitored. otherwise returns a tuple where the first
    *   element indicates if the request is accepted and the second element is the limit object for
    *   further adjustments
    */
  private def check(methodName: FullMethodName): Option[(Boolean, Limited)] =
    limits.get().get(methodName) match {
      case Some(limit) =>
        val current = limit.adjust(+1)
        // reject if we are already at capacity
        if (current > limit.enforcedLimit) {
          limit.adjust(-1).discard
          Some((false, limit))
        } else {
          Some((true, limit))
        }
      case None =>
        if (
          !notified.get().contains(methodName) && !RateLimitingInterceptor.doNotLimit.contains(
            methodName
          )
        ) {
          val msg = s"No upper active stream limit configured for $methodName"
          val tc = TraceContextGrpc.fromGrpcContextOrNew("ActiveRequestCounterInterceptor")
          if (warnOnUnconfiguredLimits)
            logger.warn(msg)(tc)
          else
            logger.info(msg)(tc)
          notified.updateAndGet(_ + methodName).discard
        }
        None
    }

  private class OnCloseCallListener[RespT](
      delegate: ServerCall.Listener[RespT],
      runOnceOnTermination: () => Unit,
  ) extends SimpleForwardingServerCallListener[RespT](delegate) {

    private val logger = LoggerFactory.getLogger(getClass)
    private val onTerminationCalled = new AtomicBoolean()

    private def runOnClose(): Unit =
      if (onTerminationCalled.compareAndSet(false, true)) {
        TryUtil
          .tryCatchAll(runOnceOnTermination())
          .forFailed(logger.warn(s"Exception calling onClose method", _))
      }

    override def onCancel(): Unit = {
      runOnClose()
      super.onCancel()
    }

    override def onComplete(): Unit = {
      runOnClose()
      super.onComplete()
    }

  }

}

object ActiveRequestCounterInterceptor {
  private final case class Limited(
      active: AtomicInteger,
      activeGauge: Gauge[Int],
      limitGauge: Gauge[Int],
      metricsContext: MetricsContext,
  ) {
    def enforcedLimit: Int = limitGauge.getValue
    def adjust(delta: Int): Int = {
      val newActive = active.updateAndGet(_ + delta)
      activeGauge.updateValue(newActive)
      newActive
    }

  }
}
