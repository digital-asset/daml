// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc.ratelimiting

import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.ratelimiting.LimitResult.*
import com.digitalasset.canton.networking.grpc.{ActiveStreamCounterInterceptor, CantonGrpcUtil}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap

/** Counts number of open stream and can be used with a [[RateLimitingInterceptor]] to enforce a
  * limit for open requests
  */
class StreamCounterCheck(
    initialLimits: Map[FullMethodName, NonNegativeInt],
    warnOnUnconfiguredLimits: Boolean,
    val loggerFactory: NamedLoggerFactory,
) extends ActiveStreamCounterInterceptor
    with NamedLogging {

  private val counters = new TrieMap[FullMethodName, Int]()
  private val notified = new AtomicReference[Set[FullMethodName]](Set.empty)
  private val limits = new AtomicReference[Map[FullMethodName, NonNegativeInt]](initialLimits)

  def updateLimits(key: FullMethodName, newLimit: Option[NonNegativeInt]): Unit = {
    logger.info(s"Setting limit of $key to $newLimit")(TraceContext.empty)
    limits.updateAndGet(_.updatedWith(key)(_ => newLimit)).discard
  }

  private def errorFactory(methodName: FullMethodName)(implicit
      traceContext: TraceContext
  ): RpcError = {
    val err = CantonGrpcUtil.GrpcErrors.Overloaded.TooManyStreams(methodName)
    err.log()
    err.toCantonRpcError
  }

  private def adjust(methodName: FullMethodName, delta: Int): Unit =
    counters
      .updateWith(methodName) {
        case Some(current) => Some(current + delta)
        case None => Some(delta)
      }
      .discard

  override protected def established(methodName: FullMethodName): Unit = adjust(methodName, 1)

  override protected def finished(methodName: FullMethodName): Unit = adjust(methodName, -1)

  def check(methodName: FullMethodName, isStream: IsStream): LimitResult = if (isStream)
    limits.get().get(methodName) match {
      case Some(limit) =>
        val current = counters.getOrElse(methodName, 0)
        if (current >= limit.value) {
          LimitResult.OverLimit(
            errorFactory(methodName)(
              TraceContextGrpc.fromGrpcContextOrNew("StreamCounterCheck.check")
            )
          )
        } else LimitResult.UnderLimit
      case None =>
        if (!notified.get().contains(methodName)) {
          val msg = s"No upper active stream limit configured for $methodName"
          val tc = TraceContextGrpc.fromGrpcContextOrNew("StreamCounterCheck.check")
          if (warnOnUnconfiguredLimits)
            logger.warn(msg)(tc)
          else
            logger.info(msg)(tc)
          notified.updateAndGet(_ + methodName).discard
        }
        LimitResult.UnderLimit
    }
  else LimitResult.UnderLimit

}
