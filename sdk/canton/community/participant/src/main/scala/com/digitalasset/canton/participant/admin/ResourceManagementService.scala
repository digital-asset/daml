// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.error.LedgerApiErrors.ParticipantBackpressure
import com.digitalasset.canton.ledger.participant.state.v2.SubmissionResult
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.networking.grpc.StaticGrpcServices
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.ParticipantOverloaded
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.math.Ordered.orderingToOrdered

trait ResourceManagementService {

  def metrics: ParticipantMetrics

  def warnIfOverloadedDuring: Option[NonNegativeFiniteDuration]

  private val lastSuccess: AtomicReference[CantonTimestamp] =
    new AtomicReference[CantonTimestamp](CantonTimestamp.now())
  private val lastWarning: AtomicReference[CantonTimestamp] =
    new AtomicReference[CantonTimestamp](CantonTimestamp.now())

  metrics
    .registerMaxInflightValidationRequest(() =>
      resourceLimits.maxInflightValidationRequests.map(_.unwrap)
    )
    .discard

  def checkOverloaded(currentLoad: Int)(implicit
      loggingContext: ErrorLoggingContext
  ): Option[SubmissionResult] = {
    metrics.dirtyRequests.updateValue(currentLoad)
    val errorO = checkNumberOfDirtyRequests(currentLoad).orElse(checkAndUpdateRate())
    (errorO, warnIfOverloadedDuring) match {
      case (_, None) =>
      // Warn on overloaded is disabled
      case (Some(_), Some(warnInterval)) =>
        // The participant is overloaded
        val now = CantonTimestamp.now()
        val overloadedDuration = now - lastSuccess.get()
        if (overloadedDuration >= warnInterval.duration) {
          // the system has been under high load for at least warnInterval
          val newLastWarning =
            lastWarning.updateAndGet(lw => if (now - lw >= warnInterval.duration) now else lw)
          if (newLastWarning == now) {
            // the last warning has been more than warnInterval in the past
            ParticipantOverloaded.Rejection(overloadedDuration).logWithContext()
          }
        }
      case (None, _) =>
        // The participant is not overloaded
        lastSuccess.set(CantonTimestamp.now())
    }

    errorO
  }

  protected def checkNumberOfDirtyRequests(
      currentLoad: Int
  )(implicit loggingContext: ErrorLoggingContext): Option[SubmissionResult] =
    resourceLimits.maxInflightValidationRequests
      .filter(currentLoad >= _.unwrap)
      .map(limit => {
        val status =
          ParticipantBackpressure
            .Rejection(
              s"too many in-flight validation requests (count: $currentLoad, limit: $limit)"
            )
            .rpcStatus()
        // Choosing SynchronousReject instead of Overloaded, because that allows us to specify a custom error message.
        SubmissionResult.SynchronousError(status)
      })

  protected def checkAndUpdateRate()(implicit
      loggingContext: ErrorLoggingContext
  ): Option[SubmissionResult]

  def resourceLimits: ResourceLimits

  def writeResourceLimits(limits: ResourceLimits)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  def refreshCache()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]
}

object ResourceManagementService {
  class CommunityResourceManagementService(
      override val warnIfOverloadedDuring: Option[NonNegativeFiniteDuration],
      override val metrics: ParticipantMetrics,
  ) extends ResourceManagementService {

    override protected def checkAndUpdateRate()(implicit
        loggingContext: ErrorLoggingContext
    ): Option[SubmissionResult] =
      None

    /** The community edition only supports a fixed configuration that cannot be changed.
      */
    override def resourceLimits: ResourceLimits = ResourceLimits.community

    override def writeResourceLimits(limits: ResourceLimits)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Unit] =
      FutureUnlessShutdown.failed(
        StaticGrpcServices.notSupportedByCommunityStatus.asRuntimeException()
      )

    override def refreshCache()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
      FutureUnlessShutdown.unit
  }
}
