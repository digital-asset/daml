// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.Eval
import com.digitalasset.canton.config.RequireTypes.NonNegativeNumeric
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.error.LedgerApiErrors.ParticipantBackpressure
import com.digitalasset.canton.ledger.participant.state.SubmissionResult
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.ParticipantOverloaded
import com.digitalasset.canton.participant.store.ParticipantSettingsStore
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.RateLimiter

import java.util.concurrent.atomic.AtomicReference
import scala.math.Ordered.orderingToOrdered

class ResourceManagementService(
    store: Eval[ParticipantSettingsStore],
    val warnIfOverloadedDuring: Option[NonNegativeFiniteDuration],
    val metrics: ParticipantMetrics,
) {

  private val lastSuccess: AtomicReference[CantonTimestamp] =
    new AtomicReference[CantonTimestamp](CantonTimestamp.now())
  private val lastWarning: AtomicReference[CantonTimestamp] =
    new AtomicReference[CantonTimestamp](CantonTimestamp.now())

  metrics
    .registerMaxInflightValidationRequest(() =>
      resourceLimits.maxInflightValidationRequests.map(_.unwrap)
    )
    .discard

  private val rateLimiterRef: AtomicReference[Option[RateLimiter]] = new AtomicReference(
    createLimiter(store.value.settings.resourceLimits)
  )

  private def createLimiter(limits: ResourceLimits): Option[RateLimiter] =
    limits.maxSubmissionRate.map(x =>
      new RateLimiter(
        NonNegativeNumeric.tryCreate(x.value.toDouble),
        limits.maxSubmissionBurstFactor,
      )
    )

  protected def checkNumberOfInflightValidationRequests(
      currentLoad: Int
  )(implicit loggingContext: ErrorLoggingContext): Option[SubmissionResult] =
    resourceLimits.maxInflightValidationRequests
      .filter(currentLoad >= _.unwrap)
      .map { limit =>
        val status =
          ParticipantBackpressure
            .Rejection(
              s"too many in-flight validation requests (count: $currentLoad, limit: $limit)"
            )
            .rpcStatus()
        // Choosing SynchronousReject instead of Overloaded, because that allows us to specify a custom error message.
        SubmissionResult.SynchronousError(status)
      }

  protected def checkAndUpdateRate()(implicit
      loggingContext: ErrorLoggingContext
  ): Option[SubmissionResult] =
    rateLimiterRef
      .get()
      .flatMap(limiter =>
        if (limiter.checkAndUpdateRate()) None
        else {
          val status =
            ParticipantBackpressure
              .Rejection(
                s"the amount of commands submitted to this participant exceeds the submission rate limit of ${limiter.maxTasksPerSecond} submissions per second with bursts of up to ${Math
                    .ceil(limiter.maxBurst)
                    .toInt} submissions"
              )
              .rpcStatus()
          // Choosing SynchronousReject instead of Overloaded, because that allows us to specify a custom error message.
          Some(SubmissionResult.SynchronousError(status))
        }
      )

  def resourceLimits: ResourceLimits = store.value.settings.resourceLimits

  def writeResourceLimits(
      limits: ResourceLimits
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    rateLimiterRef.set(createLimiter(limits))
    store.value.writeResourceLimits(limits)
  }

  def refreshCache()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    store.value.refreshCache()

  def checkOverloaded(currentLoad: Int)(implicit
      loggingContext: ErrorLoggingContext
  ): Option[SubmissionResult] = {
    metrics.inflightValidationRequests.updateValue(currentLoad)
    val errorO = checkNumberOfInflightValidationRequests(currentLoad).orElse(checkAndUpdateRate())
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
}
