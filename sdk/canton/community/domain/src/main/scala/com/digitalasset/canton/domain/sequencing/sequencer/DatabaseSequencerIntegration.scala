// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.UnlessShutdown.Outcome
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.sequencing.protocol.SendAsyncError
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.parallelFuture
import com.digitalasset.canton.util.retry.NoExceptionRetryPolicy
import com.digitalasset.canton.util.{MonadUtil, retry}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

/** This trait defines the interface for BlockSequencer's BlockUpdateGenerator to use on DatabaseSequencer
  * in order to accept submissions and serve events from it
  */
trait SequencerIntegration {
  def blockSequencerAcknowledge(acknowledgements: Map[Member, CantonTimestamp])(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Unit]

  def blockSequencerWrites(
      orderedOutcomes: Seq[SubmissionOutcome]
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Unit]
}

object SequencerIntegration {
  val Noop: SequencerIntegration = new SequencerIntegration {
    override def blockSequencerAcknowledge(acknowledgements: Map[Member, CantonTimestamp])(implicit
        executionContext: ExecutionContext,
        traceContext: TraceContext,
    ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.unit

    override def blockSequencerWrites(
        orderedOutcomes: Seq[SubmissionOutcome]
    )(implicit
        executionContext: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[FutureUnlessShutdown, String, Unit] =
      EitherT.pure[FutureUnlessShutdown, String](())
  }
}

trait DatabaseSequencerIntegration extends SequencerIntegration {
  this: DatabaseSequencer =>

  private implicit val retryConditionIfOverloaded
      : retry.Success[UnlessShutdown[Either[SendAsyncError, Unit]]] =
    new retry.Success({
      // We only retry overloaded as other possible error here:
      // * Unavailable - indicates a programming bug and should not happen during normal operation
      // * ShuttingDown - should not be retried as the sequencer is shutting down
      case Outcome(Left(SendAsyncError.Overloaded(_))) => false
      case _ => true
    })
  private val retryWithBackoff = retry.Backoff(
    logger = logger,
    flagCloseable = this,
    maxRetries = retry.Forever,
    // TODO(#18407): Consider making the values below configurable
    initialDelay = 10.milliseconds,
    maxDelay = 250.milliseconds,
    operationName = "block-sequencer-write-internal",
    longDescription = "Write the processed submissions into database sequencer store",
  )

  override def blockSequencerAcknowledge(acknowledgements: Map[Member, CantonTimestamp])(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Unit] =
    // TODO(#18394): Batch acknowledgements?
    performUnlessClosingF(functionFullName)(acknowledgements.toSeq.parTraverse_ {
      case (member, timestamp) =>
        this.writeAcknowledgementInternal(member, timestamp)
    })

  override def blockSequencerWrites(
      orderedOutcomes: Seq[SubmissionOutcome]
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    MonadUtil
      .sequentialTraverse_(orderedOutcomes) {
        case _: SubmissionOutcome.Discard.type =>
          EitherT.pure[FutureUnlessShutdown, String](())
        case outcome: DeliverableSubmissionOutcome =>
          EitherT(
            FutureUnlessShutdown(
              retryWithBackoff(
                this
                  .blockSequencerWriteInternal(outcome)(outcome.submissionTraceContext)
                  .value
                  .unwrap,
                NoExceptionRetryPolicy,
              )
            )
          )
            .leftMap(_.toString)
      }
}
