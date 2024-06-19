// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.data.EitherT
import cats.syntax.parallel.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.parallelFuture
import com.digitalasset.canton.util.MonadUtil

import scala.concurrent.{ExecutionContext, Future}

/** This trait defines the interface for BlockSequencer's BlockUpdateGenerator to use on DatabaseSequencer
  * in order to accept submissions and serve events from it
  */
trait SequencerIntegration {
  def blockSequencerAcknowledge(acknowledgements: Map[Member, CantonTimestamp])(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Unit]

  def blockSequencerWrites(
      orderedOutcomes: Seq[SubmissionOutcome]
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, Unit]
}

object SequencerIntegration {
  val Noop: SequencerIntegration = new SequencerIntegration {
    override def blockSequencerAcknowledge(acknowledgements: Map[Member, CantonTimestamp])(implicit
        executionContext: ExecutionContext,
        traceContext: TraceContext,
    ): Future[Unit] = Future.unit

    override def blockSequencerWrites(
        orderedOutcomes: Seq[SubmissionOutcome]
    )(implicit
        executionContext: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[Future, String, Unit] =
      EitherT.pure[Future, String](())
  }
}

trait DatabaseSequencerIntegration extends SequencerIntegration {
  this: DatabaseSequencer =>
  override def blockSequencerAcknowledge(acknowledgements: Map[Member, CantonTimestamp])(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Unit] =
    // TODO(#18394): Batch acknowledgements?
    acknowledgements.toSeq.parTraverse_ { case (member, timestamp) =>
      this.writeAcknowledgementInternal(member, timestamp)
    }

  override def blockSequencerWrites(
      orderedOutcomes: Seq[SubmissionOutcome]
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, Unit] =
    // TODO(#18394): Implement batch db write for BS sequencer writes, align with DBS batching
    MonadUtil
      .sequentialTraverse_(orderedOutcomes) {
        case _: SubmissionOutcome.Discard.type =>
          EitherT.pure[Future, String](())
        case outcome: DeliverableSubmissionOutcome =>
          this
            .blockSequencerWriteInternal(outcome)(outcome.submissionTraceContext)
            .leftMap(_.toString)
      }
}
