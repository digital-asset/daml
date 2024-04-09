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
  def blockSequencerRegisterMembers(members: Map[Member, CantonTimestamp])(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Unit]

  def blockSequencerWrites(
      orderedOutcomes: Seq[SubmissionRequestOutcome]
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, Unit]
}

object SequencerIntegration {
  val Noop: SequencerIntegration = new SequencerIntegration {
    override def blockSequencerRegisterMembers(members: Map[Member, CantonTimestamp])(implicit
        executionContext: ExecutionContext,
        traceContext: TraceContext,
    ): Future[Unit] = Future.unit

    override def blockSequencerWrites(
        orderedOutcomes: Seq[SubmissionRequestOutcome]
    )(implicit
        executionContext: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[Future, String, Unit] =
      EitherT.pure[Future, String](())
  }
}

trait DatabaseSequencerIntegration extends SequencerIntegration {
  this: DatabaseSequencer =>
  override def blockSequencerRegisterMembers(members: Map[Member, CantonTimestamp])(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Unit] =
    // TODO(#18394): Implement batch db write for member registration
    members.toSeq.parTraverse_ { case (member, timestamp) =>
      for {
        isRegistered <- this.isRegistered(member)
        _ <-
          if (!isRegistered) {
            this.registerMemberInternal(member, timestamp)
          } else {
            Future.unit
          }
      } yield ()
    }

  override def blockSequencerWrites(
      orderedOutcomes: Seq[SubmissionRequestOutcome]
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, Unit] =
    // TODO(#18394): Implement batch db write for BS sequencer writes, align with DBS batching
    MonadUtil
      .sequentialTraverse_(orderedOutcomes) { outcome =>
        this
          .blockSequencerWriteInternal(outcome)
          .leftMap(_.toString)
      }
}
