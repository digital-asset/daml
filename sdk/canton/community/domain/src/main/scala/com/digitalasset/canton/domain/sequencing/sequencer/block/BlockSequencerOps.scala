// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block

import cats.data.EitherT
import com.digitalasset.canton.domain.block.{RawLedgerBlock, SequencerDriverHealthStatus}
import com.digitalasset.canton.domain.sequencing.sequencer.errors.{
  RegisterMemberError,
  SequencerWriteError,
}
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  SendAsyncError,
  SignedContent,
  SubmissionRequest,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.ServerServiceDefinition
import org.apache.pekko.stream.KillSwitch
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

trait BlockSequencerOps extends AutoCloseable {

  /** Delivers a stream of blocks starting with `firstBlockHeight` (if specified in the factory call)
    * or the first block serveable.
    * Block heights must be consecutive.
    *
    * If `firstBlockHeight` refers to a block whose sequencing number the sequencer node has not yet observed,
    * returns a source that will eventually serve that block when it gets created.
    *
    * Must succeed if an earlier call to `subscribe` delivered a block with height `firstBlockHeight`
    * unless the block has been pruned in between, in which case it fails
    *
    * Must succeed if an earlier call to `subscribe` delivered a block with height `firstBlockHeight`
    * unless the block has been pruned in between.
    *
    * This method will be called only once, so implementations do not have to try to create separate sources
    * on every call to this method. It is acceptable to for the driver to have one internal source and just return
    * it here.
    */
  def subscribe()(implicit
      traceContext: TraceContext
  ): Source[RawLedgerBlock, KillSwitch]

  /** Send a submission request.
    * Results in a [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent.Send]].
    */
  def send(signedSubmission: SignedContent[SubmissionRequest])(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncError, Unit]

  /** Register the given member.
    * Results in a [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent.AddMember]].
    */
  def register(member: Member)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerWriteError[RegisterMemberError], Unit]

  def acknowledge(signedAcknowledgeRequest: SignedContent[AcknowledgeRequest])(implicit
      traceContext: TraceContext
  ): Future[Unit]

  def health(implicit
      traceContext: TraceContext
  ): Future[SequencerDriverHealthStatus]

  def adminServices: Seq[ServerServiceDefinition] = Seq.empty
}
