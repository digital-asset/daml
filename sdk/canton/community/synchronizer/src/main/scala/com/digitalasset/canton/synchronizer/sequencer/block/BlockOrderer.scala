// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block

import cats.data.EitherT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  SendAsyncError,
  SignedContent,
}
import com.digitalasset.canton.synchronizer.block.{RawLedgerBlock, SequencerDriverHealthStatus}
import com.digitalasset.canton.synchronizer.sequencer.Sequencer.SignedOrderingRequest
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.ServerServiceDefinition
import org.apache.pekko.stream.KillSwitch
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

import BlockSequencerFactory.OrderingTimeFixMode

/** The interface to the ordering functionality for a block sequencer.
  *
  * It is not BFT-specific, but an implementation may be Byzantine Fault tolerant with
  * respect to the ordering functionality.
  *
  * BFT reads and writes require, however client cooperation: if `f` is the maximum number of
  * tolerated faulty sequencer nodes, then a BFT read requires reading `f+1` streams
  * and BFT writes require either writing to `f+1` sequencers or writing to a single sequencer
  * and be prepared to retry on another sequencer if a BFT read for the corresponding event fails.
  */
trait BlockOrderer extends AutoCloseable {

  /** The height of the first block to be returned by `subscribe`. */
  def firstBlockHeight: Long

  /** Determines if block timestamps emitted by `subscribe` are strictly monotonically increasing. */
  def orderingTimeFixMode: OrderingTimeFixMode

  /** Delivers a stream of blocks.
    *
    * Honest and correct implementations must ensure the following:
    *
    * - If `firstBlockHeight` refers to a block whose sequencing number the sequencer node has not yet observed,
    *   returns a source that will eventually serve that block when it gets created.
    * - This method can be called only once per instance, so implementations do not have to try to create separate
    *   sources on every call to this method: it is acceptable for the implementation to have one internal source
    *   and just return it.
    * - The call must succeed if an earlier (across instances and restarts) call to `subscribe` delivered a block
    *   with height `firstBlockHeight` unless the block has been pruned in between, in which case it fails.
    * - Block heights must be consecutive and start at `firstBlockHeight`.
    * - For a given block height, all sequencer nodes of a synchronizer must emit the same block for that height.
    * - Every submission that has been sent to an honest node through `send` will be included in the output stream on a best effort basis.
    *   That means, the output stream normally contains every submission, but submissions may sometimes be dropped due to high load, crashes, etc...
    * - A submission may occur more than once in the output stream, as malicious sequencer nodes may replay requests.
    * - An event in the output can not necessarily be parsed to [[com.digitalasset.canton.synchronizer.block.LedgerBlockEvent]].
    *   If they can be parsed, the embedded signatures are not necessarily valid.
    */
  def subscribe()(implicit
      traceContext: TraceContext
  ): Source[RawLedgerBlock, KillSwitch]

  /** Orders a submission.
    * If the sequencer node is honest, this normally results in a [[block.RawLedgerBlock.RawBlockEvent.Send]].
    * In exceptional cases (crashes, high load, ...), a sequencer may drop submissions.
    * There's a double [[com.digitalasset.canton.sequencing.protocol.SignedContent]] wrapping because
    * the outer signature is the sequencer's, and the inner signature is the sender's.
    * The sequencer signature may be used by the implementation to ensure that the submission originates from the
    * expected sequencer node. This may be necessary if the implementation is split across multiple processes.
    */
  def send(signedOrderingRequest: SignedOrderingRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncError, Unit]

  /** Orders an event reception acknowledgement.
    * If the sequencer node is honest, this normally results in a [[block.RawLedgerBlock.RawBlockEvent.Acknowledgment]].
    * In exceptional cases (crashes, high load, ...), a sequencer may drop acknowledgements.
    */
  def acknowledge(signedAcknowledgeRequest: SignedContent[AcknowledgeRequest])(implicit
      traceContext: TraceContext
  ): Future[Unit]

  def health(implicit
      traceContext: TraceContext
  ): Future[SequencerDriverHealthStatus]

  def adminServices: Seq[ServerServiceDefinition] = Seq.empty

  /** Provides information for the [[SequencerSnapshot.additional]]
    * field, which can be used for onboarding.
    */
  def sequencerSnapshotAdditionalInfo(
      timestamp: CantonTimestamp
  ): EitherT[Future, SequencerError, Option[v30.BftSequencerSnapshotAdditionalInfo]]
}
