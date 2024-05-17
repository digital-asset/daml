// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block

import cats.data.EitherT
import com.digitalasset.canton.domain.block.{RawLedgerBlock, SequencerDriverHealthStatus}
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer.SignedOrderingRequest
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  SendAsyncError,
  SignedContent,
}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.ServerServiceDefinition
import org.apache.pekko.stream.KillSwitch
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

/** The interface to the ordering functionality for a block sequencer.
  *
  * It is not BFT-specific, but an implementation may be Byzantine Fault tolerant with
  * respect to the ordering functionality.
  *
  * BFT reads and writes require, however client cooperation: if `f` is the maximum number of
  * tolerated faulty sequencer nodes, then a BFT read requires reading `f+1` identical streams
  * and BFT writes require either writing to `f+1` sequencers or writing to a single sequencer
  * and be prepared to retry on another sequencer if a BFT read for the corresponding event fails.
  */
trait BlockSequencerOps extends AutoCloseable {

  /** Delivers a stream of blocks starting with `firstBlockHeight` (if specified in the factory call)
    * or the first serveable block.
    *
    * Honest and correct implementations must ensure the following:
    *
    * - If `firstBlockHeight` refers to a block whose sequencing number the sequencer node has not yet observed,
    *   returns a source that will eventually serve that block when it gets created.
    * - This method can be called only once per instance, so implementations do not have to try to create separate
    *   sources on every call to this method: it is acceptable to for the implementation to have one internal source
    *   and just return it.
    * - The call must succeed if an earlier (across instances and restarts) call to `subscribe` delivered a block
    *   with height `firstBlockHeight` unless the block has been pruned in between, in which case it fails.
    * - All sequencer nodes of a domain must produce exactly the same stream of blocks even across restarts,
    *   unless they have been pruned.
    * - Block heights must be consecutive and start at 0.
    * - If [[BlockSequencerFactory.orderingTimeFixMode]] is [[BlockSequencerFactory.OrderingTimeFixMode.ValidateOnly]],
    *   timestamps must be strictly monotonically increasing.
    * - A successful [[send]] does not currently ensure that a corresponding event will appear on the stream; if
    *   at-least-once or exactly-once semantics are needed, they must be implemented above this interface by observing
    *   the stream and potentially re-submitting on timeout and/or de-duplicating. The timeout can be freely chosen;
    *   a sensible choice is re-submitting when an event corresponding to the submission has not been read back
    *   within the maximum sequencing time, but re-submitting earlier than that is also possible, although it requires
    *   to be prepared to handle potential duplicates.
    *   In practice, it is expected that an implementation makes a reasonable effort to avoid lost submissions, i.e.,
    *   the occasions in which a submission is lost, which can be implementation-specific, should be infrequent.
    *   In the future, however, in order to guarantee system availability, some degree of "at-least-once" semantics
    *   (e.g., if submitted to at least `f+1` sequencer nodes) may be required to be provided.
    */
  def subscribe()(implicit
      traceContext: TraceContext
  ): Source[RawLedgerBlock, KillSwitch]

  /** Orders a submission.
    * Results in a [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent.Send]].
    * There's a double [[com.digitalasset.canton.sequencing.protocol.SignedContent]] wrapping because
    * the outer signature is the sequencer's, and the inner signature is the sender's.
    * The sequencer signature may be used by the implementation to ensure that the submission originates from the
    * expected sequencer node. This may be necessary if the implementation is split across multiple processes.
    */
  def send(signedSubmission: SignedOrderingRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncError, Unit]

  /** Orders an event reception acknowledgement.
    * Results in a [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent.Acknowledgment]].
    */
  def acknowledge(signedAcknowledgeRequest: SignedContent[AcknowledgeRequest])(implicit
      traceContext: TraceContext
  ): Future[Unit]

  def health(implicit
      traceContext: TraceContext
  ): Future[SequencerDriverHealthStatus]

  def adminServices: Seq[ServerServiceDefinition] = Seq.empty
}
