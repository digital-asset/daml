// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.store

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.CommitMode
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry

import java.util.UUID
import scala.concurrent.Future

/** A subset of the sequencer storage methods required by the [[SequencerWriter]] with
  * convenience methods to avoid passing the `instanceIndex` everywhere.
  */
trait SequencerWriterStore extends AutoCloseable {
  protected[store] val store: SequencerStore
  def isActive: Boolean
  val instanceIndex: Int

  /** Validate that the commit mode of a session is inline with the configured expected commit mode.
    * Return a human readable message about the mismatch in commit modes if not.
    */
  def validateCommitMode(configuredCommitMode: CommitMode)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit] =
    store.validateCommitMode(configuredCommitMode)

  /** Lookup an existing member id for the given member.
    * Return [[scala.None]] if no id exists.
    */
  def lookupMember(member: Member)(implicit
      traceContext: TraceContext
  ): Future[Option[RegisteredMember]] =
    store.lookupMember(member)

  /** Save a series of payloads to the store.
    * Is up to the caller to determine a reasonable batch size and no batching is done within the store.
    */
  def savePayloads(payloads: NonEmpty[Seq[Payload]], instanceDiscriminator: UUID)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SavePayloadsError, Unit] =
    store.savePayloads(payloads, instanceDiscriminator)

  /** Save a series of events to the store.
    * Callers should determine batch size. No batching is done within the store.
    * Callers MUST ensure that event-ids are unique otherwise stores will throw (likely a constraint violation).
    */
  def saveEvents(events: NonEmpty[Seq[Sequenced[PayloadId]]])(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    store.saveEvents(instanceIndex, events)

  def resetWatermark(ts: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SaveWatermarkError, Unit] =
    store.resetWatermark(instanceIndex, ts)

  /** Write the watermark that we promise not to write anything earlier than.
    * Does not indicate that there is an event written by this sequencer for this timestamp as there may be no activity
    * at the sequencer, but updating the timestamp allows the sequencer to indicate that it's still alive.
    * Return an error if we find our sequencer is offline.
    */
  def saveWatermark(ts: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SaveWatermarkError, Unit] =
    store.saveWatermark(instanceIndex, ts)

  /** Read the watermark for this sequencer and its online/offline status.
    * Currently only used for testing.
    */
  def fetchWatermark(maxRetries: Int = retry.Forever)(implicit
      traceContext: TraceContext
  ): Future[Option[Watermark]] =
    store.fetchWatermark(instanceIndex, maxRetries)

  /** Mark the sequencer as online and return a timestamp for when this sequencer can start safely producing events.
    * @param now Now according to this sequencer's clock which will be used if it is ahead of the lowest available
    *            timestamp from other sequencers.
    */
  def goOnline(now: CantonTimestamp)(implicit traceContext: TraceContext): Future[CantonTimestamp] =
    store.goOnline(instanceIndex, now)

  /** Flag that we're going offline (likely due to a shutdown) */
  def goOffline()(implicit traceContext: TraceContext): Future[Unit] =
    store.goOffline(instanceIndex)

  /** Delete all events that are ahead of the watermark of this sequencer.
    * These events will not have been read and should be removed before returning the sequencer online.
    * Should not be called alongside updating the watermark for this sequencer and only while the sequencer is offline.
    * Returns the watermark that was used for the deletion.
    */
  def deleteEventsPastWatermark()(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] =
    store.deleteEventsPastWatermark(instanceIndex)

}

/** Writer store that just passes directly through to the underlying store using the provided instance index. */
private[store] class SimpleSequencerWriterStore(
    override val instanceIndex: Int,
    override protected[store] val store: SequencerStore,
) extends SequencerWriterStore {
  override val isActive: Boolean = true // nothing fancy to see here
  override def close(): Unit = ()
}

object SequencerWriterStore {

  /** Just reuse the general store assuming that we're the only writer to the database */
  def singleInstance(store: SequencerStore): SequencerWriterStore =
    new SimpleSequencerWriterStore(0, store)
}
