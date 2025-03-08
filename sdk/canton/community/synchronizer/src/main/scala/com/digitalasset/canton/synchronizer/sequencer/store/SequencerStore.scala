// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.store

import cats.Order.*
import cats.data.EitherT
import cats.kernel.Order
import cats.syntax.either.*
import cats.syntax.order.*
import cats.syntax.parallel.*
import cats.{Functor, Show}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{CachingConfigs, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  ClosedEnvelope,
  MessageId,
  SequencedEvent,
}
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.synchronizer.sequencer.*
import com.digitalasset.canton.synchronizer.sequencer.PruningError.UnsafePruningPoint
import com.digitalasset.canton.synchronizer.sequencer.store.SequencerStore.SequencerPruningResult
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.{HasTraceContext, TraceContext, Traced}
import com.digitalasset.canton.util.EitherTUtil.condUnitET
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{BytesUnit, ErrorUtil, retry}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{ProtoDeserializationError, SequencerCounter, checked}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import com.google.rpc.status.Status
import slick.jdbc.{GetResult, SetParameter}

import java.util.UUID
import scala.annotation.tailrec
import scala.collection.immutable.SortedSet
import scala.concurrent.ExecutionContext
import scala.math.Numeric.Implicits.*

/** In the sequencer database we use integers to represent members. Wrap this in the APIs to not
  * confuse with other numeric types.
  */
final case class SequencerMemberId(private val id: Int) extends PrettyPrinting {
  def unwrap: Int = id

  override protected def pretty: Pretty[SequencerMemberId] = prettyOfParam(_.id)
}

object SequencerMemberId {
  implicit val sequencerMemberIdOrdering: Ordering[SequencerMemberId] =
    Ordering.by[SequencerMemberId, Int](_.id)
  implicit val sequencerMemberIdOrder: Order[SequencerMemberId] = fromOrdering(
    sequencerMemberIdOrdering
  )

  implicit val setSequencerMemberIdParameter: SetParameter[SequencerMemberId] = (v, pp) =>
    pp.setInt(v.id)
  implicit val getSequencerMemberIdResult: GetResult[SequencerMemberId] =
    GetResult(_.nextInt()).andThen(SequencerMemberId(_))
  implicit val setSequencerMemberIdParameterO: SetParameter[Option[SequencerMemberId]] = (v, pp) =>
    pp.setIntOption(v.map(_.id))
  implicit val getSequencerMemberIdResultO: GetResult[Option[SequencerMemberId]] =
    GetResult(_.nextIntOption()).andThen(_.map(SequencerMemberId(_)))
  implicit val sequencerMemberIdShow: Show[SequencerMemberId] =
    Show.show[SequencerMemberId](_.toString)
}

sealed trait IdOrPayload

/** Identifier for a payload. Should ideally be unique however this will be validated in
  * [[SequencerStore.savePayloads]]. Is expected id is a timestamp in microseconds.
  */
final case class PayloadId(private val id: CantonTimestamp)
    extends IdOrPayload
    with PrettyPrinting {
  def unwrap: CantonTimestamp = id

  override protected def pretty: Pretty[PayloadId] = prettyOfClass(
    unnamedParam(_.id)
  )

}

object PayloadId {
  implicit def payloadIdSetParameter(implicit
      tsSetParameter: SetParameter[CantonTimestamp]
  ): SetParameter[PayloadId] =
    (payloadId, pp) => tsSetParameter(payloadId.unwrap, pp)
  implicit def payloadIdGetResult(implicit
      tsGetResult: GetResult[CantonTimestamp]
  ): GetResult[PayloadId] =
    tsGetResult.andThen(PayloadId(_))
  implicit def payloadIdOptionSetParameter(implicit
      tsSetParameterO: SetParameter[Option[CantonTimestamp]]
  ): SetParameter[Option[PayloadId]] =
    (payloadIdO, pp) => tsSetParameterO(payloadIdO.map(_.unwrap), pp)
  implicit def payloadIdOptionGetResult(implicit
      tsGetResultO: GetResult[Option[CantonTimestamp]]
  ): GetResult[Option[PayloadId]] =
    tsGetResultO.andThen(_.map(PayloadId(_)))
}

sealed trait Payload extends IdOrPayload

/** Payload with a assigned id and content as bytes */
final case class BytesPayload(id: PayloadId, content: ByteString) extends Payload {
  def decodeBatchAndTrim(
      protocolVersion: ProtocolVersion,
      member: Member,
  ): Batch[ClosedEnvelope] = {
    val fullBatch = Batch
      .fromByteString(protocolVersion, content)
      .valueOr(err => throw new DbDeserializationException(err.toString))
    Batch.trimForMember(fullBatch, member)
  }
}

final case class FilteredBatch(id: PayloadId, batch: Batch[ClosedEnvelope]) extends Payload

/** Sequencer events in a structure suitable for persisting in our events store. The payload type is
  * parameterized to allow specifying either a full payload or just a id referencing a payload.
  */
sealed trait StoreEvent[+PayloadReference] extends HasTraceContext {

  val sender: SequencerMemberId

  /** Who gets notified of the event once it is successfully sequenced */
  val notifies: WriteNotification

  /** Description of the event to be used in logs */
  val description: String

  def messageId: MessageId

  /** All members that should receive (parts of) this event */
  def members: NonEmpty[Set[SequencerMemberId]]

  def map[P](f: PayloadReference => P): StoreEvent[P]

  def payloadO: Option[PayloadReference]

  /** The timestamp of the snapshot to be used for determining the signing key of this event,
    * resolving group addresses, and for checking signatures on envelopes, if absent, sequencing
    * time will be used instead. Absent on errors.
    */
  def topologyTimestampO: Option[CantonTimestamp]

  /** If traffic management is enabled, there should always be traffic information. The information
    * might be discarded later though, in case the event is being processed as part of a
    * subscription for any of the recipients that isn't the sender.
    */
  def trafficReceiptO: Option[TrafficReceipt]
}

final case class ReceiptStoreEvent(
    override val sender: SequencerMemberId,
    messageId: MessageId,
    override val topologyTimestampO: Option[CantonTimestamp],
    override val traceContext: TraceContext,
    override val trafficReceiptO: Option[TrafficReceipt],
) extends StoreEvent[Nothing] {
  override val notifies: WriteNotification = WriteNotification.Members(SortedSet(sender))

  override val description: String = show"receipt[message-id:$messageId]"

  override val members: NonEmpty[Set[SequencerMemberId]] = NonEmpty(Set, sender)

  override def map[P](f: Nothing => P): StoreEvent[P] = this

  override def payloadO: Option[Nothing] = None
}

/** Structure for storing a deliver events.
  * @param members
  *   should include the sender and event recipients as they all will read the event
  * @param topologyTimestampO
  *   The timestamp of the snapshot to be used for determining the signing key of this event,
  *   resolving group addresses, and for checking signatures on envelopes [[scala.None]] means that
  *   the sequencing timestamp should be used.
  */
final case class DeliverStoreEvent[P](
    override val sender: SequencerMemberId,
    messageId: MessageId,
    override val members: NonEmpty[SortedSet[SequencerMemberId]],
    payload: P,
    override val topologyTimestampO: Option[CantonTimestamp],
    override val traceContext: TraceContext,
    override val trafficReceiptO: Option[TrafficReceipt],
) extends StoreEvent[P] {
  override lazy val notifies: WriteNotification = WriteNotification.Members(members)

  override val description: String = show"deliver[message-id:$messageId]"

  override def map[P2](f: P => P2): StoreEvent[P2] =
    copy(payload = f(payload))

  override def payloadO: Option[P] = Some(payload)
}

object DeliverStoreEvent {

  /** Typically in deliver events the recipients are just the recipients of the batch. However in
    * our store we want all members that will read the event to be queryable in a single collection
    * to efficiently implement as a filter at the database. So ensure that the sender is also
    * included in this collection as they will receive a deliver event as a receipt even if they
    * aren't receiving anything from the batch.
    */
  def ensureSenderReceivesEvent(
      sender: SequencerMemberId,
      messageId: MessageId,
      members: Set[SequencerMemberId],
      payload: BytesPayload,
      topologyTimestampO: Option[CantonTimestamp],
      trafficReceiptO: Option[TrafficReceipt],
  )(implicit traceContext: TraceContext): DeliverStoreEvent[BytesPayload] = {
    // ensure that sender is a recipient
    val recipientsWithSender = NonEmpty(SortedSet, sender, members.toSeq*)
    DeliverStoreEvent(
      sender,
      messageId,
      recipientsWithSender,
      payload,
      topologyTimestampO,
      traceContext,
      trafficReceiptO,
    )
  }
}

final case class DeliverErrorStoreEvent(
    override val sender: SequencerMemberId,
    messageId: MessageId,
    error: Option[ByteString],
    override val traceContext: TraceContext,
    override val trafficReceiptO: Option[TrafficReceipt],
) extends StoreEvent[Nothing] {
  override val notifies: WriteNotification = WriteNotification.Members(SortedSet(sender))
  override val description: String = show"deliver-error[message-id:$messageId]"
  override val members: NonEmpty[Set[SequencerMemberId]] = NonEmpty(Set, sender)
  override def map[P](f: Nothing => P): StoreEvent[P] = this
  override val payloadO: Option[Nothing] = None

  override val topologyTimestampO: Option[CantonTimestamp] = None
}

object DeliverErrorStoreEvent {
  def serializeError(
      status: Status,
      protocolVersion: ProtocolVersion,
  ): ByteString =
    VersionedStatus
      .create(status, protocolVersion)
      .toByteString

  def apply(
      sender: SequencerMemberId,
      messageId: MessageId,
      status: Status,
      protocolVersion: ProtocolVersion,
      traceContext: TraceContext,
      trafficReceiptO: Option[TrafficReceipt],
  ): DeliverErrorStoreEvent = {
    val serializedError =
      DeliverErrorStoreEvent.serializeError(status, protocolVersion)

    DeliverErrorStoreEvent(
      sender,
      messageId,
      Some(serializedError),
      traceContext,
      trafficReceiptO,
    )
  }

  def fromByteString(
      serializedErrorO: Option[ByteString],
      protocolVersion: ProtocolVersion,
  ): ParsingResult[Status] =
    serializedErrorO.fold[ParsingResult[Status]](
      Left(ProtoDeserializationError.FieldNotSet("error"))
    )(serializedError =>
      VersionedStatus
        .fromByteString(protocolVersion, serializedError)
        .map(_.status)
    )
}

final case class Presequenced[+E <: StoreEvent[_]](
    event: E,
    maxSequencingTimeO: Option[CantonTimestamp],
    blockSequencerTimestampO: Option[CantonTimestamp] = None,
) extends HasTraceContext {

  def map[F <: StoreEvent[_]](fn: E => F): Presequenced[F] =
    this.copy(event = fn(event))

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def traverse[F[_], E2 <: StoreEvent[_]](fn: E => F[E2])(implicit
      F: Functor[F]
  ): F[Presequenced[E2]] = F.map(fn(event)) { newEvent =>
    if (event eq newEvent) this.asInstanceOf[Presequenced[E2]]
    else Presequenced(newEvent, maxSequencingTimeO)
  }

  override def traceContext: TraceContext = event.traceContext
}

object Presequenced {
  def withMaxSequencingTime[E <: StoreEvent[_]](
      event: E,
      maxSequencingTime: CantonTimestamp,
      blockSequencerTimestampO: Option[CantonTimestamp],
  ): Presequenced[E] =
    Presequenced(event, Some(maxSequencingTime), blockSequencerTimestampO)
  def alwaysValid[E <: StoreEvent[_]](
      event: E,
      blockSequencerTimestamp: Option[CantonTimestamp] = None,
  ): Presequenced[E] = Presequenced(event, None, blockSequencerTimestamp)
}

/** Wrapper to assign a timestamp to a event. Useful to structure this way as events are only
  * timestamped right before they are persisted (this is effectively the "sequencing" step). Before
  * this point the sequencer component is free to reorder incoming events.
  */
final case class Sequenced[+P](timestamp: CantonTimestamp, event: StoreEvent[P])
    extends HasTraceContext {
  override def traceContext: TraceContext = event.traceContext

  def map[A](fn: P => A): Sequenced[A] = copy(event = event.map(fn))
}

/** Checkpoint a sequencer subscription can be reinitialized from.
  *
  * @param counter
  *   The sequencer counter associated to the event with the given timestamp.
  * @param timestamp
  *   The timestamp of the event with the given sequencer counter.
  * @param latestTopologyClientTimestamp
  *   The latest timestamp before or at `timestamp` at which an event was created from a batch that
  *   contains an envelope addressed to the topology client used by the SequencerReader.
  */
final case class CounterCheckpoint(
    counter: SequencerCounter,
    timestamp: CantonTimestamp,
    latestTopologyClientTimestamp: Option[CantonTimestamp],
) extends PrettyPrinting {

  override protected def pretty: Pretty[CounterCheckpoint] = prettyOfClass(
    param("counter", _.counter),
    param("timestamp", _.timestamp),
    paramIfDefined("latest topology client timestamp", _.latestTopologyClientTimestamp),
  )
}

object CounterCheckpoint {

  /** We care very little about the event itself and just need the counter and timestamp */
  def apply(
      event: SequencedEvent[_],
      latestTopologyClientTimestamp: Option[CantonTimestamp],
  ): CounterCheckpoint =
    CounterCheckpoint(event.counter, event.timestamp, latestTopologyClientTimestamp)

  implicit def getResultCounterCheckpoint: GetResult[CounterCheckpoint] = GetResult { r =>
    val counter = r.<<[SequencerCounter]
    val timestamp = r.<<[CantonTimestamp]
    val latestTopologyClientTimestamp = r.<<[Option[CantonTimestamp]]
    CounterCheckpoint(counter, timestamp, latestTopologyClientTimestamp)
  }
}

sealed trait SavePayloadsError
object SavePayloadsError {

  /** We found an existing payload with the same key
    * @param payloadId
    *   the payload insert that failed
    * @param conflictingInstanceDiscriminator
    *   The discriminator of the instance that successfully inserted a payload with this id. The
    *   discriminator is logged at debug upon sequencer writer startup so is possible to discover
    *   which instance did the write.
    */
  final case class ConflictingPayloadId(
      payloadId: PayloadId,
      conflictingInstanceDiscriminator: UUID,
  ) extends SavePayloadsError

  /** We should have already inserted the payload but it wasn't there when reading. Likely can only
    * be caused by the payload being removed unexpectedly.
    */
  final case class PayloadMissing(payloadId: PayloadId) extends SavePayloadsError
}

sealed trait SaveCounterCheckpointError
object SaveCounterCheckpointError {

  /** We've attempted to write a counter checkpoint but found an existing checkpoint for this
    * counter with a different timestamp. This is very bad and suggests that we are serving
    * inconsistent streams to the member.
    */
  final case class CounterCheckpointInconsistent(
      existingTimestamp: CantonTimestamp,
      existingLatestTopologyClientTimestamp: Option[CantonTimestamp],
  ) extends SaveCounterCheckpointError
}

sealed trait SaveLowerBoundError
object SaveLowerBoundError {

  /** Returned if the bound we're trying to save is below any existing bound. */
  final case class BoundLowerThanExisting(
      existingBound: CantonTimestamp,
      suppliedBound: CantonTimestamp,
  ) extends SaveLowerBoundError
}

/** Time that the sequencer commits to not writing events before, and therefore it is safe to read
  * events less or equal to this timestamp.
  */
final case class Watermark(timestamp: CantonTimestamp, online: Boolean)

sealed trait SaveWatermarkError
object SaveWatermarkError {
  case object WatermarkFlaggedOffline extends SaveWatermarkError

  case object WatermarkFlaggedOnline extends SaveWatermarkError

  /** We expect that there is only a single writer for a given watermark that is not written
    * concurrently. If when checking the value we find it is not what we expect, it likely indicates
    * a configuration error causing multiple sequencer processes to be written as the same sequencer
    * node index.
    */
  final case class WatermarkUnexpectedlyChanged(message: String) extends SaveWatermarkError
}

final case class RegisteredMember(
    memberId: SequencerMemberId,
    registeredFrom: CantonTimestamp,
    enabled: Boolean,
)

/** Used for verifying what pruning is doing in tests */
@VisibleForTesting
private[canton] final case class SequencerStoreRecordCounts(
    events: Long,
    payloads: Long,
    counterCheckpoints: Long,
) {
  def -(other: SequencerStoreRecordCounts): SequencerStoreRecordCounts = SequencerStoreRecordCounts(
    events - other.events,
    payloads - other.payloads,
    counterCheckpoints - other.counterCheckpoints,
  )
}

trait ReadEvents {
  def nextTimestamp: Option[CantonTimestamp]
  def events: Seq[Sequenced[IdOrPayload]]
}

final case class ReadEventPayloads(events: Seq[Sequenced[IdOrPayload]]) extends ReadEvents {
  def nextTimestamp: Option[CantonTimestamp] = events.lastOption.map(_.timestamp)
}

/** No events found but may return the safe watermark across online sequencers to read from the next
  * time
  */
final case class SafeWatermark(nextTimestamp: Option[CantonTimestamp]) extends ReadEvents {
  def events: Seq[Sequenced[IdOrPayload]] = Seq.empty
}

/** An interface for validating members of a sequencer, i.e. that member is registered at a certain
  * time.
  */
trait SequencerMemberValidator {
  def isMemberRegisteredAt(member: Member, time: CantonTimestamp)(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[Boolean]
}

/** Persistence for the Sequencer. Writers are expected to create a [[SequencerWriterStore]] which
  * may delegate to this underlying store through an appropriately managed storage instance.
  */
trait SequencerStore extends SequencerMemberValidator with NamedLogging with AutoCloseable {

  protected implicit val executionContext: ExecutionContext

  private val memberCache = new SequencerMemberCache(Traced.lift(lookupMemberInternal(_)(_)))

  /** Whether the sequencer store operates is used for a block sequencer or a standalone database
    * sequencer.
    */
  def blockSequencerMode: Boolean

  /** Validate that the commit mode of a session is inline with the configured expected commit mode.
    * Return a human readable message about the mismatch in commit modes if not.
    */
  def validateCommitMode(configuredCommitMode: CommitMode)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit]

  /** Register the provided member. Should be idempotent if member is already registered and return
    * the existing id.
    */
  def registerMember(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencerMemberId]

  /** Lookup an existing member id for the given member. Will return a cached value if available.
    * Return [[scala.None]] if no id exists.
    */
  final def lookupMember(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[RegisteredMember]] =
    memberCache(member)

  /** Lookup member directly without caching. */
  protected def lookupMemberInternal(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[RegisteredMember]]

  override def isMemberRegisteredAt(member: Member, time: CantonTimestamp)(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    lookupMember(member)
      .map { regMemberO =>
        val registered = regMemberO.exists(_.registeredFrom <= time)
        logger.trace(s"Checked if member $member is registered at time $time: $registered")
        registered
      }

  /** Save a series of payloads to the store. Is up to the caller to determine a reasonable batch
    * size and no batching is done within the store.
    * @param payloads
    *   the payloads to save
    * @param instanceDiscriminator
    *   a unique ephemeral value to ensure that no other sequencer instances are writing conflicting
    *   payloads without having to check the payload body
    */
  def savePayloads(payloads: NonEmpty[Seq[BytesPayload]], instanceDiscriminator: UUID)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SavePayloadsError, Unit]

  /** Save a series of events to the store. Callers should determine batch size. No batching is done
    * within the store. Callers MUST ensure that event-ids are unique as no errors will be returned
    * if a duplicate is present (for the sequencer writer see
    * [[sequencer.PartitionedTimestampGenerator]] for use with their instance index).
    */
  def saveEvents(instanceIndex: Int, events: NonEmpty[Seq[Sequenced[PayloadId]]])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Maximum memory usage by the events in the buffer before it starts dropping events. If set to
    * 0, caching is disabled and all requests go to the underlying store (e.g. database).
    */
  def bufferedEventsMaxMemory: BytesUnit

  @VisibleForTesting
  lazy val eventsBufferEnabled: Boolean = bufferedEventsMaxMemory.toLong > 0L

  protected val eventsBuffer = new EventsBuffer(bufferedEventsMaxMemory, loggerFactory)

  /** In case of single instance sequencer we can use in-memory fanout buffer for events */
  final def bufferEvents(
      events: NonEmpty[Seq[Sequenced[BytesPayload]]]
  ): Unit =
    if (eventsBufferEnabled) eventsBuffer.bufferEvents(events)

  final def resetAndPreloadBuffer()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    if (eventsBufferEnabled) preloadBufferInternal().map { _ =>
      val original = eventsBuffer.snapshot()
      val sorted = original.sortBy(_.timestamp)
      if (original != sorted)
        throw new IllegalStateException(
          "The preloaded events buffer does not contain elements in strict monotonic ascending order."
        )
    }
    else FutureUnlessShutdown.unit

  protected def preloadBufferInternal()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Flag any sequencers that have a last updated watermark on or before the given `cutoffTime` as
    * offline.
    */
  def markLaggingSequencersOffline(cutoffTime: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Reset the watermark to an earlier value, i.e. in case of working as a part of block sequencer.
    * Also sets the sequencer as offline. If current watermark value is before `ts`, it will be left
    * unchanged. If a watermark doesn't yet exist, it won't be inserted, because it would
    * effectively mean setting it to a future value.
    */
  def resetWatermark(instanceIndex: Int, ts: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SaveWatermarkError, Unit]

  /** Write the watermark that we promise not to write anything earlier than. Does not indicate that
    * there is an event written by this sequencer for this timestamp as there may be no activity at
    * the sequencer, but updating the timestamp allows the sequencer to indicate that it's still
    * alive. Return an error if we find our sequencer is offline.
    */
  def saveWatermark(instanceIndex: Int, ts: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SaveWatermarkError, Unit]

  /** Read the watermark for this sequencer and its online/offline status. Currently only used for
    * testing.
    */
  def fetchWatermark(instanceIndex: Int, maxRetries: Int = retry.Forever)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Watermark]]

  /** Return the minimum watermark across all online sequencers
    */
  def safeWatermark(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]]

  /** Flag that we're going offline (likely due to a shutdown) */
  def goOffline(instanceIndex: Int)(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit]

  /** Mark the sequencer as online and return a timestamp for when this sequencer can start safely
    * producing events.
    * @param now
    *   Now according to this sequencer's clock which will be used if it is ahead of the lowest
    *   available timestamp from other sequencers.
    */
  def goOnline(instanceIndex: Int, now: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[CantonTimestamp]

  /** Fetch the indexes of all sequencers that are currently online */
  def fetchOnlineInstances(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SortedSet[Int]]

  /** Internal non-buffered implementation of `readEvents`.
    */
  protected def readEventsInternal(
      memberId: SequencerMemberId,
      fromExclusiveO: Option[CantonTimestamp],
      limit: Int,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[ReadEvents]

  def readPayloads(
      payloadIds: Seq[IdOrPayload],
      member: Member,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[PayloadId, Batch[ClosedEnvelope]]]

  /** Read all events of which a member is a recipient from the provided timestamp but no greater
    * than the earliest watermark. Passing both `member` and `memberId` to avoid a database query
    * for the lookup.
    */
  def readEvents(
      memberId: SequencerMemberId,
      member: Member,
      fromExclusiveO: Option[CantonTimestamp],
      limit: Int,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[ReadEvents] = {
    logger.debug(
      s"Reading events for member $member from timestamp $fromExclusiveO with limit $limit"
    )
    val cache = eventsBuffer.snapshot()

    fromExclusiveO match {
      case Some(fromExclusive) =>
        cache.headOption match {
          case Some(earliestEvent) if earliestEvent.timestamp <= fromExclusive =>
            // If the buffer has events that are newer than the requested timestamp, we can use the buffer
            val start = SequencerStore.binarySearch[Sequenced[BytesPayload], CantonTimestamp](
              cache,
              _.timestamp,
              fromExclusive,
            )
            val events = cache
              .slice(start, cache.size)
              .view
              .filter(_.event.members.contains(memberId))
              .take(limit)
              .toSeq

            logger.debug(
              s"Serving ${events.length} events from the buffer"
            )

            if (events.nonEmpty) {
              FutureUnlessShutdown.pure(
                ReadEventPayloads(events)
              )
            } else {
              // No events to read, advance the read watermark to the latest event's timestamp in the buffer
              // Note that if fromExclusive > cache.lastOption.timestamp, we keep the watermark unchanged
              // not to move it backwards and potentially read events twice
              FutureUnlessShutdown.pure(
                SafeWatermark(cache.lastOption.map(_.timestamp) max Some(fromExclusive))
              )
            }
          case _ =>
            logger.debug(
              s"Falling back to database access for events"
            )
            // If the buffer does not start earlier than the `fromExclusive` timestamp,
            // we cannot serve the request with the buffer only, we need to fallback to read from the database
            readEventsInternal(memberId, fromExclusiveO, limit)
        }
      case None =>
        logger.debug(
          s"Falling back to the database access for events"
        )
        // In case we start from the beginning of events, we cannot determine if we can rely on the buffer
        readEventsInternal(memberId, fromExclusiveO, limit)
    }
  }

  /** Delete all events and checkpoints that are ahead of the watermark of this sequencer. These
    * events will not have been read and should be removed before returning the sequencer online.
    * Should not be called alongside updating the watermark for this sequencer and only while the
    * sequencer is offline. Returns the watermark that was used for the deletion.
    */
  def deleteEventsAndCheckpointsPastWatermark(instanceIndex: Int)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]]

  /** Save a checkpoint that as of a certain timestamp the member has this counter value. Any future
    * subscriptions can then use this as a starting point for serving their event stream rather than
    * starting from 0.
    */
  def saveCounterCheckpoint(
      memberId: SequencerMemberId,
      checkpoint: CounterCheckpoint,
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): EitherT[FutureUnlessShutdown, SaveCounterCheckpointError, Unit]

  def saveCounterCheckpoints(
      checkpoints: Seq[(SequencerMemberId, CounterCheckpoint)]
  )(implicit
      traceContext: TraceContext,
      externalCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit]

  /** Fetch a checkpoint with a counter value less than the provided counter. */
  def fetchClosestCheckpointBefore(memberId: SequencerMemberId, counter: SequencerCounter)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CounterCheckpoint]]

  def fetchLatestCheckpoint()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]]

  def fetchEarliestCheckpointForMember(memberId: SequencerMemberId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CounterCheckpoint]]

  /** Write an acknowledgement that member has processed earlier timestamps. Only the latest
    * timestamp needs to be stored. Earlier timestamps can be overwritten. Acknowledgements of
    * earlier timestamps should be ignored.
    */
  def acknowledge(
      member: SequencerMemberId,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  /** Return the latest acknowledgements for all members */
  @VisibleForTesting
  protected[store] def latestAcknowledgements()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[SequencerMemberId, CantonTimestamp]]

  /** Build a status object representing the current state of the sequencer. */
  def status(now: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencerPruningStatus]

  /** Count records currently stored by the sequencer. Used for pruning tests. */
  @VisibleForTesting
  protected[canton] def countRecords(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencerStoreRecordCounts]

  /** Fetch the lower bound of events that can be read. Returns `None` if all events can be read. */
  def fetchLowerBound()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]]

  /** Save an updated lower bound of events that can be read. Must be equal or greater than any
    * prior set lower bound.
    * @throws java.lang.IllegalArgumentException
    *   if timestamp is lower than existing lower bound
    */
  def saveLowerBound(ts: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SaveLowerBoundError, Unit]

  /** Prevents member from sending and reading from the sequencer, and allows unread data for this
    * member to be pruned. It however won't stop any sends addressed to this member.
    */
  def disableMember(
      member: Member
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    for {
      memberIdO <- memberCache(member).map(_.map(_.memberId))
      _ <- memberIdO match {
        case Some(memberId) => disableMemberInternal(memberId)
        case None =>
          logger.warn(s"disableMember called for member $member that is not registered")
          FutureUnlessShutdown.unit
      }
      _ = memberCache.invalidate(member)
    } yield ()

  def disableMemberInternal(member: SequencerMemberId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Prune as much data as safely possible from before the given timestamp.
    * @param requestedTimestamp
    *   the timestamp that we would like to prune up to (see docs on using the pruning status and
    *   disabling members for picking this value)
    * @param status
    *   the pruning status that should be used for determining a safe to prune time for validation
    * @param payloadToEventMargin
    *   the maximum time margin between payloads and events. once we have a safe to prune timestamp
    *   we simply prune all payloads at `safeTimestamp - margin` to ensure no payloads are removed
    *   where events will remain. typically sourced from
    *   [[SequencerWriterConfig.payloadToEventMargin]].
    * @return
    *   the timestamp up to which the database sequencer has been pruned (lower than
    *   requestedTimestamp) and a human readable report on what has been removed.
    */
  def prune(
      requestedTimestamp: CantonTimestamp,
      status: SequencerPruningStatus,
      payloadToEventMargin: NonNegativeFiniteDuration,
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): EitherT[FutureUnlessShutdown, PruningError, SequencerPruningResult] = {
    val disabledClients = status.disabledClients

    logger.debug(show"Pruning Sequencer: disabled-members: ${disabledClients.members}")

    val safeTimestamp = status.safePruningTimestamp
    logger.debug(s"Safe pruning timestamp is [$safeTimestamp]")

    // generates and saves counter checkpoints for all members at the requested timestamp
    def saveRecentCheckpoints(): FutureUnlessShutdown[Unit] = for {
      checkpoints <- checkpointsAtTimestamp(requestedTimestamp)
      _ = {
        logger.debug(
          s"Saving checkpoints $checkpoints for members at timestamp $requestedTimestamp"
        )
      }
      checkpoints <- checkpoints.toList.parTraverse { case (member, checkpoint) =>
        lookupMember(member).map {
          case Some(registeredMember) => registeredMember.memberId -> checkpoint
          case _ => ErrorUtil.invalidState(s"Member $member should be registered")
        }
      }
      _ <- saveCounterCheckpoints(checkpoints)
    } yield ()

    // Setting the lower bound to this new timestamp prevents any future readers from reading before this point.
    // As we've already ensured all known enabled readers have read beyond this point this should be harmless.
    // If the existing lower bound timestamp is already above the suggested timestamp value for pruning it suggests
    // that later data has already been pruned. Can happen if an earlier timestamp is required for pruning.
    // We'll just log a info message and move forward with pruning (which likely won't remove anything).
    def updateLowerBound(timestamp: CantonTimestamp): FutureUnlessShutdown[Unit] =
      saveLowerBound(timestamp).value
        .map(_.leftMap { case SaveLowerBoundError.BoundLowerThanExisting(existing, _) =>
          logger.info(
            s"The sequencer has already been pruned up until $existing. Pruning from $requestedTimestamp will not remove any data."
          )
          () // effectively swallow
        }.merge)

    def performPruning(atBeforeExclusive: CantonTimestamp): FutureUnlessShutdown[String] =
      for {
        eventsRemoved <- pruneEvents(atBeforeExclusive)
        // purge all payloads before the point that they could be associated with these events.
        // this may leave some orphaned payloads but that's fine, they'll be pruned at a later point.
        // we do this as this approach is much quicker than looking at each event we prune and then looking up that payload
        // to delete, and also ensures payloads that may have been written for events that weren't sequenced are removed
        // (if the event was dropped due to a crash or validation issue).
        payloadsRemoved <- prunePayloads(atBeforeExclusive.minus(payloadToEventMargin.duration))
        checkpointsRemoved <- pruneCheckpoints(atBeforeExclusive)
      } yield s"Removed at least $eventsRemoved events, at least $payloadsRemoved payloads, at least $checkpointsRemoved counter checkpoints"

    for {
      _ <- condUnitET[FutureUnlessShutdown](
        requestedTimestamp <= safeTimestamp,
        UnsafePruningPoint(requestedTimestamp, safeTimestamp),
      )

      _ <- EitherT.right(saveRecentCheckpoints())
      _ <- EitherT.right(updateLowerBound(requestedTimestamp))
      description <- EitherT.right(performPruning(requestedTimestamp))

    } yield SequencerPruningResult(Some(requestedTimestamp), description)
  }

  /** Prune events before the given timestamp
    * @return
    *   a best efforts count of how many events were removed. this value can be less than the number
    *   of events actually removed if technical issues prevent a full count from being returned
    *   (e.g. with a database we may retry a delete after a connectivity issue and find that all
    *   events were successfully removed and have 0 rows removed returned).
    */
  protected[store] def pruneEvents(beforeExclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Int]

  /** Prune payloads before the given timestamp
    * @return
    *   a best efforts count of how many events were removed. this value can be less than the number
    *   of payloads actually removed if technical issues prevent a full count from being returned.
    */
  protected[store] def prunePayloads(beforeExclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Int]

  /** Prune counter checkpoints for the given member before the given timestamp.
    * @return
    *   A lower bound on the number of checkpoints removed.
    */
  protected[store] def pruneCheckpoints(beforeExclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Int]

  /** Locate a timestamp relative to the earliest available event based on a skip index starting at
    * 0. Useful to monitor the progress of pruning and for pruning in batches.
    * @return
    *   The timestamp of the (skip+1)'th event if it exists, None otherwise.
    */
  def locatePruningTimestamp(skip: NonNegativeInt)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]]

  /** The state returned here is used to initialize a separate database sequencer (that does not
    * share the same database as this one) using [[initializeFromSnapshot]] such that this new
    * sequencer has enough information (registered members, checkpoints, etc) to be able to process
    * new events from the same point as this sequencer to the same clients. This is typically used
    * by block sequencers that use the database sequencer as local storage such that they will
    * process the same events in the same order and they need to be able to spin up new block
    * sequencers from a specific point in time.
    * @return
    *   state at the given time
    */
  def readStateAtTimestamp(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencerSnapshot]

  def checkpointsAtTimestamp(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[Member, CounterCheckpoint]]

  /** Compute a counter checkpoint for every member at the requested `timestamp` and save it to the
    * store.
    */
  def recordCounterCheckpointsAtTimestamp(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext,
      externalCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit]

  def initializeFromSnapshot(initialState: SequencerInitialState)(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val snapshot = initialState.snapshot
    val lastTs = snapshot.lastTs
    for {
      memberCheckpoints <- EitherT.right(snapshot.status.members.toSeq.parTraverseFilter {
        memberStatus =>
          for {
            id <- registerMember(memberStatus.member, memberStatus.registeredAt)
            _ <-
              if (!memberStatus.enabled) disableMember(memberStatus.member)
              else FutureUnlessShutdown.unit
            _ <- memberStatus.lastAcknowledged.fold(FutureUnlessShutdown.unit)(ack =>
              acknowledge(id, ack)
            )
            counterCheckpoint =
              // Some members can be registered, but not have any events yet, so there can be no CounterCheckpoint in the snapshot
              snapshot.heads.get(memberStatus.member).map { counter =>
                (id -> CounterCheckpoint(
                  counter,
                  lastTs,
                  initialState.latestSequencerEventTimestamp,
                ))
              }
          } yield counterCheckpoint
      })
      _ <- EitherT.right(saveCounterCheckpoints(memberCheckpoints))
      _ <- saveLowerBound(lastTs).leftMap(_.toString)
      _ <- saveWatermark(0, lastTs).leftMap(_.toString)
    } yield ()
  }
}

object SequencerStore {
  def apply(
      storage: Storage,
      protocolVersion: ProtocolVersion,
      bufferedEventsMaxMemory: BytesUnit,
      bufferedEventsPreloadBatchSize: PositiveInt,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      sequencerMember: Member,
      blockSequencerMode: Boolean,
      cachingConfigs: CachingConfigs,
      overrideCloseContext: Option[CloseContext] = None,
  )(implicit executionContext: ExecutionContext): SequencerStore =
    storage match {
      case _: MemoryStorage =>
        new InMemorySequencerStore(
          protocolVersion,
          sequencerMember,
          blockSequencerMode = blockSequencerMode,
          loggerFactory,
        )
      case dbStorage: DbStorage =>
        new DbSequencerStore(
          dbStorage,
          protocolVersion,
          bufferedEventsMaxMemory,
          bufferedEventsPreloadBatchSize,
          timeouts,
          loggerFactory,
          sequencerMember,
          blockSequencerMode = blockSequencerMode,
          cachingConfigs = cachingConfigs,
          overrideCloseContext = overrideCloseContext,
        )
    }

  /** Sequencer pruning result
    * @param actuallyPrunedToUp
    *   timestamp actually pruned up to, often lower than requested timestamp; empty when no events
    *   remaining after pruning
    * @param report
    *   human readable report also used for logging
    */
  final case class SequencerPruningResult(
      actuallyPrunedToUp: Option[CantonTimestamp],
      report: String,
  )

  /** This implementation always returns the insertion point of the needle in the haystack. In case
    * of element existing it will return the index to insert needle after all the existing elements.
    */
  @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
  private[store] def binarySearch[A, B](
      haystack: Vector[A],
      accessor: A => B,
      needle: B,
  )(implicit
      ord: Ordering[B]
  ): Int = {
    import ord.*
    @tailrec
    def search(left: Int, right: Int): Int =
      if (left >= right) left
      else {
        val mid = left + (right - left) / 2
        if (accessor(haystack(mid)) <= needle) search(mid + 1, right)
        else search(left, mid)
      }

    // Optimizations for empty haystack and needle being outside the range
    if (haystack.isEmpty) {
      0
    } else {
      if (needle < accessor(checked(haystack.head))) {
        0
      } else if (accessor(checked(haystack.last)) <= needle) {
        haystack.size
      } else {
        search(0, haystack.size)
      }
    }
  }
}
