// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.store

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.option.*
import cats.syntax.order.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyReturningOps.*
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.config.BatchingConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.sequencing.protocol.{Batch, ClosedEnvelope}
import com.digitalasset.canton.synchronizer.block.UninitializedBlockHeight
import com.digitalasset.canton.synchronizer.sequencer.*
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{BytesUnit, EitherTUtil, ErrorUtil, retry}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

import java.util.UUID
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListMap}
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.SortedSet
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*

/** Thrown when a record that must be unique is inserted with a non-unique key. Mirrors the type of
  * exceptions that we would expect to see from a database.
  */
class UniqueKeyViolationException(message: String) extends RuntimeException(message)

class InMemorySequencerStore(
    protocolVersion: ProtocolVersion,
    override val sequencerMember: Member,
    override val blockSequencerMode: Boolean,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    protected val executionContext: ExecutionContext
) extends SequencerStore {

  override protected val batchingConfig: BatchingConfig = BatchingConfig()

  private case class StoredPayload(instanceDiscriminator: UUID, content: ByteString)

  private val nextNewMemberId = new AtomicInteger()
  private val members = TrieMap[Member, RegisteredMember]()
  private val memberPrunedPreviousEventTimestamps: mutable.Map[Member, CantonTimestamp] =
    mutable.Map.empty
  private val payloads = new ConcurrentSkipListMap[CantonTimestamp, StoredPayload]()
  private val events = new ConcurrentSkipListMap[CantonTimestamp, StoreEvent[PayloadId]]()
  private val watermark = new AtomicReference[Option[Watermark]](None)
  // using a concurrent hash map for the thread safe computeIfPresent updates
  private val acknowledgements =
    new ConcurrentHashMap[SequencerMemberId, CantonTimestamp]()
  private val lowerBound =
    new AtomicReference[Option[(CantonTimestamp, Option[CantonTimestamp])]](None)

  override def validateCommitMode(
      configuredCommitMode: CommitMode
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    // we're running in-memory so we immediately committing to the "store" we have
    EitherTUtil.unitUS

  override def registerMember(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencerMemberId] =
    FutureUnlessShutdown.pure {
      members
        .getOrElseUpdate(
          member,
          RegisteredMember(
            SequencerMemberId(nextNewMemberId.getAndIncrement()),
            timestamp,
            enabled = true,
          ),
        )
        .memberId
    }

  protected override def lookupMemberInternal(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[RegisteredMember]] =
    FutureUnlessShutdown.pure(
      members.get(member)
    )

  override def savePayloads(
      payloadsToInsert: NonEmpty[Seq[BytesPayload]],
      instanceDiscriminator: UUID,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SavePayloadsError, Unit] =
    payloadsToInsert.toNEF.parTraverse { case BytesPayload(id, content) =>
      Option(payloads.putIfAbsent(id.unwrap, StoredPayload(instanceDiscriminator, content)))
        .flatMap { existingPayload =>
          // if we found an existing payload it must have a matching instance discriminator
          if (existingPayload.instanceDiscriminator == instanceDiscriminator) None // no error
          else {
            if (blockSequencerMode) {
              None
            } else {
              SavePayloadsError.ConflictingPayloadId(id, existingPayload.instanceDiscriminator).some
            }
          }
        }
        .toLeft(())
        .leftWiden[SavePayloadsError]
        .toEitherT[FutureUnlessShutdown]
    }.void

  override def saveEvents(instanceIndex: Int, eventsToInsert: NonEmpty[Seq[Sequenced[PayloadId]]])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.pure(
      eventsToInsert.foreach { event =>
        Option(events.putIfAbsent(event.timestamp, event.event))
          .foreach(_ =>
            throw new UniqueKeyViolationException(
              s"Event timestamp is not unique [${event.timestamp}]"
            )
          )
      }
    )

  // Disable the buffer for in-memory store
  override def bufferedEventsMaxMemory: BytesUnit = BytesUnit.zero

  override def resetWatermark(instanceIndex: Int, ts: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SaveWatermarkError, Unit] =
    EitherT.pure[FutureUnlessShutdown, SaveWatermarkError] {
      watermark.getAndUpdate {
        case None => None
        case Some(current) if ts <= current.timestamp => Some(Watermark(ts, online = false))
        case Some(current) => Some(current)
      }.discard
    }

  override def saveWatermark(instanceIndex: Int, ts: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SaveWatermarkError, Unit] =
    EitherT.pure[FutureUnlessShutdown, SaveWatermarkError] {
      watermark.set(Some(Watermark(ts, online = true)))
    }

  override def fetchWatermark(instanceIndex: Int, maxRetries: Int = retry.Forever)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Watermark]] =
    FutureUnlessShutdown.pure(watermark.get())

  override def safeWatermark(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] =
    FutureUnlessShutdown.pure(watermark.get.map(_.timestamp))

  override def goOffline(instanceIndex: Int)(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.unit

  override def goOnline(instanceIndex: Int, now: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[CantonTimestamp] =
    FutureUnlessShutdown.pure {
      // we're the only sequencer that can write the watermark so just take the provided value
      watermark.set(Some(Watermark(now, online = true)))
      now
    }

  override def readEvents(
      memberId: SequencerMemberId,
      member: Member,
      fromExclusiveO: Option[CantonTimestamp] = None,
      limit: Int = 100,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[ReadEvents] = readEventsInternal(memberId, fromExclusiveO, limit)

  override protected def readEventsInternal(
      memberId: SequencerMemberId,
      fromExclusiveO: Option[CantonTimestamp] = None,
      limit: Int = 100,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[ReadEvents] = FutureUnlessShutdown.pure {
    import scala.jdk.CollectionConverters.*

    val watermarkO = watermark.get().map(_.timestamp)

    // if there's no watermark, we can't return any events
    watermarkO.fold[ReadEvents](SafeWatermark(watermarkO)) { watermark =>
      val payloads =
        fromExclusiveO
          .fold(events.tailMap(CantonTimestamp.MinValue, true))(events.tailMap(_, false))
          .entrySet()
          .iterator()
          .asScala
          .takeWhile(e => e.getKey <= watermark)
          .filter(e => isMemberRecipient(memberId)(e.getValue))
          .take(limit)
          .map(entry => Sequenced(entry.getKey, entry.getValue))
          .toList

      if (payloads.nonEmpty)
        ReadEventPayloads(payloads)
      else
        SafeWatermark(Some(watermark))
    }
  }

  override def readPayloads(payloadIds: Seq[IdOrPayload], member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[PayloadId, Batch[ClosedEnvelope]]] =
    FutureUnlessShutdown.pure(
      payloadIds.flatMap {
        case id: PayloadId =>
          Option(payloads.get(id.unwrap))
            .map(storedPayload =>
              id -> BytesPayload(id, storedPayload.content)
                .decodeBatchAndTrim(protocolVersion, member)
            )
            .toList
        case payload: BytesPayload =>
          List(payload.id -> payload.decodeBatchAndTrim(protocolVersion, member))
        case batch: FilteredBatch => List(batch.id -> Batch.trimForMember(batch.batch, member))
      }.toMap
    )

  private def isMemberRecipient(member: SequencerMemberId)(event: StoreEvent[_]): Boolean =
    event match {
      case deliver: DeliverStoreEvent[_] =>
        deliver.members.contains(
          member
        ) // only if they're a recipient (sender should already be a recipient)
      case receipt: ReceiptStoreEvent => receipt.sender == member // only if we're the sender
      case error: DeliverErrorStoreEvent => error.sender == member // only if we're the sender
    }

  /** No implementation as only required for crash recovery */
  override def deleteEventsPastWatermark(instanceIndex: Int)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] =
    FutureUnlessShutdown.pure(watermark.get().map(_.timestamp))

  def fetchPreviousEventTimestamp(memberId: SequencerMemberId, timestampInclusive: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] =
    FutureUnlessShutdown.pure {
      events
        .headMap(
          timestampInclusive.min(
            watermark.get().map(_.timestamp).getOrElse(CantonTimestamp.MaxValue)
          ),
          true,
        )
        .asScala
        .filter { case (_, event) => isMemberRecipient(memberId)(event) }
        .map { case (ts, _) => ts }
        .maxOption
        .orElse(memberPrunedPreviousEventTimestamps.get(lookupExpectedMember(memberId)))
    }

  override def acknowledge(
      member: SequencerMemberId,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.pure {
    // update the acknowledges with this timestamp only if greater than a existing value
    val _ = acknowledgements
      .compute(
        member,
        (_, existingOrNull) => Option(existingOrNull).map(_ max timestamp).getOrElse(timestamp),
      )
  }

  override def latestAcknowledgements()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[SequencerMemberId, CantonTimestamp]] =
    FutureUnlessShutdown.pure(acknowledgements.asScala.toMap)

  override def fetchLowerBound()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(CantonTimestamp, Option[CantonTimestamp])]] =
    FutureUnlessShutdown.pure(lowerBound.get())

  override def saveLowerBound(
      ts: CantonTimestamp,
      latestTopologyClientTimestamp: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SaveLowerBoundError, Unit] = {
    val newValueO = lowerBound.updateAndGet { existingO =>
      existingO
        .map { case (existingTs, existingTopologyTs) =>
          (existingTs max ts, existingTopologyTs max latestTopologyClientTimestamp)
        }
        .getOrElse((ts, latestTopologyClientTimestamp))
        .some
    }

    newValueO match {
      case Some(updatedValue) =>
        EitherT.cond[FutureUnlessShutdown](
          updatedValue == (ts, latestTopologyClientTimestamp),
          (),
          SaveLowerBoundError.BoundLowerThanExisting(
            updatedValue,
            (ts, latestTopologyClientTimestamp),
          ),
        )
      case None => // shouldn't happen
        ErrorUtil.internalError(new IllegalStateException("Lower bound should have been updated"))
    }
  }

  override protected[store] def pruneEvents(beforeExclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Int] =
    FutureUnlessShutdown.pure(prune(events, beforeExclusive))

  override protected[store] def prunePayloads(beforeExclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Int] =
    FutureUnlessShutdown.pure(prune(payloads, beforeExclusive))

  private def prune[A](
      timeOrderedSkipMap: ConcurrentSkipListMap[CantonTimestamp, A],
      pruningTimestamp: CantonTimestamp,
  ): Int = {
    val removed = new AtomicInteger()

    // .headMap returns everything before and not including the timestamp.
    // as the pruning timestamp should have been calculated based on the safe watermarks, and we ensure no records earlier
    // than these are inserted we can safely do this iteration to remove.
    timeOrderedSkipMap.headMap(pruningTimestamp) forEach { case (timestamp, _) =>
      removed.incrementAndGet()
      val _ = timeOrderedSkipMap.remove(timestamp)
    }

    removed.get()
  }

  override def locatePruningTimestamp(skip: NonNegativeInt)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] = FutureUnlessShutdown.pure {
    import scala.jdk.OptionConverters.*
    events.keySet().stream().skip(skip.value.toLong).findFirst().toScala
  }

  override def status(
      now: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[SequencerPruningStatus] =
    FutureUnlessShutdown.pure(internalStatus(now))

  private def internalStatus(
      now: CantonTimestamp
  ): SequencerPruningStatus =
    SequencerPruningStatus(
      lowerBound =
        lowerBound.get().map { case (timestamp, _) => timestamp }.getOrElse(CantonTimestamp.Epoch),
      now = now,
      members = members.collect {
        case (member, RegisteredMember(memberId, registeredFrom, enabled))
            if (registeredFrom <= now) =>
          SequencerMemberStatus(
            member,
            registeredFrom,
            lastAcknowledged = acknowledgements.asScala.get(memberId),
            enabled = enabled,
          )
      }.toSet,
    )

  /** This store does not support multiple concurrent instances so will do nothing. */
  override def markLaggingSequencersOffline(cutoffTime: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.unit

  /** Members must be registered to receive a memberId, so can typically assume they exist in this
    * structure
    */
  private def lookupExpectedMember(memberId: SequencerMemberId): Member =
    members
      .collectFirst { case (member, RegisteredMember(`memberId`, _, _)) =>
        member
      }
      .getOrElse(sys.error(s"Member id [$memberId] is not registered"))

  override protected def disableMemberInternal(
      memberId: SequencerMemberId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.pure {
      val member = lookupExpectedMember(memberId)
      members.put(member, members(member).copy(enabled = false)).discard
    }

  /** There can be no other sequencers sharing this storage */
  override def fetchOnlineInstances(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SortedSet[Int]] =
    FutureUnlessShutdown.pure(SortedSet.empty)

  @VisibleForTesting
  override protected[canton] def countRecords(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencerStoreRecordCounts] =
    FutureUnlessShutdown.pure(
      SequencerStoreRecordCounts(
        events.size().toLong,
        payloads.size.toLong,
      )
    )

  override def close(): Unit = ()

  override def readStateAtTimestamp(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencerSnapshot] = {

    // expand every event with members, group by timestamps per member, and take the max timestamp
    val previousEventTimestamps = events
      .headMap(timestamp, true)
      .asScala
      .toList
      .flatMap { case (timestamp, event) =>
        event.members.toList.map(member => (member, timestamp))
      }
      .groupMap1 { case (member, _) => member } { case (_, timestamp) => timestamp }
      .map { case (memberId, timestamps) =>
        (lookupExpectedMember(memberId), Some(timestamps.max1))
      }

    val previousEventTimestampsWithFallback = members.keySet.map { member =>
      member -> previousEventTimestamps.getOrElse(
        member,
        memberPrunedPreviousEventTimestamps.get(member),
      )
    }.toMap

    FutureUnlessShutdown.pure(
      SequencerSnapshot(
        timestamp,
        UninitializedBlockHeight,
        previousEventTimestampsWithFallback,
        internalStatus(timestamp),
        Map.empty,
        None,
        protocolVersion,
        Seq.empty,
        Seq.empty,
      )
    )
  }

  override protected def updatePrunedPreviousEventTimestampsInternal(
      updatedPreviousTimestamps: Map[SequencerMemberId, CantonTimestamp]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    updatedPreviousTimestamps.foreach { case (memberId, timestamp) =>
      memberPrunedPreviousEventTimestamps.put(lookupExpectedMember(memberId), timestamp).discard
    }
    FutureUnlessShutdown.unit
  }

  // Buffer is disabled for in-memory store
  override protected def preloadBufferInternal()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.unit

  override def latestTopologyClientRecipientTimestamp(
      member: Member,
      timestampExclusive: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] =
    fetchLowerBound().map { lowerBoundO =>
      val registeredMember = members.getOrElse(
        member,
        ErrorUtil.invalidState(
          s"Member $member is not registered in the sequencer store"
        ),
      )
      val sequencerMemberId = members
        .getOrElse(
          sequencerMember,
          ErrorUtil.invalidState(
            s"Sequencer member $sequencerMember is not registered in the sequencer store"
          ),
        )
        .memberId
      val latestTopologyTimestampCandidate = events
        .headMap(
          timestampExclusive.min(
            watermark.get().map(_.timestamp).getOrElse(CantonTimestamp.MaxValue)
          ),
          false,
        )
        .asScala
        .filter { case (_, event) =>
          isMemberRecipient(registeredMember.memberId)(event) && isMemberRecipient(
            sequencerMemberId
          )(
            event
          )
        }
        .map { case (ts, _) => ts }
        .maxOption
        .orElse(
          memberPrunedPreviousEventTimestamps.get(member)
        )

      lowerBoundO match {
        // if onboarded / pruned and the candidate returns below the lower bound (from sequencer_members table),
        // we should rather use the lower bound
        case Some((lowerBound, topologyLowerBound))
            if latestTopologyTimestampCandidate.forall(_ < lowerBound) =>
          topologyLowerBound
        // if no lower bound is set we use the candidate or fall back to the member registration time
        case _ => Some(latestTopologyTimestampCandidate.getOrElse(registeredMember.registeredFrom))
      }
    }

  override def previousEventTimestamp(
      memberId: SequencerMemberId,
      timestampExclusive: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] = FutureUnlessShutdown.pure(
    events
      .headMap(
        timestampExclusive.min(
          watermark.get().map(_.timestamp).getOrElse(CantonTimestamp.MaxValue)
        ),
        false,
      )
      .asScala
      .filter { case (_, event) => isMemberRecipient(memberId)(event) }
      .map { case (ts, _) => ts }
      .maxOption
      .orElse(
        memberPrunedPreviousEventTimestamps.get(lookupExpectedMember(memberId))
      )
  )
}
