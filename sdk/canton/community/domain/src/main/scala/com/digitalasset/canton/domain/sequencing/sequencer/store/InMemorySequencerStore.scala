// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.store

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.option.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.block.UninitializedBlockHeight
import com.digitalasset.canton.domain.sequencing.sequencer.*
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, retry}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

import java.util.UUID
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListMap}
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.SortedSet
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

/** Thrown when a record that must be unique is inserted with a non-unique key.
  * Mirrors the type of exceptions that we would expect to see from a database.
  */
class UniqueKeyViolationException(message: String) extends RuntimeException(message)

class InMemorySequencerStore(
    protocolVersion: ProtocolVersion,
    sequencerMember: Member,
    override val blockSequencerMode: Boolean,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    protected val executionContext: ExecutionContext
) extends SequencerStore {
  private case class StoredPayload(instanceDiscriminator: UUID, content: ByteString)

  private val nextNewMemberId = new AtomicInteger()
  private val members = TrieMap[Member, RegisteredMember]()
  private val payloads = new ConcurrentSkipListMap[CantonTimestamp, StoredPayload]()
  private val events = new ConcurrentSkipListMap[CantonTimestamp, StoreEvent[PayloadId]]()
  private val watermark = new AtomicReference[Option[Watermark]](None)
  private val checkpoints =
    new TrieMap[(RegisteredMember, SequencerCounter, CantonTimestamp), Option[CantonTimestamp]]()
  // using a concurrent hash map for the thread safe computeIfPresent updates
  private val acknowledgements =
    new ConcurrentHashMap[SequencerMemberId, CantonTimestamp]()
  private val lowerBound = new AtomicReference[Option[CantonTimestamp]](None)

  override def validateCommitMode(
      configuredCommitMode: CommitMode
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] =
    // we're running in-memory so we immediately committing to the "store" we have
    EitherTUtil.unit

  override def registerMember(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[SequencerMemberId] =
    Future.successful {
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
  ): Future[Option[RegisteredMember]] =
    Future.successful(
      members.get(member)
    )

  override def savePayloads(payloadsToInsert: NonEmpty[Seq[Payload]], instanceDiscriminator: UUID)(
      implicit traceContext: TraceContext
  ): EitherT[Future, SavePayloadsError, Unit] =
    payloadsToInsert.toNEF.parTraverse { case Payload(id, content) =>
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
        .toEitherT[Future]
    }.void

  override def saveEvents(instanceIndex: Int, eventsToInsert: NonEmpty[Seq[Sequenced[PayloadId]]])(
      implicit traceContext: TraceContext
  ): Future[Unit] =
    Future.successful(
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
  override val maxBufferedEvents: Int = 0

  override def resetWatermark(instanceIndex: Int, ts: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SaveWatermarkError, Unit] =
    EitherT.pure[Future, SaveWatermarkError] {
      watermark.getAndUpdate {
        case None => Some(Watermark(ts, online = false))
        case Some(current) if ts <= current.timestamp => Some(Watermark(ts, online = false))
        case Some(current) => Some(current)
      }.discard
    }

  override def saveWatermark(instanceIndex: Int, ts: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SaveWatermarkError, Unit] =
    EitherT.pure[Future, SaveWatermarkError] {
      watermark.set(Some(Watermark(ts, online = true)))
    }

  override def fetchWatermark(instanceIndex: Int, maxRetries: Int = retry.Forever)(implicit
      traceContext: TraceContext
  ): Future[Option[Watermark]] =
    Future.successful(watermark.get())

  override def safeWatermark(implicit traceContext: TraceContext): Future[Option[CantonTimestamp]] =
    Future.successful(watermark.get.map(_.timestamp))

  override def goOffline(instanceIndex: Int)(implicit traceContext: TraceContext): Future[Unit] =
    Future.unit

  override def goOnline(instanceIndex: Int, now: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[CantonTimestamp] =
    Future.successful {
      // we're the only sequencer that can write the watermark so just take the provided value
      watermark.set(Some(Watermark(now, online = true)))
      now
    }

  override def readEvents(
      memberId: SequencerMemberId,
      fromExclusiveO: Option[CantonTimestamp] = None,
      limit: Int = 100,
  )(implicit
      traceContext: TraceContext
  ): Future[ReadEvents] = readEventsInternal(memberId, fromExclusiveO, limit)

  override protected def readEventsInternal(
      memberId: SequencerMemberId,
      fromExclusiveO: Option[CantonTimestamp] = None,
      limit: Int = 100,
  )(implicit
      traceContext: TraceContext
  ): Future[ReadEvents] = Future.successful {
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

  override def readPayloads(payloadIds: Seq[IdOrPayload])(implicit
      traceContext: TraceContext
  ): Future[Map[PayloadId, Payload]] =
    Future.successful(
      payloadIds.flatMap {
        case id: PayloadId =>
          Option(payloads.get(id.unwrap))
            .map(storedPayload => id -> Payload(id, storedPayload.content))
            .toList
        case payload: Payload => List(payload.id -> payload)
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
  ): Future[Option[CantonTimestamp]] =
    Future.successful(watermark.get().map(_.timestamp))

  override def saveCounterCheckpoint(
      memberId: SequencerMemberId,
      checkpoint: CounterCheckpoint,
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): EitherT[Future, SaveCounterCheckpointError, Unit] = {
    checkpoints
      .updateWith(
        (members(lookupExpectedMember(memberId)), checkpoint.counter, checkpoint.timestamp)
      ) {
        case Some(Some(existing)) =>
          checkpoint.latestTopologyClientTimestamp match {
            case Some(newTimestamp) if newTimestamp > existing =>
              Some(checkpoint.latestTopologyClientTimestamp)
            case Some(_) => Some(Some(existing))
            case _ => None
          }
        case Some(None) | None => Some(checkpoint.latestTopologyClientTimestamp)
      }
      .discard
    EitherT.pure[Future, SaveCounterCheckpointError](())
  }

  override def fetchClosestCheckpointBefore(memberId: SequencerMemberId, counter: SequencerCounter)(
      implicit traceContext: TraceContext
  ): Future[Option[CounterCheckpoint]] =
    Future.successful {
      val registeredMember = members(lookupExpectedMember(memberId))
      checkpoints.keySet
        .filter(_._1 == registeredMember)
        .filter(_._2 < counter)
        .maxByOption(_._3)
        .map { case (_, foundCounter, foundTimestamp) =>
          val lastTopologyClientTimestamp =
            checkpoints
              .get((registeredMember, foundCounter, foundTimestamp))
              .flatten
          CounterCheckpoint(foundCounter, foundTimestamp, lastTopologyClientTimestamp)
        }
    }

  override def fetchLatestCheckpoint()(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] =
    Future.successful {
      val maxCheckpoint = checkpoints.keySet
        .maxByOption { case (_, _, timestamp) => timestamp }
        .map { case (_, _, timestamp) => timestamp }
        .filter(ts => ts > CantonTimestamp.Epoch)
      lazy val minEvent = Option(events.ceilingKey(CantonTimestamp.Epoch.immediateSuccessor))
      maxCheckpoint.orElse(minEvent)
    }

  override def fetchEarliestCheckpointForMember(memberId: SequencerMemberId)(implicit
      traceContext: TraceContext
  ): Future[Option[CounterCheckpoint]] =
    Future.successful {
      checkpoints.keySet
        .collect {
          case key @ (member, _, _) if member.memberId == memberId => key
        }
        .minByOption(_._3)
        .map { case (_, counter, timestamp) =>
          val lastTopologyClientTimestamp =
            checkpoints
              .get((members(lookupExpectedMember(memberId)), counter, timestamp))
              .flatten
          CounterCheckpoint(counter, timestamp, lastTopologyClientTimestamp)
        }
    }

  override def acknowledge(
      member: SequencerMemberId,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit] = Future.successful {
    // update the acknowledges with this timestamp only if greater than a existing value
    val _ = acknowledgements
      .compute(
        member,
        (_, existingOrNull) => Option(existingOrNull).map(_ max timestamp).getOrElse(timestamp),
      )
  }

  override def latestAcknowledgements()(implicit
      traceContext: TraceContext
  ): Future[Map[SequencerMemberId, CantonTimestamp]] =
    Future.successful(acknowledgements.asScala.toMap)

  override def fetchLowerBound()(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] =
    Future.successful(lowerBound.get())

  override def saveLowerBound(
      ts: CantonTimestamp
  )(implicit traceContext: TraceContext): EitherT[Future, SaveLowerBoundError, Unit] = {
    val newValueO = lowerBound.updateAndGet { existingO =>
      existingO.map(_ max ts).getOrElse(ts).some
    }

    newValueO match {
      case Some(updatedValue) =>
        EitherT.cond[Future](
          updatedValue == ts,
          (),
          SaveLowerBoundError.BoundLowerThanExisting(updatedValue, ts),
        )
      case None => // shouldn't happen
        ErrorUtil.internalError(new IllegalStateException("Lower bound should have been updated"))
    }
  }

  override protected[store] def pruneEvents(beforeExclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Int] =
    Future.successful(prune(events, beforeExclusive))

  override protected[store] def prunePayloads(beforeExclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Int] =
    Future.successful(prune(payloads, beforeExclusive))

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

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.While"))
  override protected[store] def pruneCheckpoints(
      beforeExclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Int] = {
    implicit val closeContext: CloseContext = CloseContext(
      FlagCloseable(logger, ProcessingTimeout())
    )
    val pruningCheckpoints = computeMemberCheckpoints(beforeExclusive).toSeq.map {
      case (member, checkpoint) =>
        (members(member).memberId, checkpoint)
    }
    saveCounterCheckpoints(pruningCheckpoints).map { _ =>
      val removedCheckpointsCounter = new AtomicInteger()
      checkpoints.keySet
        .filter { case (_, _, timestamp) =>
          timestamp < beforeExclusive
        }
        .foreach { key =>
          checkpoints.remove(key).discard
          removedCheckpointsCounter.incrementAndGet().discard
        }
      removedCheckpointsCounter.get()
    }
  }

  override def locatePruningTimestamp(skip: NonNegativeInt)(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] = Future.successful {
    import scala.jdk.OptionConverters.*
    events.keySet().stream().skip(skip.value.toLong).findFirst().toScala
  }

  override def status(
      now: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[SequencerPruningStatus] =
    Future.successful(internalStatus(now))

  private def internalStatus(
      now: CantonTimestamp
  ): SequencerPruningStatus =
    SequencerPruningStatus(
      lowerBound = lowerBound.get().getOrElse(CantonTimestamp.Epoch),
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
  ): Future[Unit] = Future.unit

  /** Members must be registered to receive a memberId, so can typically assume they exist in this structure */
  private def lookupExpectedMember(memberId: SequencerMemberId): Member =
    members
      .collectFirst { case (member, RegisteredMember(`memberId`, _, _)) =>
        member
      }
      .getOrElse(sys.error(s"Member id [$memberId] is not registered"))

  override def disableMemberInternal(
      memberId: SequencerMemberId
  )(implicit traceContext: TraceContext): Future[Unit] =
    Future.successful {
      val member = lookupExpectedMember(memberId)
      members.put(member, members(member).copy(enabled = false)).discard
    }

  /** There can be no other sequencers sharing this storage */
  override def fetchOnlineInstances(implicit traceContext: TraceContext): Future[SortedSet[Int]] =
    Future.successful(SortedSet.empty)

  @VisibleForTesting
  override protected[canton] def countRecords(implicit
      traceContext: TraceContext
  ): Future[SequencerStoreRecordCounts] =
    Future.successful(
      SequencerStoreRecordCounts(
        events.size().toLong,
        payloads.size.toLong,
        checkpoints.size.toLong,
      )
    )

  override def close(): Unit = ()

  override def readStateAtTimestamp(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[SequencerSnapshot] = {

    val memberCheckpoints = computeMemberCheckpoints(timestamp)

    val lastTs = memberCheckpoints.map(_._2.timestamp).maxOption.getOrElse(CantonTimestamp.MinValue)

    Future.successful(
      SequencerSnapshot(
        lastTs,
        UninitializedBlockHeight,
        memberCheckpoints.fmap(_.counter),
        internalStatus(lastTs),
        Map.empty,
        None,
        protocolVersion,
        Seq.empty,
        Seq.empty,
      )
    )
  }

  def checkpointsAtTimestamp(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Map[Member, CounterCheckpoint]] = Future.successful(computeMemberCheckpoints(timestamp))

  private def computeMemberCheckpoints(
      timestamp: CantonTimestamp
  ): Map[Member, CounterCheckpoint] = {
    val watermarkO = watermark.get()
    val sequencerMemberO = members.get(sequencerMember)

    watermarkO.fold[Map[Member, CounterCheckpoint]](Map()) { watermark =>
      val registeredMembers = members.filter {
        case (_member, RegisteredMember(_, registeredFrom, enabled)) =>
          enabled && registeredFrom <= timestamp
      }.toSeq
      val validEvents = events
        .headMap(if (watermark.timestamp < timestamp) watermark.timestamp else timestamp, true)
        .asScala
        .toSeq

      registeredMembers.map { case (member, registeredMember @ RegisteredMember(id, _, _)) =>
        val checkpointO = checkpoints.keySet
          .filter(_._1 == registeredMember)
          .filter(_._3 <= timestamp)
          .maxByOption(_._3)
          .map { case (_, counter, ts) =>
            CounterCheckpoint(counter, ts, checkpoints.get((registeredMember, counter, ts)).flatten)
          }

        val memberEvents = validEvents.filter(e =>
          isMemberRecipient(id)(e._2) && checkpointO.fold(true)(_.timestamp < e._1)
        )

        val latestSequencerTimestamp = sequencerMemberO
          .flatMap(member =>
            validEvents
              .filter(e => isMemberRecipient(id)(e._2) && isMemberRecipient(member.memberId)(e._2))
              .map(_._1)
              .maxOption
          )
          .orElse(checkpointO.flatMap(_.latestTopologyClientTimestamp))

        val checkpoint = CounterCheckpoint(
          checkpointO.map(_.counter).getOrElse(SequencerCounter(-1)) + memberEvents.size,
          timestamp,
          latestSequencerTimestamp,
        )
        (member, checkpoint)
      }.toMap
    }
  }

  override def saveCounterCheckpoints(
      checkpoints: Seq[(SequencerMemberId, CounterCheckpoint)]
  )(implicit traceContext: TraceContext, externalCloseContext: CloseContext): Future[Unit] =
    checkpoints.toList.parTraverse_ { case (memberId, checkpoint) =>
      saveCounterCheckpoint(memberId, checkpoint).value
    }

  override def recordCounterCheckpointsAtTimestamp(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext, externalCloseContext: CloseContext): Future[Unit] = {
    implicit val closeContext: CloseContext = CloseContext(
      FlagCloseable(logger, ProcessingTimeout())
    )
    val memberCheckpoints = computeMemberCheckpoints(timestamp)
    val memberIdCheckpointsF = memberCheckpoints.toList.parTraverseFilter {
      case (member, checkpoint) =>
        lookupMember(member).map {
          _.map(_.memberId -> checkpoint)
        }
    }
    memberIdCheckpointsF.flatMap { memberIdCheckpoints =>
      saveCounterCheckpoints(memberIdCheckpoints)(traceContext, closeContext)
    }
  }

  // Buffer is disabled for in-memory store
  override def prePopulateBuffer(implicit traceContext: TraceContext): Future[Unit] = Future.unit
}

object InMemorySequencerStore {
  final case class CheckpointDataAtCounter(
      timestamp: CantonTimestamp,
      latestTopologyClientTimestamp: Option[CantonTimestamp],
  ) {
    def toCheckpoint(sequencerCounter: SequencerCounter): CounterCheckpoint =
      CounterCheckpoint(sequencerCounter, timestamp, latestTopologyClientTimestamp)

    def toInconsistent: SaveCounterCheckpointError.CounterCheckpointInconsistent =
      SaveCounterCheckpointError.CounterCheckpointInconsistent(
        timestamp,
        latestTopologyClientTimestamp,
      )
  }

  object CheckpointDataAtCounter {
    def fromCheckpoint(checkpoint: CounterCheckpoint): CheckpointDataAtCounter =
      CheckpointDataAtCounter(checkpoint.timestamp, checkpoint.latestTopologyClientTimestamp)
  }
}
