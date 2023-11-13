// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import cats.data.OptionT
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.LedgerTransactionId
import com.digitalasset.canton.concurrent.{DirectExecutionContext, FutureSupervisor}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{
  HasLoggerName,
  NamedLoggerFactory,
  NamedLogging,
  NamedLoggingContext,
}
import com.digitalasset.canton.metrics.MetricsHelper
import com.digitalasset.canton.participant.event.RecordOrderPublisher.{
  PendingEventPublish,
  PendingPublish,
}
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.store.EventLogId.{
  DomainEventLogId,
  ParticipantEventLogId,
}
import com.digitalasset.canton.participant.store.MultiDomainEventLog.*
import com.digitalasset.canton.participant.store.{
  EventLogId,
  MultiDomainEventLog,
  ParticipantEventLog,
  SingleDimensionEventLog,
  TransferStore,
}
import com.digitalasset.canton.participant.sync.TimestampedEvent.{EventId, TransactionEventId}
import com.digitalasset.canton.participant.sync.{
  LedgerSyncEvent,
  SyncDomainPersistentStateLookup,
  TimestampedEvent,
}
import com.digitalasset.canton.participant.{
  GlobalOffset,
  LocalOffset,
  RequestOffset,
  TopologyOffset,
}
import com.digitalasset.canton.platform.pekkostreams.dispatcher.Dispatcher
import com.digitalasset.canton.platform.pekkostreams.dispatcher.SubSource.RangeSource
import com.digitalasset.canton.protocol.TargetDomainId
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil, SimpleExecutionQueue}

import java.util.concurrent.atomic.AtomicReference
import scala.collection.immutable.{SortedMap, TreeMap}
import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.reflect.ClassTag

class InMemoryMultiDomainEventLog(
    lookupEvent: NamedLoggingContext => (
        EventLogId,
        LocalOffset,
    ) => Future[TimestampedEvent],
    offsetsLookup: InMemoryOffsetsLookup,
    byEventId: NamedLoggingContext => EventId => OptionT[Future, (EventLogId, LocalOffset)],
    clock: Clock,
    metrics: ParticipantMetrics,
    override val transferStoreFor: TargetDomainId => Either[String, TransferStore],
    override val indexedStringStore: IndexedStringStore,
    override protected val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
)(override protected implicit val executionContext: ExecutionContext)
    extends MultiDomainEventLog
    with FlagCloseableAsync
    with NamedLogging {

  private case class Entries(
      firstOffset: GlobalOffset,
      lastOffset: Option[GlobalOffset],
      lastLocalOffsets: Map[EventLogId, LocalOffset],
      lastRequestOffsets: Map[EventLogId, RequestOffset],
      references: Set[(EventLogId, LocalOffset)],
      referencesByOffset: SortedMap[GlobalOffset, (EventLogId, LocalOffset, CantonTimestamp)],
      publicationTimeUpperBound: CantonTimestamp,
  )

  private val entriesRef: AtomicReference[Entries] =
    new AtomicReference(
      Entries(
        firstOffset = ledgerFirstOffset,
        lastOffset = None,
        lastLocalOffsets = Map.empty,
        lastRequestOffsets = Map.empty,
        references = Set.empty,
        referencesByOffset = TreeMap.empty,
        publicationTimeUpperBound = CantonTimestamp.MinValue,
      )
    )

  /*
    Ideally, we would like the Index of the dispatcher to be a GlobalOffset.
    However, since the `zeroIndex` is exclusive, we cannot guarantee that it is positive (and need to be able to represent 0).
    Hence, we use a `NonNegativeLong` and refine it when we have more knowledge.
   */
  private val dispatcher: Dispatcher[NonNegativeLong] = Dispatcher[NonNegativeLong](
    loggerFactory.name,
    ledgerFirstOffset.unwrap.decrement, // start index is exclusive
    ledgerFirstOffset.unwrap.decrement, // end index is inclusive
  )

  private val executionQueue = new SimpleExecutionQueue(
    "in-mem-multi-domain-event-log-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
  )

  // Must run sequentially
  private def publishInternal(data: PublicationData) = {
    implicit val traceContext: TraceContext = data.traceContext
    val PublicationData(id, event, inFlightReference) = data
    val localOffset = event.localOffset

    // Since we're in an execution queue, we don't need the compare-and-set operations
    val Entries(
      firstOffset,
      lastOffsetO,
      lastLocalOffsets,
      lastRequestOffsets,
      references,
      referencesByOffset,
      publicationTimeUpperBound,
    ) = entriesRef.get()

    if (references.contains((id, localOffset))) {
      logger.info(
        show"Skipping publication of event reference from event log $id with local offset $localOffset, as it has already been published."
      )
      Future.unit
    } else if (lastLocalOffsets.get(id).forall(_ < localOffset)) {
      val nextOffset = lastOffsetO match {
        case Some(lastOffset) => lastOffset.increment
        case None => ledgerFirstOffset
      }

      val now = clock.monotonicTime()
      val publicationTime = Ordering[CantonTimestamp].max(now, publicationTimeUpperBound)
      if (now < publicationTime) {
        logger.info(
          s"Local participant clock at $now is before a previous publication time $publicationTime. Has the clock been reset, e.g., during participant failover?"
        )
      }
      logger.debug(
        show"Published event from event log $id with local offset $localOffset at global offset $nextOffset with publication time $publicationTime."
      )

      val updatedLastRequestOffsets = localOffset match {
        case ro: RequestOffset => lastRequestOffsets + (id -> ro)
        case _: TopologyOffset => lastRequestOffsets
      }

      val newEntries = Entries(
        firstOffset = firstOffset,
        lastOffset = Some(nextOffset),
        lastLocalOffsets = lastLocalOffsets + (id -> localOffset),
        lastRequestOffsets = updatedLastRequestOffsets,
        references = references + ((id, localOffset)),
        referencesByOffset =
          referencesByOffset + (nextOffset -> ((id, localOffset, publicationTime))),
        publicationTimeUpperBound = publicationTime,
      )
      entriesRef.set(newEntries)

      val notifyTransferF: Future[Unit] = event.event match {
        case transfer: LedgerSyncEvent.TransferEvent if transfer.isTransferringParticipant =>
          notifyOnPublishTransfer(Seq((transfer, nextOffset)))

        case _ => Future.unit
      }

      notifyTransferF.map { _ =>
        dispatcher.signalNewHead(nextOffset.toNonNegative) // new end index is inclusive
        val deduplicationInfo = DeduplicationInfo.fromTimestampedEvent(event)
        val publication = OnPublish.Publication(
          nextOffset,
          publicationTime,
          inFlightReference,
          deduplicationInfo,
          event.event,
        )
        notifyOnPublish(Seq(publication))

        metrics.updatesPublished.mark(event.eventSize.toLong)(MetricsContext.Empty)
      }

    } else {
      ErrorUtil.internalError(
        new IllegalArgumentException(
          show"Unable to publish event at id $id and localOffset $localOffset, as that would reorder events."
        )
      )
    }
  }

  override def publish(data: PublicationData): Future[Unit] = {
    implicit val traceContext: TraceContext = data.traceContext

    // Overkill to use an executionQueue here, but it facilitates testing, because it
    // makes it behave similar to the DB version.
    FutureUtil.doNotAwait(
      executionQueue
        .execute(publishInternal(data), s"publish event ${data.event}")
        .onShutdown(logger.debug(s"Publish event ${data.event} aborted due to shutdown")),
      "An exception occurred while publishing an event. Stop publishing events.",
    )
    Future.unit
  }

  override def fetchUnpublished(id: EventLogId, upToInclusiveO: Option[LocalOffset])(implicit
      traceContext: TraceContext
  ): Future[Seq[PendingPublish]] = {
    val fromExclusive = entriesRef.get().lastLocalOffsets.get(id)
    for {
      unpublishedOffsets <- offsetsLookup.lookupOffsetsBetween(id)(
        fromExclusive,
        upToInclusiveO,
      )
      unpublishedEvents <- unpublishedOffsets.parTraverse(offset =>
        lookupEvent(namedLoggingContext)(id, offset)
      )
    } yield unpublishedEvents.map { tse =>
      PendingEventPublish(tse, tse.timestamp, id)
    }
  }

  override def prune(
      upToInclusive: GlobalOffset
  )(implicit traceContext: TraceContext): Future[Unit] = {
    entriesRef
      .updateAndGet {
        case Entries(
              firstOffset,
              nextOffsetO,
              lastLocalOffsets,
              lastRequestOffsets,
              references,
              referencesByOffset,
              publicationTimeUpperBound,
            ) =>
          val pruned = referencesByOffset.rangeTo(upToInclusive)
          val newReferences = references -- pruned.values.map {
            case (eventLogId, localOffset, _processingTime) => eventLogId -> localOffset
          }
          val newReferencesByOffset = referencesByOffset -- pruned.keys

          val updatedLastLocalOffsets = scala.collection.mutable.Map.empty[EventLogId, LocalOffset]
          val updatedLastRequestOffsets =
            scala.collection.mutable.Map.empty[EventLogId, RequestOffset]

          newReferences.foreach { case (eventLodId, localOffset) =>
            updatedLastLocalOffsets
              .updateWith(eventLodId) {
                case Some(current) => Some(current.max(localOffset))
                case None => Some(localOffset)
              }
              .discard

            localOffset match {
              case requestOffset: RequestOffset =>
                updatedLastRequestOffsets
                  .updateWith(eventLodId) {
                    case Some(current) => Some(current.max(requestOffset))
                    case None => Some(requestOffset)
                  }
                  .discard

              case _ => ()
            }
          }

          Entries(
            firstOffset = firstOffset.max(upToInclusive.increment),
            lastOffset = nextOffsetO.map(_.max(upToInclusive.increment)),
            lastLocalOffsets = updatedLastLocalOffsets.toMap,
            lastRequestOffsets = updatedLastRequestOffsets.toMap,
            references = newReferences,
            referencesByOffset = newReferencesByOffset,
            publicationTimeUpperBound = publicationTimeUpperBound,
          )
      }
      .discard[Entries]
    Future.unit
  }

  override def subscribe(
      startInclusive: Option[GlobalOffset]
  )(implicit tc: TraceContext): Source[(GlobalOffset, Traced[LedgerSyncEvent]), NotUsed] = {
    logger.debug(show"Subscribing at ${startInclusive.showValueOrNone}...")

    dispatcher
      .startingAt(
        startInclusive
          .getOrElse(entriesRef.get.firstOffset)
          .unwrap
          .decrement, // start index is exclusive
        RangeSource { (fromExcl, toIncl) =>
          Source(
            entriesRef.get.referencesByOffset
              .range(GlobalOffset(fromExcl.increment), GlobalOffset(toIncl.increment))
          )
            .mapAsync(1) { // Parallelism 1 is ok, as the lookup operation are quite fast with in memory stores.
              case (globalOffset, (id, localOffset, _processingTime)) =>
                for {
                  event <- lookupEvent(namedLoggingContext)(id, localOffset)
                } yield globalOffset.toNonNegative -> Traced(event.event)(
                  event.traceContext
                )
            }
        },
      )
      .map { case (offset, event) =>
        // by construction, the offset is positive
        GlobalOffset.tryFromLong(offset.unwrap) -> event
      }
  }

  override def lookupEventRange(upToInclusive: Option[GlobalOffset], limit: Option[Int])(implicit
      traceContext: TraceContext
  ): Future[Seq[(GlobalOffset, TimestampedEvent)]] = {
    val referencesInRange =
      entriesRef.get().referencesByOffset.rangeTo(upToInclusive.getOrElse(GlobalOffset.MaxValue))
    val limitedReferencesInRange = limit match {
      case Some(n) => referencesInRange.take(n)
      case None => referencesInRange
    }
    limitedReferencesInRange.toList.parTraverse {
      case (globalOffset, (id, localOffset, _processingTime)) =>
        lookupEvent(namedLoggingContext)(id, localOffset).map(globalOffset -> _)
    }
  }

  override def lookupByEventIds(
      eventIds: Seq[TimestampedEvent.EventId]
  )(implicit traceContext: TraceContext): Future[
    Map[TimestampedEvent.EventId, (GlobalOffset, TimestampedEvent, CantonTimestamp)]
  ] = {
    eventIds
      .parTraverseFilter { eventId =>
        byEventId(namedLoggingContext)(eventId).flatMap { case (eventLogId, localOffset) =>
          OptionT(globalOffsetFor(eventLogId, localOffset)).semiflatMap {
            case (globalOffset, publicationTime) =>
              lookupEvent(namedLoggingContext)(eventLogId, localOffset).map { event =>
                (eventId, (globalOffset, event, publicationTime))
              }
          }
        }.value
      }
      .map(_.toMap)
  }

  override def lookupTransactionDomain(
      transactionId: LedgerTransactionId
  )(implicit traceContext: TraceContext): OptionT[Future, DomainId] =
    byEventId(namedLoggingContext)(TransactionEventId(transactionId)).subflatMap {
      case (DomainEventLogId(id), _localOffset) => Some(id.item)
      case (ParticipantEventLogId(_), _localOffset) => None
    }

  override def lastLocalOffset(
      eventLogId: EventLogId,
      upToInclusive: Option[GlobalOffset] = None,
      timestampInclusive: Option[CantonTimestamp] = None,
  )(implicit traceContext: TraceContext): Future[Option[LocalOffset]] = {
    if (upToInclusive.isEmpty && timestampInclusive.isEmpty) {
      // In this case, we don't need to inspect the events
      Future.successful(entriesRef.get().lastLocalOffsets.get(eventLogId))
    } else
      lastLocalOffsetBeforeOrAt[LocalOffset](eventLogId, upToInclusive, timestampInclusive)
  }

  override def lastRequestOffset(
      eventLogId: EventLogId,
      upToInclusive: Option[GlobalOffset] = None,
      timestampInclusive: Option[CantonTimestamp] = None,
  )(implicit traceContext: TraceContext): Future[Option[RequestOffset]] = {
    if (upToInclusive.isEmpty && timestampInclusive.isEmpty) {
      // In this case, we don't need to inspect the events
      Future.successful(entriesRef.get().lastRequestOffsets.get(eventLogId))
    } else
      lastLocalOffsetBeforeOrAt[RequestOffset](eventLogId, upToInclusive, timestampInclusive)
  }

  private def lastLocalOffsetBeforeOrAt[T <: LocalOffset: ClassTag](
      eventLogId: EventLogId,
      upToInclusive: Option[GlobalOffset],
      timestampInclusive: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): Future[Option[T]] = {
    val referencesUpTo = entriesRef
      .get()
      .referencesByOffset
      .rangeTo(upToInclusive.getOrElse(GlobalOffset.MaxValue))
      .values
    val reversedLocalOffsets =
      referencesUpTo
        .collect {
          case (id, localOffset: T, _processingTime) if id == eventLogId => localOffset
        }
        .toList
        .reverse

    reversedLocalOffsets.collectFirstSomeM { localOffset =>
      lookupEvent(namedLoggingContext)(eventLogId, localOffset).map(event =>
        timestampInclusive
          .map(ts => Option.when(event.timestamp <= ts)(localOffset))
          .getOrElse(Some(localOffset))
      )
    }
  }

  override def locateOffset(
      deltaFromBeginning: Long
  )(implicit traceContext: TraceContext): OptionT[Future, GlobalOffset] =
    OptionT.fromOption(
      entriesRef.get().referencesByOffset.drop(deltaFromBeginning.toInt).headOption.map {
        case (offset, _) => offset
      }
    )

  override def locatePruningTimestamp(skip: NonNegativeInt)(implicit
      traceContext: TraceContext
  ): OptionT[Future, CantonTimestamp] =
    OptionT.fromOption(
      entriesRef
        .get()
        .referencesByOffset
        .drop(skip.value)
        .headOption
        .map { case (_offset, (_eventLogId, _localOffset, publicationTime)) =>
          publicationTime
        }
    )

  override def lookupOffset(globalOffset: GlobalOffset)(implicit
      traceContext: TraceContext
  ): OptionT[Future, (EventLogId, LocalOffset, CantonTimestamp)] =
    OptionT.fromOption(entriesRef.get().referencesByOffset.get(globalOffset))

  override def globalOffsetFor(eventLogId: EventLogId, localOffset: LocalOffset)(implicit
      traceContext: TraceContext
  ): Future[Option[(GlobalOffset, CantonTimestamp)]] = Future.successful {
    entriesRef.get().referencesByOffset.collectFirst {
      case (globalOffset, (id, offset, publicationTime))
          if id == eventLogId && offset == localOffset =>
        globalOffset -> publicationTime
    }
  }

  override def getOffsetByTimeUpTo(
      upToInclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): OptionT[Future, GlobalOffset] = {
    val entries = entriesRef.get()
    // As timestamps are increasing with global offsets, we could do a binary search here,
    // but it's not yet worth the effort.
    val lastO = entries.referencesByOffset
      .to(Iterable)
      .takeWhile { case (_offset, (_eventLogId, _localOffset, publicationTime)) =>
        publicationTime <= upToInclusive
      }
      .lastOption
    val offsetO = lastO.map { case (offset, _data) => offset }
    OptionT(Future.successful(offsetO))
  }

  override def getOffsetByTimeAtOrAfter(fromInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): OptionT[Future, (GlobalOffset, EventLogId, LocalOffset)] = {
    val entries = entriesRef.get()
    // As timestamps are increasing with global offsets, we could do a binary search here,
    // but it's not yet worth the effort.
    val offsetO = entries.referencesByOffset.to(Iterable).collectFirst {
      case (offset, (eventLogId, localOffset, publicationTime))
          if publicationTime >= fromInclusive =>
        (offset, eventLogId, localOffset)
    }
    OptionT(Future.successful(offsetO))
  }

  override def lastGlobalOffset(
      upToInclusive: Option[GlobalOffset] = None
  )(implicit traceContext: TraceContext): OptionT[Future, GlobalOffset] = {
    upToInclusive match {
      case Some(upToInclusive) =>
        OptionT.fromOption(
          entriesRef
            .get()
            .referencesByOffset
            .rangeTo(upToInclusive)
            .lastOption
            .map { case (offset, _) => offset }
        )

      case None => OptionT.fromOption(entriesRef.get().lastOffset)
    }
  }

  override def publicationTimeLowerBound: CantonTimestamp =
    entriesRef.get().referencesByOffset.lastOption.fold(CantonTimestamp.MinValue) {
      case (_offset, (_eventLogId, _localOffset, publicationTime)) => publicationTime
    }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty.*
    Seq(
      SyncCloseable("executionQueue", executionQueue.close()),
      AsyncCloseable(
        s"${this.getClass}: dispatcher",
        dispatcher.shutdown(),
        timeouts.shutdownShort.duration,
      ),
    )
  }

  override def flush(): Future[Unit] = executionQueue.flush()

  override def reportMaxEventAgeMetric(oldestEventTimestamp: Option[CantonTimestamp]): Unit =
    MetricsHelper.updateAgeInHoursGauge(
      clock,
      metrics.pruning.prune.maxEventAge,
      oldestEventTimestamp,
    )
}

object InMemoryMultiDomainEventLog extends HasLoggerName {
  def apply(
      syncDomainPersistentStates: SyncDomainPersistentStateLookup,
      participantEventLog: ParticipantEventLog,
      clock: Clock,
      timeouts: ProcessingTimeout,
      transferStoreFor: TargetDomainId => Either[String, TransferStore],
      indexedStringStore: IndexedStringStore,
      metrics: ParticipantMetrics,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): InMemoryMultiDomainEventLog = {

    def allEventLogs: Map[EventLogId, SingleDimensionEventLog[EventLogId]] =
      syncDomainPersistentStates.getAll.map { case (_, state) =>
        val eventLog = state.eventLog
        (eventLog.id: EventLogId) -> eventLog
      } + (participantEventLog.id -> participantEventLog)

    val offsetsLooker =
      new InMemoryOffsetsLookupImpl(syncDomainPersistentStates, participantEventLog)

    new InMemoryMultiDomainEventLog(
      lookupEvent(allEventLogs),
      offsetsLooker,
      byEventId(allEventLogs),
      clock,
      metrics,
      transferStoreFor,
      indexedStringStore,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )
  }

  private def lookupEvent(allEventLogs: => Map[EventLogId, SingleDimensionEventLog[EventLogId]])(
      namedLoggingContext: NamedLoggingContext
  )(id: EventLogId, localOffset: LocalOffset): Future[TimestampedEvent] = {
    implicit val loggingContext: NamedLoggingContext = namedLoggingContext
    implicit val tc: TraceContext = loggingContext.traceContext
    implicit val ec: ExecutionContext = DirectExecutionContext(loggingContext.tracedLogger)
    allEventLogs(id)
      .eventAt(localOffset)
      .getOrElse(
        ErrorUtil.internalError(
          new IllegalStateException(
            show"Unable to lookup event at offset $localOffset in event log $id."
          )
        )
      )
  }

  private def byEventId(allEventLogs: => Map[EventLogId, SingleDimensionEventLog[EventLogId]])(
      namedLoggingContext: NamedLoggingContext
  )(eventId: EventId): OptionT[Future, (EventLogId, LocalOffset)] = {
    implicit val loggingContext: NamedLoggingContext = namedLoggingContext
    implicit val tc: TraceContext = loggingContext.traceContext
    implicit val ec: ExecutionContext = DirectExecutionContext(loggingContext.tracedLogger)

    OptionT(for {
      result <- allEventLogs.toList.parTraverseFilter { case (eventLogId, eventLog) =>
        eventLog.eventById(eventId).map(event => eventLogId -> event.localOffset).value
      }
    } yield result.headOption)
  }
}
