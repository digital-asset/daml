// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.OptionT
import cats.syntax.functorFilter.*
import cats.syntax.option.*
import cats.syntax.parallel.*
import com.daml.metrics.api.MetricsContext
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LedgerTransactionId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.MetricsHelper
import com.digitalasset.canton.participant.event.RecordOrderPublisher.{
  PendingEventPublish,
  PendingPublish,
}
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.store.EventLogId.ParticipantEventLogId
import com.digitalasset.canton.participant.store.MultiDomainEventLog.{
  DeduplicationInfo,
  OnPublish,
  PublicationData,
}
import com.digitalasset.canton.participant.store.db.DbMultiDomainEventLog.*
import com.digitalasset.canton.participant.store.{EventLogId, MultiDomainEventLog, TransferStore}
import com.digitalasset.canton.participant.sync.TimestampedEvent.TransactionEventId
import com.digitalasset.canton.participant.sync.{LedgerSyncEvent, TimestampedEvent}
import com.digitalasset.canton.participant.{GlobalOffset, LocalOffset, RequestOffset}
import com.digitalasset.canton.pekkostreams.dispatcher.Dispatcher
import com.digitalasset.canton.pekkostreams.dispatcher.SubSource.RangeSource
import com.digitalasset.canton.protocol.TargetDomainId
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.resource.DbStorage.Implicits.{
  getResultLfPartyId as _,
  getResultPackageId as _,
}
import com.digitalasset.canton.resource.DbStorage.Profile
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.*
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import slick.jdbc.GetResult

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

/** Must be created by factory methods on DbSingleDimensionEventLog for optionality on how to perform the required
  * async initialization of current head.
  *
  * @param publicationTimeBoundInclusive The highest publication time of all previously published events
  *                                      (or [[com.digitalasset.canton.data.CantonTimestamp.MinValue]] if no events were published).
  * @param inboxSize capacity of source queue that stores event before they are persisted.
  * @param maxConcurrentPublications maximum number of concurrent calls to `publish`.
  *                                  If event publication back-pressures for some reason (e.g. db is unavailable),
  *                                  `publish` will throw an exception,
  *                                  if the number of concurrent calls exceeds this number.
  *                                  A high number comes with higher memory usage, as Pekko allocates a buffer of that size internally.
  * @param maxBatchSize maximum number of events that will be persisted in a single database transaction.
  * @param batchTimeout after this timeout, the collect events so far will be persisted, even if `maxBatchSize` has
  *                     not been attained.
  *                     A small number comes with higher CPU usage, as Pekko schedules periodic task at that delay.
  */
class DbMultiDomainEventLog private[db] (
    initialLastGlobalOffset: Option[GlobalOffset],
    publicationTimeBoundInclusive: CantonTimestamp,
    lastLocalOffsets: TrieMap[Int, LocalOffset],
    inboxSize: Int = 4000,
    maxConcurrentPublications: Int = 100,
    maxBatchSize: PositiveInt,
    batchTimeout: FiniteDuration = 10.millis,
    participantEventLogId: ParticipantEventLogId,
    storage: DbStorage,
    clock: Clock,
    metrics: ParticipantMetrics,
    override val transferStoreFor: TargetDomainId => Either[String, TransferStore],
    override val indexedStringStore: IndexedStringStore,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(
    override protected implicit val executionContext: ExecutionContext,
    implicit val mat: Materializer,
) extends MultiDomainEventLog
    with FlagCloseableAsync
    with HasCloseContext
    with NamedLogging
    with HasFlushFuture {

  import ParticipantStorageImplicits.*
  import TimestampedEvent.getResultTimestampedEvent
  import storage.api.*
  import storage.converters.*

  /*
    Ideally, we would like the Index of the dispatcher to be a GlobalOffset.
    However, since the `zeroIndex` is exclusive, we cannot guarantee that it is positive (and need to be able to represent 0).
    Hence, we use a `NonNegativeLong` and refine it when we have more knowledge.
   */
  private val dispatcher: Dispatcher[NonNegativeLong] =
    Dispatcher(
      loggerFactory.name,
      MultiDomainEventLog.ledgerFirstOffset.unwrap.decrement, // start index is exclusive
      initialLastGlobalOffset.fold(MultiDomainEventLog.ledgerFirstOffset.unwrap.decrement)(
        _.toNonNegative
      ), // end index is inclusive
    )

  /** Lower bound on the last offset that we have signalled to the dispatcher. */
  private val lastPublishedOffset = new AtomicReference[Option[GlobalOffset]](
    initialLastGlobalOffset
  )

  /** Non-strict upper bound on the publication time of all previous events.
    * Increases monotonically.
    */
  private val publicationTimeUpperBoundRef: AtomicReference[CantonTimestamp] =
    new AtomicReference[CantonTimestamp](publicationTimeBoundInclusive)

  /** Non-strict lower bound on the latest publication time of a published event.
    * Increases monotonically.
    */
  private val publicationTimeLowerBoundRef: AtomicReference[CantonTimestamp] =
    new AtomicReference[CantonTimestamp](publicationTimeBoundInclusive)

  /** The [[scala.concurrent.Promise]] is completed after the event has been published. */
  private val source = Source.queue[(PublicationData, Promise[Unit])](
    bufferSize = inboxSize,
    overflowStrategy = OverflowStrategy.backpressure,
    maxConcurrentOffers = maxConcurrentPublications,
  )

  private val publicationFlow = source
    .viaMat(KillSwitches.single)(Keep.both)
    .groupedWithin(maxBatchSize.unwrap, batchTimeout)
    .mapAsync(1)(doPublish)
    .toMat(Sink.ignore)(Keep.both)

  private val ((eventsQueue, killSwitch), done) =
    PekkoUtil.runSupervised(
      logger.error("An exception occurred while publishing an event. Stop publishing events.", _)(
        TraceContext.empty
      ),
      publicationFlow,
    )

  override def publish(data: PublicationData): Future[Unit] = {
    implicit val traceContext: TraceContext = data.traceContext

    val promise = Promise[Unit]()
    for {
      result <- eventsQueue.offer(data -> promise)
    } yield {
      result match {
        case QueueOfferResult.Enqueued => // nothing to do
          addToFlushAndLogError(s"Publish offset ${data.localOffset} from ${data.eventLogId}")(
            promise.future
          )
        case _: QueueCompletionResult =>
          ErrorUtil.internalError(
            new IllegalStateException(
              s"Failed to publish event, as the queue is completed. $result"
            )
          )
        case QueueOfferResult.Dropped =>
          // This should never happen due to overflowStrategy backpressure.
          ErrorUtil.internalError(
            new IllegalStateException(
              s"Failed to publish event. The event has been unexpectedly dropped."
            )
          )
      }
    }
  }

  /** `insert` assigns global offsets in ascending order, but some events may be skipped because they are already there.
    * So we go through the events in reverse order and try to match them against the found global offsets,
    * stopping at the global offset that was known previously.
    *
    * @param eventsToPublish
    * @param globalOffsetStrictLowerBound Strict lower bound on the global offset for the events
    * @param lastEvents
    * @return
    */
  private def publications(
      eventsToPublish: Seq[PublicationData],
      globalOffsetStrictLowerBound: Option[GlobalOffset],
      lastEvents: NonEmpty[Seq[(GlobalOffset, EventLogId, LocalOffset, CantonTimestamp)]],
  ): Seq[OnPublish.Publication] = {
    val bound = globalOffsetStrictLowerBound.fold(0L)(_.toLong)
    val cappedLastEvents = lastEvents.forgetNE.iterator.takeWhile {
      case (globalOffset, _eventLogId, _localOffset, _publicationTime) =>
        globalOffset.toLong > bound
    }
    IterableUtil
      .subzipBy(cappedLastEvents, eventsToPublish.reverseIterator) {
        case (
              (globalOffset, allocatedEventLogId, allocatedLocalOffset, publicationTime),
              PublicationData(eventLogId, event, inFlightReference),
            ) =>
          Option.when(
            allocatedEventLogId == eventLogId && allocatedLocalOffset == event.localOffset
          ) {
            val deduplicationInfo = DeduplicationInfo.fromTimestampedEvent(event)
            OnPublish
              .Publication(
                globalOffset,
                publicationTime,
                inFlightReference,
                deduplicationInfo,
                event.event,
              )
          }
      }
      .reverse
  }

  /** Allocates the global offsets for the batch of events and notifies the dispatcher on [[onPublish]].
    * Must run sequentially.
    */
  private def doPublish(events: Seq[(PublicationData, Promise[Unit])]): Future[Unit] = {
    val eventsToPublish = events.map(_._1)

    /*
      We can have this here because this method is supposed to be called sequentially and it
      is the only place where we update `lastPublishedOffset`
     */
    val previousGlobalOffset = lastPublishedOffset.get()

    implicit val batchTraceContext: TraceContext = TraceContext.ofBatch(eventsToPublish)(logger)

    def nextPublicationTime(): CantonTimestamp = {
      val now = clock.monotonicTime()
      val next = publicationTimeUpperBoundRef.updateAndGet(oldPublicationTime =>
        Ordering[CantonTimestamp].max(now, oldPublicationTime)
      )
      if (now < next) {
        logger.info(
          s"Local participant clock at $now is before a previous publication time $next. Has the clock been reset, e.g., during participant failover?"
        )
      }
      next
    }

    def advancePublicationTimeLowerBound(newBound: CantonTimestamp): Unit = {
      val oldBound = publicationTimeLowerBoundRef.getAndUpdate(oldBound =>
        Ordering[CantonTimestamp].max(oldBound, newBound)
      )
      if (oldBound < newBound)
        logger.trace(s"Advanced publication time lower bound to $newBound")
      else
        logger.trace(
          s"Publication time lower bound remains at $oldBound as new bound $newBound is not higher"
        )
    }

    for {
      _ <- enforceInOrderPublication(eventsToPublish)
      publicationTime = nextPublicationTime()
      _ <- insert(eventsToPublish, publicationTime)

      // Find the global offsets assigned to the inserted events
      foundEventsO <- lastEvents(events.size).map(NonEmpty.from)
      foundEvents = foundEventsO.getOrElse {
        ErrorUtil.internalError(
          new IllegalStateException(
            "Failed to publish events to par_linearized_event_log. The table appears to be empty."
          )
        )
      }
      (newGlobalOffset, _, _, _) = foundEvents.head1

      published = publications(
        eventsToPublish = eventsToPublish,
        globalOffsetStrictLowerBound = previousGlobalOffset,
        lastEvents = foundEvents,
      )

      transferEvents = published.mapFilter { publication =>
        publication.event match {
          case transfer: LedgerSyncEvent.TransferEvent if transfer.isTransferringParticipant =>
            Some((transfer, publication.globalOffset))
          case _ => None
        }
      }

      // We wait here because we don't want to update the ledger end (new head of the dispatcher) if notification fails
      _ <- notifyOnPublishTransfer(transferEvents)

    } yield {
      logger.debug(show"Signalling global offset $newGlobalOffset.")

      dispatcher.signalNewHead(newGlobalOffset.toNonNegative)

      lastPublishedOffset.set(newGlobalOffset.some)
      // Advance the publication time lower bound to `publicationTime`
      // only if it was actually stored for at least one event.
      if (published.nonEmpty) {
        advancePublicationTimeLowerBound(publicationTime)
      }
      notifyOnPublish(published)

      events.foreach { case (data @ PublicationData(id, event, _inFlightReference), promise) =>
        promise.success(())
        logger.debug(
          show"Published event from event log $id with local offset ${event.localOffset} with publication time $publicationTime."
        )(data.traceContext)
        metrics.updatesPublished.mark(event.eventSize.toLong)(MetricsContext.Empty)
      }
    }
  }

  private def enforceInOrderPublication(events: Seq[PublicationData]): Future[Unit] = {
    val eventsBeforeOrAtLastLocalOffset = events.mapFilter {
      case data @ PublicationData(id, event, _inFlightReference) =>
        implicit val traceContext: TraceContext = data.traceContext
        val localOffset = event.localOffset
        lastLocalOffsets.get(id.index) match {
          case Some(lastLocalOffset) if localOffset <= lastLocalOffset =>
            logger.info(
              show"The event at id $id and local offset $localOffset can't be published, because the last published offset is already $lastLocalOffset."
            )
            Some(Traced((id, localOffset)))
          case _ =>
            lastLocalOffsets.put(id.index, localOffset).discard
            None
        }
    }

    eventsBeforeOrAtLastLocalOffset.parTraverse_(_.withTraceContext(implicit traceContext => {
      case (id, localOffset) =>
        for {
          existingGlobalOffsetO <- globalOffsetFor(id, localOffset)
        } yield existingGlobalOffsetO match {
          case Some(_) =>
            logger.info(
              show"The event at id $id and local offset $localOffset already exists in par_linearized_event_log. Nothing to do."
            )
          case None =>
            ErrorUtil.internalError(
              new IllegalArgumentException(
                show"Unable to publish event at id $id and localOffset $localOffset, as that would reorder events."
              )
            )
        }
    }))
  }

  private def insert(events: Seq[PublicationData], publicationTime: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    val insertStatement = storage.profile match {
      case _: DbStorage.Profile.Oracle =>
        """merge /*+ INDEX ( par_linearized_event_log ( local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker, log_id ) ) */
            |into par_linearized_event_log lel
            |using (select ? log_id, ? local_offset_effective_time, ? local_offset_discriminator, ? local_offset_tie_breaker from dual) input
            |on (lel.local_offset_effective_time = input.local_offset_effective_time and lel.local_offset_discriminator = input.local_offset_discriminator and lel.local_offset_tie_breaker = input.local_offset_tie_breaker and lel.log_id = input.log_id)
            |when not matched then
            |  insert (log_id, local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker, publication_time)
            |  values (input.log_id, input.local_offset_effective_time, input.local_offset_discriminator, input.local_offset_tie_breaker, ?)""".stripMargin
      case _: DbStorage.Profile.Postgres | _: DbStorage.Profile.H2 =>
        """insert into par_linearized_event_log (log_id, local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker, publication_time)
            |values (?, ?, ?, ?, ?)
            |on conflict do nothing""".stripMargin
    }
    val bulkInsert = DbStorage.bulkOperation_(
      insertStatement,
      events,
      storage.profile,
    ) { pp => tracedEvent =>
      val PublicationData(id, event, _inFlightReference) = tracedEvent
      pp >> id.index
      pp >> event.localOffset
      pp >> publicationTime
    }
    val query = storage.withSyncCommitOnPostgres(bulkInsert)
    storage.queryAndUpdate(query, functionFullName)
  }

  override def fetchUnpublished(id: EventLogId, upToInclusiveO: Option[LocalOffset])(implicit
      traceContext: TraceContext
  ): Future[List[PendingPublish]] = {
    val fromExclusive = lastLocalOffsets.get(id.index)
    logger.info(
      s"Fetch unpublished in log $id, from $fromExclusive (exclusive) up to $upToInclusiveO (inclusive)"
    )

    {
      for {
        unpublishedLocalOffsets <- DbSingleDimensionEventLog.lookupEventRange(
          storage = storage,
          eventLogId = id,
          fromExclusive = fromExclusive,
          toInclusive = upToInclusiveO,
          fromTimestampInclusive = None,
          toTimestampInclusive = None,
          limit = None,
        )
      } yield {
        unpublishedLocalOffsets.toList
          .map { case (_rc, tse) =>
            PendingEventPublish(tse, tse.timestamp, id)
          }
          .sortBy(_.event.localOffset)
      }
    }
  }

  override def prune(
      upToInclusive: GlobalOffset
  )(implicit traceContext: TraceContext): Future[Unit] = {
    {
      storage
        .update_(
          sqlu"delete from par_linearized_event_log where global_offset <= $upToInclusive",
          functionFullName,
        )
    }
  }

  override def subscribe(startInclusive: Option[GlobalOffset])(implicit
      traceContext: TraceContext
  ): Source[(GlobalOffset, Traced[LedgerSyncEvent]), NotUsed] = {
    dispatcher
      .startingAt(
        // start index is exclusive
        startInclusive.getOrElse(MultiDomainEventLog.ledgerFirstOffset).unwrap.decrement,
        RangeSource { (fromExcl, toIncl) =>
          Source(
            RangeUtil
              .partitionIndexRange(fromExcl.unwrap, toIncl.unwrap, maxBatchSize.unwrap.toLong)
          )
            .mapAsync(1) { case (batchFromExcl, batchToIncl) =>
              storage.query(
                sql"""select /*+ INDEX (linearized_event_log pk_linearized_event_log, par_event_log pk_par_event_log) */ global_offset, content, trace_context
                  from par_linearized_event_log lel
                  join par_event_log el on lel.log_id = el.log_id and lel.local_offset_effective_time = el.local_offset_effective_time and lel.local_offset_tie_breaker = el.local_offset_tie_breaker
                  where global_offset > $batchFromExcl and global_offset <= $batchToIncl
                  order by global_offset asc"""
                  .as[
                    (NonNegativeLong, Traced[LedgerSyncEvent])
                  ],
                functionFullName,
              )
            }
            .mapConcat(identity)
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
    storage
      .query(
        sql"""select global_offset, el.local_offset_effective_time, el.local_offset_discriminator, el.local_offset_tie_breaker, request_sequencer_counter, el.event_id, content, trace_context
                from par_linearized_event_log lel join par_event_log el on lel.log_id = el.log_id
                and lel.local_offset_effective_time = el.local_offset_effective_time and lel.local_offset_discriminator = el.local_offset_discriminator and lel.local_offset_tie_breaker = el.local_offset_tie_breaker
                where global_offset <= ${upToInclusive.fold(Long.MaxValue)(_.toLong)}
                order by global_offset asc #${storage.limit(limit.getOrElse(Int.MaxValue))}"""
          .as[(GlobalOffset, TimestampedEvent)],
        functionFullName,
      )
  }

  override def lookupByEventIds(
      eventIds: Seq[TimestampedEvent.EventId]
  )(implicit traceContext: TraceContext): Future[
    Map[TimestampedEvent.EventId, (GlobalOffset, TimestampedEvent, CantonTimestamp)]
  ] = eventIds match {
    case NonEmpty(nonEmptyEventIds) =>
      val inClauses = DbStorage.toInClauses_(
        "el.event_id",
        nonEmptyEventIds,
        maxBatchSize,
      )
      val queries = inClauses.map { inClause =>
        import DbStorage.Implicits.BuilderChain.*
        (sql"""
            select global_offset, el.local_offset_effective_time, el.local_offset_discriminator, el.local_offset_tie_breaker, request_sequencer_counter, el.event_id, content, trace_context, publication_time
            from par_linearized_event_log lel
            join par_event_log el on lel.log_id = el.log_id and lel.local_offset_effective_time = el.local_offset_effective_time and lel.local_offset_discriminator = el.local_offset_discriminator and lel.local_offset_tie_breaker = el.local_offset_tie_breaker
            where
            """ ++ inClause).as[(GlobalOffset, TimestampedEvent, CantonTimestamp)]
      }
      storage.sequentialQueryAndCombine(queries, functionFullName).map { events =>
        events.map { case data @ (globalOffset, event, _publicationTime) =>
          val eventId = event.eventId.getOrElse(
            ErrorUtil.internalError(
              new DbDeserializationException(
                s"Event $event at global offset $globalOffset does not have an event ID."
              )
            )
          )
          eventId -> data
        }.toMap
      }
    case _ => Future.successful(Map.empty)
  }

  override def lookupTransactionDomain(transactionId: LedgerTransactionId)(implicit
      traceContext: TraceContext
  ): OptionT[Future, DomainId] = {
    storage
      .querySingle(
        sql"""select log_id from par_event_log where event_id = ${TransactionEventId(
            transactionId
          )}"""
          .as[Int]
          .headOption,
        functionFullName,
      )
      .flatMap(idx => IndexedDomain.fromDbIndexOT("event_log", indexedStringStore)(idx))
      .map(_.domainId)
  }

  private def lastLocalOffsetBeforeOrAt[T <: LocalOffset](
      eventLogId: EventLogId,
      upToInclusive: Option[GlobalOffset],
      timestampInclusive: CantonTimestamp,
      localOffsetDiscriminator: Option[Int],
  )(implicit traceContext: TraceContext, getResult: GetResult[T]): Future[Option[T]] = {
    import DbStorage.Implicits.BuilderChain.*

    {
      val tsFilter = sql" and el.ts <= $timestampInclusive"
      val localOffsetDiscriminatorFilter =
        localOffsetDiscriminator.fold(sql" ")(disc =>
          sql" and lel.local_offset_discriminator=$disc"
        )
      val globalOffsetFilter = upToInclusive
        .map(upToInclusive => sql" and global_offset <= $upToInclusive")
        .getOrElse(sql" ")

      val ordering = sql" order by global_offset desc #${storage.limit(1)}"

      // Note for idempotent retries, we don't require that the global offset has an actual ledger entry reference
      val base =
        sql"""select lel.local_offset_effective_time, lel.local_offset_discriminator, lel.local_offset_tie_breaker
              from par_linearized_event_log lel
              join par_event_log el on lel.log_id = el.log_id and lel.local_offset_effective_time = el.local_offset_effective_time and lel.local_offset_discriminator = el.local_offset_discriminator and lel.local_offset_tie_breaker = el.local_offset_tie_breaker
              where lel.log_id = ${eventLogId.index}
              """

      val query =
        (base ++ globalOffsetFilter ++ tsFilter ++ localOffsetDiscriminatorFilter ++ ordering)
          .as[T]
          .headOption

      storage.query(query, functionFullName)
    }
  }

  override def lastLocalOffsetBeforeOrAt(
      eventLogId: EventLogId,
      upToInclusive: Option[GlobalOffset] = None,
      timestampInclusive: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Option[LocalOffset]] =
    lastLocalOffsetBeforeOrAt(eventLogId, upToInclusive, timestampInclusive, None)

  override def lastRequestOffsetBeforeOrAt(
      eventLogId: EventLogId,
      upToInclusive: Option[GlobalOffset] = None,
      timestampInclusive: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Option[RequestOffset]] =
    lastLocalOffsetBeforeOrAt[RequestOffset](
      eventLogId,
      upToInclusive,
      timestampInclusive,
      Some(LocalOffset.RequestOffsetDiscriminator),
    )

  private def lastLocalOffset[T <: LocalOffset](
      eventLogId: EventLogId,
      upToInclusive: Option[GlobalOffset],
      localOffsetDiscriminator: Option[Int],
  )(implicit traceContext: TraceContext, getResult: GetResult[T]): Future[Option[T]] = {
    import DbStorage.Implicits.BuilderChain.*

    {
      val localOffsetDiscriminatorFilter =
        localOffsetDiscriminator.fold(sql" ")(disc => sql" and local_offset_discriminator=$disc")
      val globalOffsetFilter = upToInclusive
        .map(upToInclusive => sql" and global_offset <= $upToInclusive")
        .getOrElse(sql" ")

      val ordering = sql" order by global_offset desc #${storage.limit(1)}"

      // Note for idempotent retries, we don't require that the global offset has an actual ledger entry reference
      val base =
        sql"""select local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker
              from par_linearized_event_log
              where log_id = ${eventLogId.index}
              """

      val query =
        (base ++ globalOffsetFilter ++ localOffsetDiscriminatorFilter ++ ordering).as[T].headOption

      storage.query(query, functionFullName)
    }
  }

  override def lastLocalOffset(
      eventLogId: EventLogId,
      upToInclusive: Option[GlobalOffset] = None,
  )(implicit traceContext: TraceContext): Future[Option[LocalOffset]] =
    lastLocalOffset(eventLogId, upToInclusive, None)

  override def lastRequestOffset(
      eventLogId: EventLogId,
      upToInclusive: Option[GlobalOffset] = None,
  )(implicit traceContext: TraceContext): Future[Option[RequestOffset]] =
    lastLocalOffset[RequestOffset](
      eventLogId,
      upToInclusive,
      Some(LocalOffset.RequestOffsetDiscriminator),
    )

  override def locateOffset(
      deltaFromBeginning: Long
  )(implicit traceContext: TraceContext): OptionT[Future, GlobalOffset] = {
    // The following query performs a table scan which can in theory become a problem if deltaFromBeginning is large.
    // We cannot simply perform a lookup as big/serial columns can have gaps.
    // However as we are planning to prune in batches, deltaFromBeginning will be limited by the batch size and be
    // reasonable.
    storage.querySingle(
      sql"select global_offset from par_linearized_event_log order by global_offset #${storage
          .limit(1, deltaFromBeginning)}"
        .as[GlobalOffset]
        .headOption,
      functionFullName,
    )
  }

  override def locatePruningTimestamp(
      skip: NonNegativeInt
  )(implicit traceContext: TraceContext): OptionT[Future, CantonTimestamp] = {
    storage.querySingle(
      sql"select publication_time from par_linearized_event_log order by global_offset #${storage
          .limit(1, skip.value.toLong)}"
        .as[CantonTimestamp]
        .headOption,
      functionFullName,
    )
  }

  override def lookupOffset(globalOffset: GlobalOffset)(implicit
      traceContext: TraceContext
  ): OptionT[Future, (EventLogId, LocalOffset, CantonTimestamp)] = {
    storage
      .querySingle(
        sql"select log_id, local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker, publication_time from par_linearized_event_log where global_offset = $globalOffset"
          .as[(Int, LocalOffset, CantonTimestamp)]
          .headOption,
        functionFullName,
      )
      .flatMap { case (logIndex, offset, ts) =>
        EventLogId.fromDbLogIdOT("linearized_event_log", indexedStringStore)(logIndex).map { x =>
          (x, offset, ts)
        }
      }
  }

  override def globalOffsetFor(eventLogId: EventLogId, localOffset: LocalOffset)(implicit
      traceContext: TraceContext
  ): Future[Option[(GlobalOffset, CantonTimestamp)]] = {
    storage.query(
      sql"""
      select lel.global_offset, lel.publication_time
      from par_linearized_event_log lel
      where lel.log_id = ${eventLogId.index} and lel.local_offset_effective_time = ${localOffset.effectiveTime} and lel.local_offset_tie_breaker = ${localOffset.tieBreaker}
      #${storage.limit(1)}
      """.as[(GlobalOffset, CantonTimestamp)].headOption,
      functionFullName,
    )
  }

  override def getOffsetByTimeUpTo(
      upToInclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): OptionT[Future, GlobalOffset] = {
    // The publication time increases with the global offset,
    // so we order first by the publication time so that the same index `idx_linearized_event_log_publication_time`
    // can be used for the where clause and the ordering
    val query =
      sql"""
          select global_offset
          from par_linearized_event_log
          where publication_time <= $upToInclusive
          order by publication_time desc, global_offset desc
          #${storage.limit(1)}
          """.as[GlobalOffset].headOption
    storage.querySingle(query, functionFullName)
  }

  override def getOffsetByTimeAtOrAfter(
      fromInclusive: CantonTimestamp
  )(implicit
      traceContext: TraceContext
  ): OptionT[Future, (GlobalOffset, EventLogId, LocalOffset)] = {
    // The publication time increases with the global offset,
    // so we order first by the publication time so that the same index `idx_linearized_event_log_publication_time`
    // can be used for the where clause and the ordering
    val query =
      sql"""
          select global_offset, log_id, local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker
          from par_linearized_event_log
          where publication_time >= $fromInclusive
          order by publication_time asc, global_offset asc
          #${storage.limit(1)}
          """.as[(GlobalOffset, Int, LocalOffset)].headOption
    storage.querySingle(query, functionFullName).flatMap { case (offset, logIndex, localOffset) =>
      EventLogId.fromDbLogIdOT("linearized_event_log", indexedStringStore)(logIndex).map { x =>
        (offset, x, localOffset)
      }
    }
  }

  override def lastGlobalOffset(upToInclusive: Option[GlobalOffset] = None)(implicit
      traceContext: TraceContext
  ): OptionT[Future, GlobalOffset] = {

    def query(upToInclusive: GlobalOffset): Future[Option[GlobalOffset]] =
      lastOffsetAndPublicationTime(storage, upToInclusive).map(_.map(_._1))

    upToInclusive match {
      case Some(upToInclusive) => OptionT(query(upToInclusive))

      case None =>
        val head: NonNegativeLong = dispatcher.getHead()

        OptionT.fromOption(
          Option.when(head > MultiDomainEventLog.ledgerFirstOffset.toNonNegative)(
            GlobalOffset.tryFromLong(head.unwrap) // head is at least 1
          )
        )
    }
  }

  /** Returns the `count` many last events in descending global offset order. */
  private def lastEvents(count: Int)(implicit
      traceContext: TraceContext
  ): Future[Seq[(GlobalOffset, EventLogId, LocalOffset, CantonTimestamp)]] = {
    val query = storage.profile match {
      case Profile.Oracle(_jdbc) =>
        sql"select * from ((select global_offset, log_id, local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker, publication_time from par_linearized_event_log order by global_offset desc)) where rownum < ${count + 1}"
          .as[(GlobalOffset, Int, LocalOffset, CantonTimestamp)]
      case _ =>
        sql"select global_offset, log_id, local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker, publication_time from par_linearized_event_log order by global_offset desc #${storage
            .limit(count)}"
          .as[(GlobalOffset, Int, LocalOffset, CantonTimestamp)]
    }
    storage.query(query, functionFullName).flatMap { vec =>
      vec.parTraverseFilter { case (offset, logId, localOffset, ts) =>
        EventLogId
          .fromDbLogIdOT("linearized event log", indexedStringStore)(logId)
          .map { evLogId =>
            (offset, evLogId, localOffset, ts)
          }
          .value
      }
    }
  }

  override def publicationTimeLowerBound: CantonTimestamp = publicationTimeLowerBoundRef.get()

  override def flush(): Future[Unit] = doFlush()

  override def reportMaxEventAgeMetric(oldestEventTimestamp: Option[CantonTimestamp]): Unit =
    MetricsHelper.updateAgeInHoursGauge(
      clock,
      metrics.pruning.maxEventAge,
      oldestEventTimestamp,
    )

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty.*
    List[AsyncOrSyncCloseable](
      SyncCloseable("eventsQueue.complete", eventsQueue.complete()),
      // The kill switch ensures that we stop processing the remaining entries in the queue.
      SyncCloseable("killSwitch.shutdown", killSwitch.shutdown()),
      AsyncCloseable(
        "done",
        done.map(_ => ()).recover {
          // The Pekko stream supervisor has already logged an exception as an error and stopped the stream
          case NonFatal(e) =>
            logger.debug(s"Ignored exception in Pekko stream done future during shutdown", e)
        },
        timeouts.shutdownShort,
      ),
    )
  }
}

object DbMultiDomainEventLog {

  def apply(
      storage: DbStorage,
      clock: Clock,
      metrics: ParticipantMetrics,
      timeouts: ProcessingTimeout,
      indexedStringStore: IndexedStringStore,
      loggerFactory: NamedLoggerFactory,
      participantEventLogId: ParticipantEventLogId,
      transferStoreFor: TargetDomainId => Either[String, TransferStore],
      maxBatchSize: PositiveInt = PositiveInt.tryCreate(1000),
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): Future[DbMultiDomainEventLog] =
    for {
      headAndPublicationTime <- lastOffsetAndPublicationTime(storage)
      localHeads <- lastLocalOffsets(storage)
    } yield {
      // We never purge the multi-domain event log completely, so if there ever was a publication time recorded,
      // we will find an upper bound.
      val initialPublicationTime = headAndPublicationTime.fold(CantonTimestamp.MinValue)(_._2)
      new DbMultiDomainEventLog(
        headAndPublicationTime.map(_._1),
        initialPublicationTime,
        localHeads,
        maxBatchSize = maxBatchSize,
        participantEventLogId = participantEventLogId,
        storage = storage,
        clock = clock,
        metrics = metrics,
        indexedStringStore = indexedStringStore,
        transferStoreFor = transferStoreFor,
        timeouts = timeouts,
        loggerFactory = loggerFactory,
      )
    }

  @VisibleForTesting
  private[db] def lastOffsetAndPublicationTime(
      storage: DbStorage,
      upToInclusive: GlobalOffset = GlobalOffset.MaxValue,
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): Future[Option[(GlobalOffset, CantonTimestamp)]] = {
    import storage.api.*

    val query =
      sql"""select global_offset, publication_time from par_linearized_event_log where global_offset <= $upToInclusive
            order by global_offset desc #${storage.limit(1)}"""
        .as[(GlobalOffset, CantonTimestamp)]
        .headOption
    storage.query(query, functionFullName)
  }

  private[db] def lastLocalOffsets(storage: DbStorage)(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
      closeContext: CloseContext,
  ): Future[TrieMap[Int, LocalOffset]] = {
    import storage.api.*

    storage.query(
      {
        for {
          /*
           We want the maximum local offset.
           Since global offset increases monotonically with the local offset on a given log, we sort by global offset.
           */
          rows <-
            sql"""select log_id, local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker
                 from par_linearized_event_log
                 where global_offset in (select max(global_offset) from par_linearized_event_log group by log_id)"""
              .as[(Int, LocalOffset)]
        } yield {
          val result = new TrieMap[Int, LocalOffset]()
          result ++= rows
          result
        }
      },
      functionFullName,
    )
  }
}
