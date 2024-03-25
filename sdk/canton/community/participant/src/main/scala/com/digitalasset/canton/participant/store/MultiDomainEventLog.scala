// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.{EitherT, OptionT}
import cats.syntax.option.*
import cats.syntax.parallel.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveLong}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.v2.ChangeId
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordOrderPublisher.PendingPublish
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.protocol.ProcessingSteps
import com.digitalasset.canton.participant.protocol.transfer.TransferData.{
  TransferInGlobalOffset,
  TransferOutGlobalOffset,
}
import com.digitalasset.canton.participant.store.EventLogId.{
  DomainEventLogId,
  ParticipantEventLogId,
}
import com.digitalasset.canton.participant.store.InFlightSubmissionStore.InFlightReference
import com.digitalasset.canton.participant.store.MultiDomainEventLog.OnPublish
import com.digitalasset.canton.participant.store.db.DbMultiDomainEventLog
import com.digitalasset.canton.participant.store.memory.InMemoryMultiDomainEventLog
import com.digitalasset.canton.participant.sync.TimestampedEvent.EventId
import com.digitalasset.canton.participant.sync.{
  LedgerSyncEvent,
  SyncDomainPersistentStateLookup,
  TimestampedEvent,
}
import com.digitalasset.canton.participant.{GlobalOffset, LocalOffset}
import com.digitalasset.canton.protocol.TargetDomainId
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{HasTraceContext, TraceContext, Traced}
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{DiscardOps, LedgerSubmissionId, LedgerTransactionId}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/** The multi domain event log merges the events from several [[SingleDimensionEventLog]]s to a single event stream.
  *
  * The underlying [[SingleDimensionEventLog]] either refer to a domain ("domain event log") or
  * to the underlying participant ("participant event log").
  *
  * Ordering guarantees:
  * 1. Events belonging to the same SingleDimensionEventLog have the same relative order in the MultiDomainEventLog
  * 2. Events are globally ordered such that any two (unpruned) events appear in the same relative order in different
  *    subscriptions and lookups.
  */
trait MultiDomainEventLog extends AutoCloseable { this: NamedLogging =>

  import MultiDomainEventLog.PublicationData

  def indexedStringStore: IndexedStringStore

  protected implicit def executionContext: ExecutionContext

  /** Appends a new event to the event log.
    *
    * The new event must already have been published in the [[SingleDimensionEventLog]] with id
    * [[MultiDomainEventLog.PublicationData.eventLogId]] at offset [[MultiDomainEventLog.PublicationData.localOffset]].
    *
    * The method is idempotent, i.e., it does nothing, if it is called twice with the same
    * [[com.digitalasset.canton.participant.store.EventLogId]] and
    * [[com.digitalasset.canton.participant.LocalOffset]].
    *
    * The returned future completes already when the event has been successfully inserted into the internal
    * (in-memory) inbox. Events will be persisted asynchronously. Actual publication has happened
    * before a subsequent flush call's future completes.
    *
    * The caller must await completion of the returned future before calling the method again.
    * Otherwise, the method may fail with an exception or return a failed future.
    * (This restriction arises, because some implementations will offer the event to an Pekko source queue, and the
    * number of concurrent offers for such queues is bounded.)
    *
    * The event log will stall, i.e., log an error and refuse to publish further events in the following cases:
    * <ul>
    *   <li>If an event cannot be persisted, even after retrying.</li>
    *   <li>If events are published out of order, i.e., `publish(id, o2).flatMap(_ => publish(id, o1))` with `o1 < o2`.
    *       Exception: The event log will not stall in case of republication of previously published events, i.e.,
    *       `publish(id, o1).flatMap(_ => publish(id, o2).flatMap(_ => publish(id, o1)))` will not stall the event log.
    *   </li>
    * </ul>
    */
  def publish(data: PublicationData): Future[Unit]

  /** Finds unpublished events in the single dimension event log.
    * More precisely, finds all events from `id` with:
    * <ul>
    * <li>local offset strictly greater than the local offset of the last published event from `id`</li>
    * <li>local offset smaller than or equal to `upToInclusiveO` (if defined).</li>
    * </ul>
    */
  def fetchUnpublished(id: EventLogId, upToInclusiveO: Option[LocalOffset])(implicit
      traceContext: TraceContext
  ): Future[Seq[PendingPublish]]

  /** Removes all events with offset up to `upToInclusive`. */
  def prune(upToInclusive: GlobalOffset)(implicit traceContext: TraceContext): Future[Unit]

  /** Yields an pekko source with all stored events, optionally starting from a given offset.
    * @throws java.lang.IllegalArgumentException if `startInclusive` is lower than [[MultiDomainEventLog.ledgerFirstOffset]].
    */
  def subscribe(startInclusive: Option[GlobalOffset])(implicit
      traceContext: TraceContext
  ): Source[(GlobalOffset, Traced[LedgerSyncEvent]), NotUsed]

  /** Yields all events with offset up to `upToInclusive`. */
  def lookupEventRange(upToInclusive: Option[GlobalOffset], limit: Option[Int])(implicit
      traceContext: TraceContext
  ): Future[Seq[(GlobalOffset, TimestampedEvent)]]

  /** Yields the global offset, event and publication time for all the published events with the given IDs.
    * Unpublished events are ignored.
    */
  def lookupByEventIds(eventIds: Seq[EventId])(implicit
      traceContext: TraceContext
  ): Future[Map[EventId, (GlobalOffset, TimestampedEvent, CantonTimestamp)]]

  /** Find the domain of a committed transaction. */
  def lookupTransactionDomain(transactionId: LedgerTransactionId)(implicit
      traceContext: TraceContext
  ): OptionT[Future, DomainId]

  /** Yields the greatest local offsets for the underlying [[SingleDimensionEventLog]] with global offset less than
    * or equal to `upToInclusive`.
    *
    * @return `(domainLastOffsets, participantLastOffset)`, where `domainLastOffsets` maps the domains in `domainIds`
    *         to the greatest local offset and the greatest request offset of the corresponding domain event log and
    *         `participantLastOffset` is the greatest participant offset.
    */
  def lastDomainOffsetsBeforeOrAtGlobalOffset(
      upToInclusive: GlobalOffset,
      domainIds: List[DomainId],
      participantEventLogId: ParticipantEventLogId,
  )(implicit
      traceContext: TraceContext
  ): Future[(Map[DomainId, Option[LocalOffset]], Option[LocalOffset])] = {
    for {
      domainLogIds <- domainIds.parTraverse(IndexedDomain.indexed(indexedStringStore))

      domainOffsets <- domainLogIds.parTraverse { domainId =>
        lastLocalOffset(
          DomainEventLogId(domainId),
          Option(upToInclusive),
        ).map(domainId.domainId -> _)
      }

      participantOffset <- lastLocalOffset(
        participantEventLogId,
        Some(upToInclusive),
      )
    } yield (domainOffsets.toMap, participantOffset)
  }

  /** Returns the greatest local offset of the [[SingleDimensionEventLog]] given by `eventLogId`, if any,
    * such that the following holds:
    * <ol>
    *   <li>The assigned global offset is below or at `upToInclusive`.</li>
    *   <li>The record time of the event is below or at `timestampInclusive` (if defined)</li>
    * </ol>
    */
  def lastLocalOffset(
      eventLogId: EventLogId,
      upToInclusive: Option[GlobalOffset] = None,
  )(implicit traceContext: TraceContext): Future[Option[LocalOffset]]

  /** Returns the greatest local offset of the [[SingleDimensionEventLog]] given by `eventLogId`, if any,
    * such that the following holds:
    * <ol>
    * <li>The assigned global offset is below or at `upToInclusive`.</li>
    * <li>The record time of the event is below or at `timestampInclusive` (if defined)</li>
    * </ol>
    */
  def lastLocalOffsetBeforeOrAt(
      eventLogId: EventLogId,
      upToInclusive: GlobalOffset,
      timestampInclusive: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Option[LocalOffset]]

  /** Yields the `deltaFromBeginning`-lowest global offset (if it exists).
    * I.e., `locateOffset(0)` yields the smallest offset, `localOffset(1)` the second smallest offset, and so on.
    */
  def locateOffset(deltaFromBeginning: Long)(implicit
      traceContext: TraceContext
  ): OptionT[Future, GlobalOffset]

  /** Yields the `skip`-lowest publication timestamp (if it exists).
    * I.e., `locatePruningTimestamp(0)` yields the smallest timestamp, `locatePruningTimestamp(1)` the second smallest timestamp, and so on.
    */
  def locatePruningTimestamp(skip: NonNegativeInt)(implicit
      traceContext: TraceContext
  ): OptionT[Future, CantonTimestamp]

  /** Returns the data associated with the given offset, if any */
  def lookupOffset(globalOffset: GlobalOffset)(implicit
      traceContext: TraceContext
  ): OptionT[Future, (EventLogId, LocalOffset, CantonTimestamp)]

  /** Returns the [[com.digitalasset.canton.participant.GlobalOffset]] under which the given local offset of
    * the given event log was published, if any, along with its publication time
    */
  def globalOffsetFor(eventLogId: EventLogId, localOffset: LocalOffset)(implicit
      traceContext: TraceContext
  ): Future[Option[(GlobalOffset, CantonTimestamp)]]

  /** Yields the largest global offset whose publication time is before or at `upToInclusive`, if any.
    * The publication time is measured on the participant's local clock.
    */
  def getOffsetByTimeUpTo(upToInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): OptionT[Future, GlobalOffset]

  /** Yields the smallest global offset whose publication time is at or after `fromInclusive`, if any.
    * The publication time is measured on the participant's local clock.
    */
  def getOffsetByTimeAtOrAfter(fromInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): OptionT[Future, (GlobalOffset, EventLogId, LocalOffset)]

  /** Yields the highest global offset up to the given bound, if any. */
  def lastGlobalOffset(upToInclusive: Option[GlobalOffset] = None)(implicit
      traceContext: TraceContext
  ): OptionT[Future, GlobalOffset]

  private val onPublish: AtomicReference[OnPublish] =
    new AtomicReference[OnPublish](OnPublish.DoNothing)

  /** Sets the listener to be called whenever events are published to the multi-domain event log. */
  def setOnPublish(newOnPublish: OnPublish): Unit = onPublish.set(newOnPublish)

  protected def notifyOnPublish(
      published: Seq[OnPublish.Publication]
  )(implicit traceContext: TraceContext): Unit =
    if (published.nonEmpty) {
      Try(onPublish.get().notify(published)).recover { case ex =>
        // If the observer throws, then we just log and carry on.
        logger.error(
          show"Notifying the MultiDomainEventLog listener failed for offsets ${published.map(_.globalOffset)}",
          ex,
        )
      }.discard
    }

  protected def transferStoreFor: TargetDomainId => Either[String, TransferStore]

  def notifyOnPublishTransfer(
      events: Seq[(LedgerSyncEvent.TransferEvent, GlobalOffset)]
  )(implicit traceContext: TraceContext): Future[Unit] = {

    events.groupBy { case (event, _) => event.targetDomain }.toList.parTraverse_ {
      case (targetDomain, eventsForDomain) =>
        lazy val updates = eventsForDomain
          .map { case (event, offset) => s"${event.transferId} (${event.kind}): $offset" }
          .mkString(", ")

        val res: EitherT[FutureUnlessShutdown, String, Unit] = for {
          transferStore <- EitherT
            .fromEither[FutureUnlessShutdown](transferStoreFor(targetDomain))
          offsets = eventsForDomain.map {
            case (out: LedgerSyncEvent.TransferredOut, offset) =>
              (out.transferId, TransferOutGlobalOffset(offset))
            case (in: LedgerSyncEvent.TransferredIn, offset) =>
              (in.transferId, TransferInGlobalOffset(offset))
          }
          _ = logger.debug(s"Updated global offsets for transfers: $updates")
          _ <- transferStore.addTransfersOffsets(offsets).leftMap(_.message)
        } yield ()

        EitherTUtil
          .toFutureUnlessShutdown(
            res.leftMap(err =>
              new RuntimeException(
                s"Unable to update global offsets for transfers ($updates): $err"
              )
            )
          )
          .onShutdown(
            throw new RuntimeException(
              "Notification upon published transfer aborted due to shutdown"
            )
          )
    }
  }

  /** Returns a lower bound on the latest publication time of a published event.
    * All events published later will receive the same or higher publication time.
    * Increases monotonically, even across restarts.
    */
  def publicationTimeLowerBound: CantonTimestamp

  /** Returns a future that completes after all publications have happened whose [[publish]] future has completed
    * before the call to flush.
    */
  private[store] def flush(): Future[Unit]

  /** Report the max-event-age metric based on the oldest event timestamp and the current clock time or
    * zero if no oldest timestamp exists (e.g. events fully pruned).
    */
  def reportMaxEventAgeMetric(oldestEventTimestamp: Option[CantonTimestamp]): Unit
}

object MultiDomainEventLog {

  val ledgerFirstOffset: GlobalOffset = GlobalOffset(PositiveLong.one)

  def create(
      syncDomainPersistentStates: SyncDomainPersistentStateLookup,
      participantEventLog: ParticipantEventLog,
      storage: Storage,
      clock: Clock,
      metrics: ParticipantMetrics,
      indexedStringStore: IndexedStringStore,
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      mat: Materializer,
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): Future[MultiDomainEventLog] = {

    storage match {
      case _: MemoryStorage =>
        val mdel =
          InMemoryMultiDomainEventLog(
            syncDomainPersistentStates,
            participantEventLog,
            clock,
            timeouts,
            TransferStore.transferStoreFor(syncDomainPersistentStates),
            indexedStringStore,
            metrics,
            futureSupervisor,
            loggerFactory,
          )
        Future.successful(mdel)
      case dbStorage: DbStorage =>
        DbMultiDomainEventLog(
          dbStorage,
          clock,
          metrics,
          timeouts,
          indexedStringStore,
          loggerFactory,
          participantEventLogId = participantEventLog.id,
          transferStoreFor = TransferStore.transferStoreFor(syncDomainPersistentStates),
        )
    }
  }

  final case class PublicationData(
      eventLogId: EventLogId,
      event: TimestampedEvent,
      inFlightReference: Option[InFlightReference],
  ) extends HasTraceContext {
    override def traceContext: TraceContext = event.traceContext
    def localOffset: LocalOffset = event.localOffset
  }

  /** Listener for publications of the multi-domain event log.
    *
    * Unlike [[MultiDomainEventLog.subscribe]], notification can be lost when the participant crashes.
    * Conversely, [[OnPublish.Publication]] contains more information than just the
    * [[com.digitalasset.canton.participant.sync.LedgerSyncEvent]].
    */
  trait OnPublish {

    /** This method is called after the [[MultiDomainEventLog]] has assigned
      * new [[com.digitalasset.canton.participant.GlobalOffset]]s to a batch of
      * [[com.digitalasset.canton.participant.sync.LedgerSyncEvent]]s.
      *
      * If this method throws a [[scala.util.control.NonFatal]] exception, the [[MultiDomainEventLog]]
      * logs and discards the exception. That is, throwing an exception does not interrupt processing or cause a shutdown.
      */
    def notify(published: Seq[OnPublish.Publication])(implicit
        batchTraceContext: TraceContext
    ): Unit
  }

  object OnPublish {
    final case class Publication(
        globalOffset: GlobalOffset,
        publicationTime: CantonTimestamp,
        inFlightReferenceO: Option[InFlightReference],
        deduplicationInfoO: Option[DeduplicationInfo],
        event: LedgerSyncEvent,
    ) extends PrettyPrinting {

      override def pretty: Pretty[Publication] = prettyOfClass(
        param("global offset", _.globalOffset),
        paramIfDefined("in-flight reference", _.inFlightReferenceO),
        paramIfDefined("deduplication info", _.deduplicationInfoO),
        param("event", _.event),
      )
    }

    case object DoNothing extends OnPublish {
      override def notify(published: Seq[OnPublish.Publication])(implicit
          batchTraceContext: TraceContext
      ): Unit = ()
    }
  }

  /** @param acceptance if true, then the command was accepted, if false, it was rejected
    */
  final case class DeduplicationInfo(
      changeId: ChangeId,
      submissionId: Option[LedgerSubmissionId],
      acceptance: Boolean,
      eventTraceContext: TraceContext,
  ) extends PrettyPrinting {
    override def pretty: Pretty[DeduplicationInfo] = prettyOfClass(
      param("change id", _.changeId),
      paramIfDefined("submission id", _.submissionId),
      param("acceptance", _.acceptance),
      param("event trace context", _.eventTraceContext),
    )
  }

  object DeduplicationInfo {

    /** If the event generates a command completion event,
      * returns the [[DeduplicationInfo]] for it.
      */
    def fromEvent(
        event: LedgerSyncEvent,
        eventTraceContext: TraceContext,
    ): Option[DeduplicationInfo] =
      event match {
        case accepted: LedgerSyncEvent.TransactionAccepted =>
          // The indexer outputs a completion event iff the completion info is set.
          accepted.completionInfoO.map { completionInfo =>
            val changeId = completionInfo.changeId
            DeduplicationInfo(
              changeId,
              completionInfo.submissionId,
              acceptance = true,
              eventTraceContext,
            )
          }

        case LedgerSyncEvent.CommandRejected(
              _recordTime,
              completionInfo,
              _reason,
              ProcessingSteps.RequestType.Transaction,
              _domainId,
            ) =>
          val changeId = completionInfo.changeId
          DeduplicationInfo(
            changeId,
            completionInfo.submissionId,
            acceptance = false,
            eventTraceContext,
          ).some

        case _ => None
      }

    def fromTimestampedEvent(event: TimestampedEvent): Option[DeduplicationInfo] =
      fromEvent(event.event, event.traceContext)
  }
}
