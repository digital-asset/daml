// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import cats.implicits.catsSyntaxParallelTraverse_
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.{CantonTimestamp, FullUnassignmentTree, Offset}
import com.digitalasset.canton.ledger.participant.state.{Reassignment, Update}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentData.*
import com.digitalasset.canton.participant.protocol.reassignment.{
  AssignmentData,
  IncompleteReassignmentData,
  UnassignmentData,
}
import com.digitalasset.canton.participant.sync.SyncPersistentStateLookup
import com.digitalasset.canton.platform.indexer.parallel.ReassignmentOffsetPersistence
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId, SerializableContract}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.{CheckedT, EitherTUtil, MonadUtil}
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future}

trait ReassignmentStore extends ReassignmentLookup {
  import ReassignmentStore.*

  /** Adds the unassignment data to the store.
    *
    * Calls to this method are idempotent, independent of the order.
    * @throws java.lang.IllegalArgumentException
    *   if the reassignment's target synchronizer is not the synchronizer this [[ReassignmentStore]]
    *   belongs to.
    */
  def addUnassignmentData(unassignmentData: UnassignmentData)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit]

  /** Adds the given offsets to the reassignment data in the store.
    *
    * The same offset can be added any number of times.
    */
  def addReassignmentsOffsets(offsets: Map[ReassignmentId, ReassignmentGlobalOffset])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit]

  /** Adds the given [[com.digitalasset.canton.data.Offset]] for the reassignment events to the
    * reassignment data in the store, provided that the reassignment data has previously been
    * stored.
    *
    * The same [[com.digitalasset.canton.data.Offset]] can be added any number of times.
    */
  def addReassignmentsOffsets(
      events: Seq[(ReassignmentId, ReassignmentGlobalOffset)]
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit] =
    for {
      preparedOffsets <- EitherT.fromEither[FutureUnlessShutdown](mergeReassignmentOffsets(events))
      _ <- addReassignmentsOffsets(preparedOffsets)
    } yield ()

  /** Marks the reassignment as completed, i.e., an assignment request was committed. If the
    * reassignment has already been completed then a
    * [[ReassignmentStore.ReassignmentAlreadyCompleted]] is reported, and the
    * [[com.digitalasset.canton.data.CantonTimestamp]] of the completion is not changed from the old
    * value.
    *
    * @param tsCompletion
    *   Provides the activeness timestamp of the committed assignment request.
    */
  def completeReassignment(reassignmentId: ReassignmentId, tsCompletion: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, Nothing, ReassignmentStoreError, Unit]

  /** Removes all completions of reassignments that have been triggered by requests with at least
    * the given timestamp. This method must not be called concurrently with
    * [[completeReassignment]], but may be called concurrently with [[addUnassignmentData]].
    *
    * Therefore, this method need not be linearizable w.r.t. [[completeReassignment]]. For example,
    * if two requests at `ts1` and `ts2` complete two reassignments while [[deleteCompletionsSince]]
    * is running for some `ts <= ts1, ts2`, then there are no guarantees which of the completions at
    * `ts1` and `ts2` remain.
    */
  def deleteCompletionsSince(criterionInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** In certain special cases, where an assignment starts before the unassignment has had a chance
    * to write the reassignment data (either due to slow processing on the source synchronizer or
    * simply because the participant is disconnected from the source synchronizer), we want to
    * insert assignment data to allow the assignment to complete.
    */
  def addAssignmentDataIfAbsent(assignmentData: AssignmentData)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit]

  /** Removes the reassignment from the store, when the unassignment request is rejected or the
    * reassignment is pruned.
    */
  @VisibleForTesting
  def deleteReassignment(reassignmentId: ReassignmentId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]
}

object ReassignmentStore {
  def reassignmentStoreFor(
      syncPersistentStateLookup: SyncPersistentStateLookup
  ): Target[SynchronizerId] => Either[String, ReassignmentStore] =
    (synchronizerId: Target[SynchronizerId]) =>
      syncPersistentStateLookup.getAll
        .get(synchronizerId.unwrap)
        .toRight(s"Unknown synchronizer `${synchronizerId.unwrap}`")
        .map(_.reassignmentStore)

  def reassignmentOffsetPersistenceFor(
      syncPersistentStateLookup: SyncPersistentStateLookup
  )(implicit
      executionContext: ExecutionContext
  ): ReassignmentOffsetPersistence = new ReassignmentOffsetPersistence {
    override def persist(
        updates: Seq[(Offset, Update)],
        tracedLogger: TracedLogger,
    )(implicit traceContext: TraceContext): Future[Unit] =
      updates
        .collect {
          case (offset, reassignmentAccepted: Update.ReassignmentAccepted)
              if reassignmentAccepted.reassignmentInfo.isReassigningParticipant =>
            (reassignmentAccepted, offset)
        }
        .groupBy { case (event, _) => event.reassignmentInfo.targetSynchronizer }
        .toList
        .parTraverse_ { case (targetSynchronizer, eventsForSynchronizer) =>
          lazy val updates = eventsForSynchronizer
            .map { case (event, offset) =>
              s"${event.reassignmentInfo.sourceSynchronizer} ${event.reassignmentInfo.unassignId} (${event.reassignment}): $offset"
            }
            .mkString(", ")

          val res: EitherT[FutureUnlessShutdown, String, Unit] = for {
            reassignmentStore <- EitherT
              .fromEither[FutureUnlessShutdown](
                reassignmentStoreFor(syncPersistentStateLookup)(targetSynchronizer)
              )
            offsets = eventsForSynchronizer.flatMap { case (reassignmentEvent, globalOffset) =>
              reassignmentGlobalOffset(reassignmentEvent.reassignment, globalOffset).map {
                ReassignmentId(
                  reassignmentEvent.reassignmentInfo.sourceSynchronizer,
                  reassignmentEvent.reassignmentInfo.unassignId,
                ) -> _
              }
            }
            _ = tracedLogger.debug(s"Updated global offsets for reassignments: $updates")
            _ <- reassignmentStore.addReassignmentsOffsets(offsets).leftMap(_.message)
          } yield ()

          EitherTUtil
            .toFutureUnlessShutdown(
              res.leftMap(err =>
                new RuntimeException(
                  s"Unable to update global offsets for reassignments ($updates): $err"
                )
              )
            )
            .onShutdown(
              throw new RuntimeException(
                "Notification upon published reassignment aborted due to shutdown"
              )
            )
        }
  }

  def reassignmentGlobalOffset(
      reassignment: Reassignment.Batch,
      offset: Offset,
  ): Option[ReassignmentGlobalOffset] =
    reassignment.headOption.map { // The batch will all have the same kind
      case _: Reassignment.Assign => AssignmentGlobalOffset(offset)
      case _: Reassignment.Unassign => UnassignmentGlobalOffset(offset)
    }

  /** Merge the offsets corresponding to the same reassignment id. Returns an error in case of
    * inconsistent offsets.
    */
  def mergeReassignmentOffsets(
      events: Seq[(ReassignmentId, ReassignmentGlobalOffset)]
  ): Either[ConflictingGlobalOffsets, Map[ReassignmentId, ReassignmentGlobalOffset]] = {
    type Acc = Map[ReassignmentId, ReassignmentGlobalOffset]
    val zero: Acc = Map.empty[ReassignmentId, ReassignmentGlobalOffset]

    MonadUtil.foldLeftM[Either[
      ConflictingGlobalOffsets,
      *,
    ], Acc, (ReassignmentId, ReassignmentGlobalOffset)](
      zero,
      events,
    ) { case (acc, (reassignmentId, offset)) =>
      val newOffsetE = acc.get(reassignmentId) match {
        case Some(value) =>
          value.merge(offset).leftMap(ConflictingGlobalOffsets(reassignmentId, _))
        case None => Right(offset)
      }

      newOffsetE.map(newOffset => acc + (reassignmentId -> newOffset))
    }
  }

  sealed trait ReassignmentStoreError extends Product with Serializable {
    def message: String
  }
  sealed trait ReassignmentLookupError extends ReassignmentStoreError {
    def cause: String
    def reassignmentId: ReassignmentId
    def message: String = s"Cannot lookup for reassignment `$reassignmentId`: $cause"
  }

  final case class UnknownReassignmentId(reassignmentId: ReassignmentId)
      extends ReassignmentLookupError {
    override def cause: String = "unknown reassignment id"
  }

  final case class ReassignmentCompleted(
      reassignmentId: ReassignmentId,
      tsCompletion: CantonTimestamp,
  ) extends ReassignmentLookupError {
    override def cause: String = "reassignment already completed"
  }

  final case class AssignmentStartingBeforeUnassignment(
      reassignmentId: ReassignmentId
  ) extends ReassignmentLookupError {
    override def cause: String = "assignment already completed before unassignment"
  }

  final case class ReassignmentGlobalOffsetsMerge(
      reassignmentId: ReassignmentId,
      error: String,
  ) extends ReassignmentStoreError {
    override def message: String =
      s"Unable to merge global offsets for reassignment `$reassignmentId`: $error"
  }

  final case class ConflictingGlobalOffsets(reassignmentId: ReassignmentId, error: String)
      extends ReassignmentStoreError {
    override def message: String = s"Conflicting global offsets for $reassignmentId: $error"
  }

  final case class ReassignmentAlreadyCompleted(
      reassignmentId: ReassignmentId,
      newCompletion: CantonTimestamp,
  ) extends ReassignmentStoreError {
    override def message: String = s"Reassignment `$reassignmentId` is already completed"
  }

  /** The data for a reassignment and possible when the reassignment was completed. */
  final case class ReassignmentEntry(
      reassignmentId: ReassignmentId,
      contracts: NonEmpty[Seq[SerializableContract]],
      unassignmentRequest: Option[FullUnassignmentTree],
      reassignmentGlobalOffset: Option[ReassignmentGlobalOffset],
      assignmentTs: Option[CantonTimestamp],
  ) {
    def unassignmentDataO: Option[UnassignmentData] =
      unassignmentRequest.map(UnassignmentData(reassignmentId, _))
    def unassignmentTs: CantonTimestamp = reassignmentId.unassignmentTs
    def sourceSynchronizer: Source[SynchronizerId] = reassignmentId.sourceSynchronizer
    def unassignmentGlobalOffset: Option[Offset] = reassignmentGlobalOffset.flatMap(_.unassignment)
    def assignmentGlobalOffset: Option[Offset] = reassignmentGlobalOffset.flatMap(_.assignment)

    def clearCompletion: ReassignmentEntry = this.copy(assignmentTs = None)
  }

  object ReassignmentEntry {
    def apply(
        reassignmentData: UnassignmentData,
        reassignmentGlobalOffset: Option[ReassignmentGlobalOffset],
        tsCompletion: Option[CantonTimestamp],
    ): ReassignmentEntry =
      ReassignmentEntry(
        reassignmentData.reassignmentId,
        reassignmentData.contracts.contracts.map(_.contract),
        Some(reassignmentData.unassignmentRequest),
        reassignmentGlobalOffset,
        tsCompletion,
      )

    def apply(
        assignmentData: AssignmentData,
        reassignmentGlobalOffset: Option[ReassignmentGlobalOffset],
        tsCompletion: Option[CantonTimestamp],
    ): ReassignmentEntry =
      ReassignmentEntry(
        assignmentData.reassignmentId,
        assignmentData.contracts.contracts.map(_.contract),
        None,
        reassignmentGlobalOffset,
        tsCompletion,
      )
  }
}

trait ReassignmentLookup {
  import ReassignmentStore.*

  /** Looks up the given in-flight reassignment and returns the data associated with the
    * reassignment.
    *
    * @return
    *   [[scala.Left$]]([[ReassignmentStore.UnknownReassignmentId]]) if the reassignment is unknown;
    *   [[scala.Left$]]([[ReassignmentStore.ReassignmentCompleted]]) if the reassignment has already
    *   been completed.
    */
  def lookup(reassignmentId: ReassignmentId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentLookupError, UnassignmentData]

  /** Find utility to look for in-flight reassignments. Reassignments are ordered by the tuple
    * (request timestamp, source synchronizer id), ie reassignments are ordered by request
    * timestamps and ties are broken with lexicographic ordering on synchronizer ids.
    *
    * The ordering here has been chosen to allow a participant to fetch all the pending
    * reassignments. The ordering has to be consistent accross calls and uniquely identify a pending
    * reassignment, but is otherwise arbitrary.
    *
    * @param requestAfter
    *   optionally, specify a strict lower bound for the reassignments returned, according to the
    *   (request timestamp, source synchronizer id) ordering
    * @param limit
    *   limit the number of results
    */
  def findAfter(requestAfter: Option[(CantonTimestamp, Source[SynchronizerId])], limit: Int)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[UnassignmentData]]

  /** Find utility to look for incomplete reassignments. Reassignments are ordered by global offset.
    *
    * A reassignment `t` is considered as incomplete at offset `validAt` if only one of the two
    * reassignment events was emitted on the indexer at `validAt`. That is, one of the following
    * hold:
    *   1. Only unassignment was emitted
    *      - `t.unassignmentGlobalOffset` is smaller or equal to `validAt`
    *      - `t.assignmentGlobalOffset` is null or greater than `validAt` 2. Only assignment was
    *        emitted
    *      - `t.assignmentGlobalOffset` is smaller or equal to `validAt`
    *      - `t.unassignmentGlobalOffset` is null or greater than `validAt`
    *
    * In particular, for a reassignment to be considered incomplete at `validAt`, then exactly one
    * of the two offsets (unassignmentGlobalOffset, assignmentGlobalOffset) is not null and smaller
    * or equal to `validAt`.
    *
    * @param sourceSynchronizer
    *   if empty, select only reassignments whose source synchronizer matches the given one
    * @param validAt
    *   select only reassignments that are successfully unassigned
    * @param stakeholders
    *   if non-empty, select only reassignments of contracts whose set of stakeholders intersects
    *   `stakeholders`.
    * @param limit
    *   limit the number of results
    */
  def findIncomplete(
      sourceSynchronizer: Option[Source[SynchronizerId]],
      validAt: Offset,
      stakeholders: Option[NonEmpty[Set[LfPartyId]]],
      limit: NonNegativeInt,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[IncompleteReassignmentData]]

  /** Find utility to look for the earliest incomplete reassignment w.r.t. the ledger end. If an
    * incomplete reassignment exists, the method returns the global offset of the incomplete
    * reassignment for either the unassignment or the assignment, whichever of these is not null,
    * the reassignment id and the target synchronizer id. It returns None if there is no incomplete
    * reassignment (either because all reassignments are complete or are in-flight, or because there
    * are no reassignments), or the reassignment table is empty.
    */
  def findEarliestIncomplete()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(Offset, ReassignmentId, Target[SynchronizerId])]]

  /** Queries the reassignment ids for the given contract ids. Optional filtering by unassignment
    * and completion (assignment) timestamps, and by source synchronizer.
    */
  def findContractReassignmentId(
      contractIds: Seq[LfContractId],
      sourceSynchronizer: Option[Source[SynchronizerId]],
      unassignmentTs: Option[CantonTimestamp],
      completionTs: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, Seq[ReassignmentId]]]

  @VisibleForTesting
  def findReassignmentEntry(reassignmentId: ReassignmentId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, UnknownReassignmentId, ReassignmentEntry]

  @VisibleForTesting
  def listInFlightReassignmentIds()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[ReassignmentId]]
}
