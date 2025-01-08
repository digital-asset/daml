// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import cats.implicits.catsSyntaxParallelTraverse_
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.{Reassignment, Update}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentData.*
import com.digitalasset.canton.participant.protocol.reassignment.{
  IncompleteReassignmentData,
  ReassignmentData,
}
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateLookup
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.platform.indexer.parallel.ReassignmentOffsetPersistence
import com.digitalasset.canton.protocol.messages.DeliveredUnassignmentResult
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.{Checked, CheckedT, EitherTUtil, MonadUtil, OptionUtil}
import com.digitalasset.canton.{LfPartyId, RequestCounter}
import monocle.macros.syntax.lens.*

import scala.concurrent.{ExecutionContext, Future}

trait ReassignmentStore extends ReassignmentLookup {
  import ReassignmentStore.*

  /** Adds the reassignment to the store.
    *
    * Calls to this method are idempotent, independent of the order.
    * Differences in [[protocol.reassignment.ReassignmentData!.unassignmentResult]] between two calls are ignored
    * if the field is [[scala.None$]] in one of the calls. If applicable, the field content is merged.
    *
    * @throws java.lang.IllegalArgumentException if the reassignment's target domain is not
    *                                            the domain this [[ReassignmentStore]] belongs to.
    */
  def addReassignment(reassignmentData: ReassignmentData)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit]

  /** Adds the given [[com.digitalasset.canton.protocol.messages.ConfirmationResultMessage]] to the reassignment data in the store,
    * provided that the reassignment data has previously been stored.
    *
    * The same [[com.digitalasset.canton.protocol.messages.ConfirmationResultMessage]] can be added any number of times.
    * This includes unassignment results that are in the [[protocol.reassignment.ReassignmentData!.unassignmentResult]]
    * added with [[addReassignment]].
    *
    * @param unassignmentResult The unassignment result to add
    * @return [[ReassignmentStore.UnknownReassignmentId]] if the reassignment has not previously been added with [[addReassignment]].
    *         [[ReassignmentStore.UnassignmentResultAlreadyExists]] if a different unassignment result for the same
    *         reassignment request has been added before, including as part of [[addReassignment]].
    */
  def addUnassignmentResult(unassignmentResult: DeliveredUnassignmentResult)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit]

  /** Adds the given offsets to the reassignment data in the store, provided that the reassignment data has previously been stored.
    *
    * The same offset can be added any number of times.
    */
  def addReassignmentsOffsets(offsets: Map[ReassignmentId, ReassignmentGlobalOffset])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentStoreError, Unit]

  /** Adds the given [[com.digitalasset.canton.data.Offset]] for the reassignment events to the reassignment data in
    * the store, provided that the reassignment data has previously been stored.
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

  /** Marks the reassignment as completed, i.e., an assignment request was committed.
    * If the reassignment has already been completed then a [[ReassignmentStore.ReassignmentAlreadyCompleted]] is reported, and the
    * [[com.digitalasset.canton.participant.util.TimeOfChange]] of the completion is not changed from the old value.
    *
    * @param timeOfCompletion Provides the request counter and activeness time of the committed assignment request.
    */
  def completeReassignment(reassignmentId: ReassignmentId, timeOfCompletion: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[FutureUnlessShutdown, Nothing, ReassignmentStoreError, Unit]

  /** Removes the reassignment from the store,
    * when the unassignment request is rejected or the reassignment is pruned.
    */
  def deleteReassignment(reassignmentId: ReassignmentId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Removes all completions of reassignments that have been triggered by requests with at least the given counter.
    * This method must not be called concurrently with [[completeReassignment]], but may be called concurrently with
    * [[addReassignment]] and [[addUnassignmentResult]].
    *
    * Therefore, this method need not be linearizable w.r.t. [[completeReassignment]].
    * For example, if two requests `rc1` complete two reassignments while [[deleteCompletionsSince]] is running for
    * some `rc <= rc1, rc2`, then there are no guarantees which of the completions of `rc1` and `rc2` remain.
    */
  def deleteCompletionsSince(criterionInclusive: RequestCounter)(implicit
      traceContext: TraceContext
  ): Future[Unit]
}

object ReassignmentStore {
  def reassignmentStoreFor(
      syncDomainPersistentStates: SyncDomainPersistentStateLookup
  ): Target[SynchronizerId] => Either[String, ReassignmentStore] =
    (synchronizerId: Target[SynchronizerId]) =>
      syncDomainPersistentStates.getAll
        .get(synchronizerId.unwrap)
        .toRight(s"Unknown domain `${synchronizerId.unwrap}`")
        .map(_.reassignmentStore)

  def reassignmentOffsetPersistenceFor(
      syncDomainPersistentStates: SyncDomainPersistentStateLookup
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
        .groupBy { case (event, _) => event.reassignmentInfo.targetDomain }
        .toList
        .parTraverse_ { case (targetDomain, eventsForDomain) =>
          lazy val updates = eventsForDomain
            .map { case (event, offset) =>
              s"${event.reassignmentInfo.sourceDomain} ${event.reassignmentInfo.unassignId} (${event.reassignment}): $offset"
            }
            .mkString(", ")

          val res: EitherT[FutureUnlessShutdown, String, Unit] = for {
            reassignmentStore <- EitherT
              .fromEither[FutureUnlessShutdown](
                reassignmentStoreFor(syncDomainPersistentStates)(targetDomain)
              )
            offsets = eventsForDomain.map { case (reassignmentEvent, globalOffset) =>
              val reassignmentGlobal = reassignmentEvent.reassignment match {
                case _: Reassignment.Assign => AssignmentGlobalOffset(globalOffset)
                case _: Reassignment.Unassign => UnassignmentGlobalOffset(globalOffset)
              }
              ReassignmentId(
                reassignmentEvent.reassignmentInfo.sourceDomain,
                reassignmentEvent.reassignmentInfo.unassignId,
              ) -> reassignmentGlobal
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

  /** Merge the offsets corresponding to the same reassignment id.
    * Returns an error in case of inconsistent offsets.
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
      timeOfCompletion: TimeOfChange,
  ) extends ReassignmentLookupError {
    override def cause: String = "reassignment already completed"
  }

  final case class ReassignmentDataAlreadyExists(old: ReassignmentData, `new`: ReassignmentData)
      extends ReassignmentStoreError {
    def reassignmentId: ReassignmentId = old.reassignmentId

    override def message: String =
      s"Reassignment data for reassignment `$reassignmentId` already exists and differs from the new one"
  }

  final case class UnassignmentResultAlreadyExists(
      reassignmentId: ReassignmentId,
      old: DeliveredUnassignmentResult,
      `new`: DeliveredUnassignmentResult,
  ) extends ReassignmentStoreError {
    override def message: String =
      s"Unassignment result for reassignment `$reassignmentId` already exists and differs from the new one"
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
      newCompletion: TimeOfChange,
  ) extends ReassignmentStoreError {
    override def message: String = s"Reassignment `$reassignmentId` is already completed"
  }

  /** The data for a reassignment and possible when the reassignment was completed. */
  final case class ReassignmentEntry(
      reassignmentData: ReassignmentData,
      timeOfCompletion: Option[TimeOfChange],
  ) {
    def isCompleted: Boolean = timeOfCompletion.nonEmpty

    def mergeWith(
        other: ReassignmentEntry
    ): Checked[ReassignmentDataAlreadyExists, ReassignmentAlreadyCompleted, ReassignmentEntry] =
      for {
        mergedData <- Checked.fromEither(
          reassignmentData
            .mergeWith(other.reassignmentData)
            .toRight(ReassignmentDataAlreadyExists(reassignmentData, other.reassignmentData))
        )
        mergedToc <- OptionUtil
          .mergeEqual(timeOfCompletion, other.timeOfCompletion)
          .fold[
            Checked[ReassignmentDataAlreadyExists, ReassignmentAlreadyCompleted, Option[
              TimeOfChange
            ]]
          ] {
            val thisToC =
              timeOfCompletion.getOrElse(
                throw new IllegalStateException("Time of completion should be defined")
              )
            val otherToC =
              other.timeOfCompletion.getOrElse(
                throw new IllegalStateException("Time of completion should be defined")
              )

            Checked.continueWithResult(
              ReassignmentAlreadyCompleted(reassignmentData.reassignmentId, otherToC),
              Some(thisToC),
            )
          }(Checked.result)
      } yield
        if ((mergedData eq reassignmentData) && (mergedToc eq timeOfCompletion)) this
        else ReassignmentEntry(mergedData, mergedToc)

    private[store] def addUnassignmentResult(
        unassignmentResult: DeliveredUnassignmentResult
    ): Either[UnassignmentResultAlreadyExists, ReassignmentEntry] =
      reassignmentData
        .addUnassignmentResult(unassignmentResult)
        .toRight {
          val old = reassignmentData.unassignmentResult.getOrElse(
            throw new IllegalStateException("unassignment result should not be empty")
          )
          UnassignmentResultAlreadyExists(reassignmentData.reassignmentId, old, unassignmentResult)
        }
        .map(ReassignmentEntry(_, timeOfCompletion))

    private[store] def addUnassignmentGlobalOffset(
        offset: ReassignmentGlobalOffset
    ): Either[ReassignmentGlobalOffsetsMerge, ReassignmentEntry] = {

      val newGlobalOffsetE = reassignmentData.reassignmentGlobalOffset
        .fold[Either[ReassignmentGlobalOffsetsMerge, ReassignmentGlobalOffset]](Right(offset))(
          _.merge(offset).leftMap(
            ReassignmentGlobalOffsetsMerge(reassignmentData.reassignmentId, _)
          )
        )

      newGlobalOffsetE.map(newGlobalOffset =>
        this.focus(_.reassignmentData.reassignmentGlobalOffset).replace(Some(newGlobalOffset))
      )
    }

    def complete(
        timeOfChange: TimeOfChange
    ): Checked[ReassignmentDataAlreadyExists, ReassignmentAlreadyCompleted, ReassignmentEntry] =
      mergeWith(ReassignmentEntry(reassignmentData, Some(timeOfChange)))

    def clearCompletion: ReassignmentEntry = ReassignmentEntry(reassignmentData, None)
  }
}

trait ReassignmentLookup {
  import ReassignmentStore.*

  /** Looks up the given in-flight reassignment and returns the data associated with the reassignment.
    *
    * @return [[scala.Left$]]([[ReassignmentStore.UnknownReassignmentId]]) if the reassignment is unknown;
    *         [[scala.Left$]]([[ReassignmentStore.ReassignmentCompleted]]) if the reassignment has already been completed.
    */
  def lookup(reassignmentId: ReassignmentId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentLookupError, ReassignmentData]

  /** Find utility to look for in-flight reassignments.
    * Results need not be consistent with [[lookup]].
    */
  def find(
      filterSource: Option[Source[SynchronizerId]],
      filterRequestTimestamp: Option[CantonTimestamp],
      filterSubmitter: Option[LfPartyId],
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[ReassignmentData]]

  /** Find utility to look for in-flight reassignments.
    * Reassignments are ordered by the tuple (request timestamp, source synchronizer id), ie reassignments are ordered by request timestamps
    * and ties are broken with lexicographic ordering on synchronizer ids.
    *
    * The ordering here has been chosen to allow a participant to fetch all the pending reassignments. The ordering has to
    * be consistent accross calls and uniquely identify a pending reassignment, but is otherwise arbitrary.
    *
    * @param requestAfter optionally, specify a strict lower bound for the reassignments returned, according to the
    *                     (request timestamp, source synchronizer id) ordering
    * @param limit limit the number of results
    */
  def findAfter(requestAfter: Option[(CantonTimestamp, Source[SynchronizerId])], limit: Int)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[ReassignmentData]]

  /** Find utility to look for incomplete reassignments.
    * Reassignments are ordered by global offset.
    *
    * A reassignment `t` is considered as incomplete at offset `validAt` if only one of the two reassignment events
    * was emitted on the multi-domain event log at `validAt`. That is, one of the following hold:
    *   1. Only unassignment was emitted
    *       - `t.unassignmentGlobalOffset` is smaller or equal to `validAt`
    *       - `t.assignmentGlobalOffset` is null or greater than `validAt`
    *   2. Only assignment was emitted
    *       - `t.assignmentGlobalOffset` is smaller or equal to `validAt`
    *       - `t.unassignmentGlobalOffset` is null or greater than `validAt`
    *
    * In particular, for a reassignment to be considered incomplete at `validAt`, then exactly one of the two offsets
    * (unassignmentGlobalOffset, assignmentGlobalOffset) is not null and smaller or equal to `validAt`.
    *
    * @param sourceDomain if empty, select only reassignments whose source domain matches the given one
    * @param validAt select only reassignments that are successfully unassigned
    * @param stakeholders if non-empty, select only reassignments of contracts whose set of stakeholders
    *                     intersects `stakeholders`.
    * @param limit limit the number of results
    */
  def findIncomplete(
      sourceDomain: Option[Source[SynchronizerId]],
      validAt: Offset,
      stakeholders: Option[NonEmpty[Set[LfPartyId]]],
      limit: NonNegativeInt,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[IncompleteReassignmentData]]

  /** Find utility to look for the earliest incomplete reassignment w.r.t. the ledger end.
    * If an incomplete reassignment exists, the method returns the global offset of the incomplete reassignment for either the
    * unassignment or the assignment, whichever of these is not null, the reassignment id and the target synchronizer id.
    * It returns None if there is no incomplete reassignment (either because all reassignments are complete or are in-flight,
    * or because there are no reassignments), or the reassignment table is empty.
    */
  def findEarliestIncomplete()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(Offset, ReassignmentId, Target[SynchronizerId])]]

  /** Queries the reassignment ids for the given contract ids. Optional filtering by unassignment and
    * completion (assignment) timestamps, and by source domain.
    */
  def findContractReassignmentId(
      contractIds: Seq[LfContractId],
      sourceDomain: Option[Source[SynchronizerId]],
      unassignmentTs: Option[CantonTimestamp],
      completionTs: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, Seq[ReassignmentId]]]
}
