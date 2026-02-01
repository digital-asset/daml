// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus
import com.digitalasset.canton.participant.admin.party.PartyReplicator.AddPartyRequestId
import com.digitalasset.canton.participant.store.PartyReplicationStateManager.{
  Modification,
  OnlinePartyReplicationOperationName,
  pendingDbOperationFromStatus,
}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.store.{PendingOperation, PendingOperationStore}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, FutureUnlessShutdownUtil, SimpleExecutionQueue}

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.util.chaining.scalaUtilChainingOps

/** ACS replication progress specific read and write methods.
  */
sealed trait AcsReplicationProgress {
  def getAcsReplicationProgress(requestId: AddPartyRequestId)(implicit
      traceContext: TraceContext
  ): Option[PartyReplicationStatus.AcsReplicationProgress]

  def updateAcsReplicationProgress(
      requestId: AddPartyRequestId,
      progress: PartyReplicationStatus.AcsReplicationProgress,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit]
}

/** The [[PartyReplicationStateManager]] manages online party replication (OnPR) state in memory and
  * conditionally on disk (on behalf of OnPRs handled by this participant in the target participant
  * TP role). This lets TPs re-initiate interrupted OnPRs after restarts or crashes.
  *
  * Note:
  *   - On TPs state is kept both in memory and on disk, as the in-memory state includes the
  *     non-serializable and hence non-persistable party replication processors. The in-memory state
  *     is eagerly loaded from the store at startup and kept in sync with the db.
  *   - The [[com.digitalasset.canton.participant.admin.party.PartyReplicator]] owns adding and
  *     removing [[com.digitalasset.canton.participant.admin.party.PartyReplicationStatus]] entries
  *     as well as updates that add (set to Some(_)) or remove (set to None) optional status
  *     subcomponents such as
  *     [[com.digitalasset.canton.participant.admin.party.PartyReplicationStatus.replicationO]] and
  *     does so from a simple execution queue.
  *   - Other components such as the
  *     [[com.digitalasset.canton.participant.protocol.party.PartyReplicationTargetParticipantProcessor]]
  *     update individual status subcomponents set to Some(_) such as
  *     [[com.digitalasset.canton.participant.admin.party.PartyReplicationStatus.AcsReplicationProgress]]
  *     from a simple execution queue.
  *   - Given that multiple components perform updates (but not necessarily in a racy fashion),
  *     error on the side of safety and have only the update methods use the simple execution queue
  *     to avoid lost updates in the absence of atomic update support in the underlying db store.
  */
final class PartyReplicationStateManager(
    participantId: ParticipantId,
    storage: Storage,
    futureSupervisor: FutureSupervisor,
    exitOnFatalFailures: Boolean,
    override protected val loggerFactory: NamedLoggerFactory,
    override protected val timeouts: ProcessingTimeout,
)(implicit executionContext: ExecutionContext)
    extends AcsReplicationProgress
    with NamedLogging
    with FlagCloseable {

  private val partyReplications = new TrieMap[AddPartyRequestId, PartyReplicationStatus]()

  private val targetParticipantStore = PendingOperationStore.apply[PartyReplicationStatus](
    storage,
    timeouts,
    loggerFactory,
    PartyReplicationStatus,
  )

  // Execution queue serializes db and map access and modifications.
  private val executionQueue = new SimpleExecutionQueue(
    "party-replication-state",
    futureSupervisor,
    timeouts,
    loggerFactory,
    crashOnFailure = exitOnFatalFailures,
  ).tap { eq =>
    implicit val tc = TraceContext.empty
    FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
      eq.executeUS(
        targetParticipantStore
          .getAll(OnlinePartyReplicationOperationName)
          .map(
            _.foreach(op =>
              partyReplications
                .putIfAbsent(op.operation.params.requestId, op.operation)
                .foreach(duplicateStatus =>
                  throw new IllegalStateException(
                    s"Duplicate party replication ${op.operation.params.requestId} upon load should be impossible: existing ${duplicateStatus.params} new ${op.operation}"
                  )
                )
            )
          ),
        "load onpr map from store",
      ),
      "load onpr map from store failed",
    )
  }

  /** Add a previously unknown OnPR returning an error if a status with the same requestId is
    * already known.
    *
    * @param status
    *   the status of a new party replication, i.e. previously unknown
    * @return
    *   an "already exists" error if a status with the same request id already exists
    */
  def add(status: PartyReplicationStatus)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        partyReplications
          .get(status.params.requestId)
          .toLeft(())
          .leftMap(alreadyExists =>
            s"Party replication ${status.params.requestId} already exists: $alreadyExists"
          )
      )
      _ <- EitherTUtil.ifThenET(participantId == status.params.targetParticipantId)(
        targetParticipantStore
          .insert(pendingDbOperationFromStatus(status))
          .leftMap(alreadyExists =>
            s"Party replication ${status.params.requestId} already exists in the store: $alreadyExists"
          )
      )
    } yield {
      // Reflect new status in memory only once store has been (conditionally) updated.
      partyReplications.putIfAbsent(status.params.requestId, status).discard
    }

  /** Update an already known OnPR status returning an error if a status with the requestId is
    * unknown.
    *
    * @param requestId
    *   the request id of the status to update
    * @param modify
    *   a status to status modifier to alter the existing status within the simple execution queue.
    *   The update expressed by modify needs to be an idempotent update, i.e. setting a new counter
    *   value rather than incrementing the counter
    * @return
    *   the updated status
    */
  def update(requestId: AddPartyRequestId, modify: Modification)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, PartyReplicationStatus] = executionQueue.executeEUS(
    for {
      prevStatus <- EitherT.fromEither[FutureUnlessShutdown](
        partyReplications
          .get(requestId)
          .toRight(s"Party replication $requestId does not exist")
      )
      updatedStatus = modify(prevStatus)
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        prevStatus.params == updatedStatus.params,
        s"Party replication $requestId parameters ${prevStatus.params} not allowed to change ${updatedStatus.params}",
      )
      _ <- EitherTUtil.ifThenET(participantId == prevStatus.params.targetParticipantId)(
        EitherT.right[String](
          targetParticipantStore.updateOperation(
            updatedStatus,
            prevStatus.params.synchronizerId,
            OnlinePartyReplicationOperationName,
            prevStatus.params.requestId.toHexString,
          )
        )
      )
    } yield {
      // Reflect updated status in memory only once store has been updated (conditionally on the TP only).
      // Before the in-memory state has been updated, readers (see under "get" below) might obtain potentially
      // stale values. See the notes in the scaladoc above and under "get" below with respect to idempotent updates.
      updatedStatus.tap(partyReplications.put(requestId, _).discard)
    },
    s"update $requestId",
  )

  def update_(requestId: AddPartyRequestId, modify: Modification)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = update(requestId, modify).map(_ => ())

  /** Fetches a status if the request id exists. When the output is used to describe an update, it
    * is important that the update only modifies the portion of the status that is changing and does
    * so in an idempotent way, i.e. setting new values rather than incrementing a counter.
    */
  def get(requestId: AddPartyRequestId): Option[PartyReplicationStatus] =
    partyReplications.get(requestId)

  def collectFirst[T](
      f: PartialFunction[(AddPartyRequestId, PartyReplicationStatus), T]
  ): Option[T] = partyReplications.iterator.collectFirst(f)

  def collect[T](f: PartialFunction[(AddPartyRequestId, PartyReplicationStatus), T]): Seq[T] =
    partyReplications.iterator.collect(f).toSeq

  override def getAcsReplicationProgress(requestId: AddPartyRequestId)(implicit
      traceContext: TraceContext
  ): Option[PartyReplicationStatus.AcsReplicationProgress] =
    get(requestId).flatMap(_.replicationO)

  override def updateAcsReplicationProgress(
      requestId: AddPartyRequestId,
      progress: PartyReplicationStatus.AcsReplicationProgress,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    update_(requestId, _.modifyReplication(_ => progress))

  override protected def onClosed(): Unit =
    LifeCycle.close(executionQueue)(logger)
}

object PartyReplicationStateManager {
  type Modification = PartyReplicationStatus => PartyReplicationStatus

  private[PartyReplicationStateManager] lazy val OnlinePartyReplicationOperationName =
    NonEmptyString.tryCreate("online_party_replication")

  private[PartyReplicationStateManager] def pendingDbOperationFromStatus(
      status: PartyReplicationStatus
  ) =
    PendingOperation[PartyReplicationStatus](
      // Synchronizer reconnect triggers onpr activity, but is not the only trigger
      PendingOperation.PendingOperationTriggerType.SynchronizerReconnect,
      OnlinePartyReplicationOperationName,
      status.params.requestId.toHexString,
      status,
      status.params.synchronizerId,
    )
}
