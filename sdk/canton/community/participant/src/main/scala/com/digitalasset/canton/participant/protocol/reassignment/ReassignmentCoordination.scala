// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.instances.future.catsStdInstancesForFuture
import cats.syntax.functor.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{
  SyncCryptoApiParticipantProvider,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.data.{
  CantonTimestamp,
  ContractsReassignmentBatch,
  ReassignmentSubmitterMetadata,
  UnassignmentData,
}
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.ReassignmentSynchronizer
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.{
  ApplicationShutdown,
  ReassignmentProcessorError,
  ReassignmentStoreFailed,
  UnknownPhysicalSynchronizer,
  UnknownSynchronizer,
}
import com.digitalasset.canton.participant.store.ReassignmentStore
import com.digitalasset.canton.participant.sync.{
  StaticSynchronizerParametersGetter,
  SyncPersistentStateManager,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.protocol.TimeProof
import com.digitalasset.canton.time.SynchronizerTimeTracker
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.SingletonTraverse.syntax.*
import com.digitalasset.canton.util.{ReassignmentTag, SameReassignmentType, SingletonTraverse}

import scala.concurrent.{ExecutionContext, Future}

class ReassignmentCoordination(
    reassignmentStoreFor: Target[SynchronizerId] => Either[
      ReassignmentProcessorError,
      ReassignmentStore,
    ],
    recentTimeProofFor: RecentTimeProofProvider,
    reassignmentSubmissionFor: PhysicalSynchronizerId => Option[ReassignmentSubmissionHandle],
    pendingUnassignments: Source[SynchronizerId] => Option[ReassignmentSynchronizer],
    staticSynchronizerParametersGetter: StaticSynchronizerParametersGetter,
    syncCryptoApi: SyncCryptoApiParticipantProvider,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  def addPendingUnassignment(
      reassignmentId: ReassignmentId,
      sourcePSId: Source[SynchronizerId],
  ): Unit =
    pendingUnassignments(sourcePSId)
      .foreach(_.add(reassignmentId))

  def completeUnassignment(
      reassignmentId: ReassignmentId,
      sourcePSId: Source[PhysicalSynchronizerId],
  ): Unit =
    pendingUnassignments(sourcePSId.map(_.logical))
      .foreach(_.completePhase7(reassignmentId))

  // Todo(i25029): Add the ability to interrupt the wait for unassignment completion in specific cases
  def waitForStartedUnassignmentToCompletePhase7(
      reassignmentId: ReassignmentId,
      sourcePSId: Source[PhysicalSynchronizerId],
  ): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.outcomeF(
      pendingUnassignments(sourcePSId.map(_.logical))
        .flatMap(_.find(reassignmentId))
        .getOrElse(Future.successful(()))
    )

  private[reassignment] def awaitSynchronizerTime(
      psid: ReassignmentTag[PhysicalSynchronizerId],
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, UnknownPhysicalSynchronizer, Unit] =
    reassignmentSubmissionFor(psid.unwrap) match {
      case Some(handle) =>
        handle.timeTracker.requestTick(timestamp, immediately = true).discard
        EitherT.right(handle.timeTracker.awaitTick(timestamp).getOrElse(Future.unit))
      case None =>
        EitherT.leftT(
          UnknownPhysicalSynchronizer(
            psid.unwrap,
            s"Unable to find synchronizer when awaiting synchronizer time $timestamp.",
          )
        )
    }

  /** Returns a future that completes when it is safe to take an identity snapshot for the given
    * `timestamp` on the given `synchronizerId`. [[scala.None$]] indicates that this point has
    * already been reached before the call. [[scala.Left$]] if the `synchronizer` is unknown or the
    * participant is not connected to the synchronizer.
    */
  private[reassignment] def awaitTimestamp[T[X] <: ReassignmentTag[X]: SameReassignmentType](
      synchronizerId: T[PhysicalSynchronizerId],
      staticSynchronizerParameters: T[StaticSynchronizerParameters],
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): Either[ReassignmentProcessorError, Option[FutureUnlessShutdown[Unit]]] =
    (for {
      cryptoApi <- syncCryptoApi.forSynchronizer(
        synchronizerId.unwrap,
        staticSynchronizerParameters.unwrap,
      )
      handle <- reassignmentSubmissionFor(synchronizerId.unwrap)
    } yield {
      handle.timeTracker.requestTick(timestamp, immediately = true).discard
      cryptoApi.awaitTimestamp(timestamp)
    }).toRight(UnknownPhysicalSynchronizer(synchronizerId.unwrap, "When waiting for timestamp"))

  /** Similar to [[awaitTimestamp]] but lifted into an [[EitherT]]
    *
    * @param onImmediate
    *   A callback that will be invoked if no wait was actually needed
    */
  private[reassignment] def awaitTimestamp[T[X] <: ReassignmentTag[X]: SameReassignmentType](
      synchronizerId: T[PhysicalSynchronizerId],
      staticSynchronizerParameters: T[StaticSynchronizerParameters],
      timestamp: CantonTimestamp,
      onImmediate: => FutureUnlessShutdown[Unit],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
    for {
      timeout <- EitherT.fromEither[FutureUnlessShutdown](
        awaitTimestamp(synchronizerId, staticSynchronizerParameters, timestamp)
      )
      _ <- EitherT.right[ReassignmentProcessorError](timeout.getOrElse(onImmediate))
    } yield ()

  /** Submits an assignment. Used by the [[UnassignmentProcessingSteps]] to automatically trigger
    * the submission of an assignment after the exclusivity timeout.
    */
  private[reassignment] def assign(
      targetSynchronizerId: Target[PhysicalSynchronizerId],
      submitterMetadata: ReassignmentSubmitterMetadata,
      reassignmentId: ReassignmentId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentProcessorError, AssignmentProcessingSteps.SubmissionResult] = {
    logger.debug(s"Triggering automatic assignment of reassignment `$reassignmentId`")

    for {
      inSubmission <- EitherT.fromEither[Future](
        reassignmentSubmissionFor(targetSynchronizerId.unwrap).toRight(
          UnknownPhysicalSynchronizer(targetSynchronizerId.unwrap, "When submitting assignment")
        )
      )
      submissionResult <- inSubmission
        .submitAssignments(
          submitterMetadata,
          reassignmentId,
        )
        .mapK(FutureUnlessShutdown.outcomeK)
        .semiflatMap(Predef.identity)
        .onShutdown(Left(ApplicationShutdown))
    } yield submissionResult
  }

  private[reassignment] def getStaticSynchronizerParameter[T[_]: SingletonTraverse](
      psid: T[PhysicalSynchronizerId]
  ): EitherT[FutureUnlessShutdown, UnknownPhysicalSynchronizer, T[StaticSynchronizerParameters]] =
    psid.traverseSingleton { (_, synchronizerId) =>
      EitherT.fromOption[FutureUnlessShutdown](
        staticSynchronizerParametersGetter.staticSynchronizerParameters(synchronizerId),
        UnknownPhysicalSynchronizer(synchronizerId, "getting static synchronizer parameters"),
      )
    }

  /** Returns a [[crypto.SynchronizerSnapshotSyncCryptoApi]] for the given `synchronizer` at the
    * given timestamp. The returned future fails with [[java.lang.IllegalArgumentException]] if the
    * `synchronizer` has not progressed far enough such that it can compute the snapshot. Use
    * [[awaitTimestamp]] to ensure progression to `timestamp`.
    */
  private[reassignment] def cryptoSnapshot[
      T[X] <: ReassignmentTag[X]: SameReassignmentType: SingletonTraverse
  ](
      psid: T[PhysicalSynchronizerId],
      staticSynchronizerParameters: T[StaticSynchronizerParameters],
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, T[
    SynchronizerSnapshotSyncCryptoApi
  ]] =
    EitherT
      .fromEither[FutureUnlessShutdown](
        // we use traverseSingleton to avoid wartremover warning about FutureTraverse
        psid.traverseSingleton { (_, synchronizerId) =>
          syncCryptoApi
            .forSynchronizer(synchronizerId, staticSynchronizerParameters.unwrap)
            .toRight(
              UnknownPhysicalSynchronizer(
                synchronizerId,
                "When getting crypto snapshot",
              ): ReassignmentProcessorError
            )
        }
      )
      .semiflatMap(
        _.traverseSingleton((_, syncCrypto) => syncCrypto.snapshot(timestamp))
      )

  private[reassignment] def awaitTimestampAndGetTaggedCryptoSnapshot[T[X] <: ReassignmentTag[X]
    : SameReassignmentType: SingletonTraverse](
      targetSynchronizerId: T[PhysicalSynchronizerId],
      staticSynchronizerParameters: T[StaticSynchronizerParameters],
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, T[
    SynchronizerSnapshotSyncCryptoApi
  ]] =
    for {
      _ <- awaitTimestamp(
        targetSynchronizerId,
        staticSynchronizerParameters,
        timestamp,
        FutureUnlessShutdown.unit,
      )
      snapshot <- cryptoSnapshot(
        targetSynchronizerId,
        staticSynchronizerParameters,
        timestamp,
      )
    } yield snapshot

  private[reassignment] def getTimeProofAndSnapshot(
      targetSynchronizerId: Target[PhysicalSynchronizerId],
      staticSynchronizerParameters: Target[StaticSynchronizerParameters],
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ReassignmentProcessorError,
    (TimeProof, Target[SynchronizerSnapshotSyncCryptoApi]),
  ] =
    for {
      timeProof <- recentTimeProofFor.get(targetSynchronizerId, staticSynchronizerParameters)
      // Since events are stored before they are processed, we wait just to be sure.
      targetCrypto <- awaitTimestampAndGetTaggedCryptoSnapshot(
        targetSynchronizerId,
        staticSynchronizerParameters,
        timeProof.timestamp,
      )
    } yield (timeProof, targetCrypto)

  /** Stores the given reassignment data on the target synchronizer. */
  private[reassignment] def addUnassignmentRequest(
      unassignmentData: UnassignmentData
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
    for {
      reassignmentStore <- EitherT.fromEither[FutureUnlessShutdown](
        reassignmentStoreFor(unassignmentData.targetPSId.map(_.logical))
      )
      _ <- reassignmentStore
        .addUnassignmentData(unassignmentData)
        .leftMap[ReassignmentProcessorError](
          ReassignmentStoreFailed(unassignmentData.reassignmentId, _)
        )
    } yield ()

  /** Stores the given assignment data on the target synchronizer. */
  private[reassignment] def addAssignmentData(
      reassignmentId: ReassignmentId,
      contracts: ContractsReassignmentBatch,
      source: Source[SynchronizerId],
      target: Target[SynchronizerId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
    for {
      reassignmentStore <- EitherT.fromEither[FutureUnlessShutdown](
        reassignmentStoreFor(target)
      )

      _ <- reassignmentStore
        .addAssignmentDataIfAbsent(
          AssignmentData(
            reassignmentId = reassignmentId,
            sourceSynchronizer = source,
            contracts = contracts,
          )
        )
        .leftMap[ReassignmentProcessorError](
          ReassignmentStoreFailed(reassignmentId, _)
        )
    } yield ()

}

object ReassignmentCoordination {
  def apply(
      reassignmentTimeProofFreshnessProportion: NonNegativeInt,
      syncPersistentStateManager: SyncPersistentStateManager,
      submissionHandles: PhysicalSynchronizerId => Option[ReassignmentSubmissionHandle],
      pendingUnassignments: Source[SynchronizerId] => Option[ReassignmentSynchronizer],
      syncCryptoApi: SyncCryptoApiParticipantProvider,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): ReassignmentCoordination = {
    def reassignmentStoreFor(
        synchronizerId: Target[SynchronizerId]
    ): Either[UnknownSynchronizer, ReassignmentStore] =
      syncPersistentStateManager
        .reassignmentStore(synchronizerId.unwrap)
        .toRight(UnknownSynchronizer(synchronizerId.unwrap, "looking for reassignment store"))

    val recentTimeProofProvider = new RecentTimeProofProvider(
      submissionHandles,
      syncCryptoApi,
      loggerFactory,
      reassignmentTimeProofFreshnessProportion,
    )

    new ReassignmentCoordination(
      reassignmentStoreFor = reassignmentStoreFor,
      recentTimeProofFor = recentTimeProofProvider,
      reassignmentSubmissionFor = submissionHandles,
      pendingUnassignments = pendingUnassignments,
      staticSynchronizerParametersGetter = syncPersistentStateManager,
      syncCryptoApi = syncCryptoApi,
      loggerFactory = loggerFactory,
    )
  }
}

trait ReassignmentSubmissionHandle {
  def timeTracker: SynchronizerTimeTracker

  def submitUnassignments(
      submitterMetadata: ReassignmentSubmitterMetadata,
      contractIds: Seq[LfContractId],
      targetSynchronizer: Target[PhysicalSynchronizerId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentProcessorError, FutureUnlessShutdown[
    UnassignmentProcessingSteps.SubmissionResult
  ]]

  def submitAssignments(
      submitterMetadata: ReassignmentSubmitterMetadata,
      reassignmentId: ReassignmentId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentProcessorError, FutureUnlessShutdown[
    AssignmentProcessingSteps.SubmissionResult
  ]]
}
