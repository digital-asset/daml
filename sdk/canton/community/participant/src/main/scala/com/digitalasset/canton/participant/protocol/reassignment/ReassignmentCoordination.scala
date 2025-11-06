// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.instances.future.catsStdInstancesForFuture
import cats.syntax.functor.*
import com.digitalasset.canton.config.ReassignmentsConfig
import com.digitalasset.canton.crypto.{
  SyncCryptoApiParticipantProvider,
  SynchronizerCryptoClient,
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
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.SingletonTraverse.syntax.*
import com.digitalasset.canton.util.{ReassignmentTag, SameReassignmentType, SingletonTraverse}

import scala.concurrent.{ExecutionContext, Future}

trait GetTopologyAtTimestamp {

  /** Will wait for the topology at the requested timestamp, unless it's too far in the future, in
    * which case it'll return None.
    */
  def maybeAwaitTopologySnapshot(
      targetPSId: Target[PhysicalSynchronizerId],
      requestedTimestamp: Target[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ReassignmentProcessorError,
    Option[Target[TopologySnapshot]],
  ]
  /*
   * TODO(i27585): After cleaning up the waiting routines, refactor to something like
   *
   *    def getTopologySnapshot(
   *      targetPSId: PhysicalSynchronizerId,
   *      requestedTimestamp: CantonTimestamp,
   *    ): Either[UnknownPhysicalSynchronizer, TopologySnapshotResult]
   *
   *    sealed trait TopologySnapshotResult
   *    case class TimestampTooFarInFuture(description: String) extends TopologySnapshotResult
   *    case class OK(await: FutureUnlessShutdown[TopologySnapshot]) extends TopologySnapshotResult
   *
   * This makes the various outcomes very clear, and isolates the Future into only the success case.
   */
}

class ReassignmentCoordination(
    reassignmentStoreFor: Target[SynchronizerId] => Either[
      ReassignmentProcessorError,
      ReassignmentStore,
    ],
    reassignmentSubmissionFor: PhysicalSynchronizerId => Option[ReassignmentSubmissionHandle],
    pendingUnassignments: Source[SynchronizerId] => Option[ReassignmentSynchronizer],
    staticSynchronizerParametersGetter: StaticSynchronizerParametersGetter,
    syncCryptoApi: SyncCryptoApiParticipantProvider,
    targetTimestampForwardTolerance: NonNegativeFiniteDuration,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with GetTopologyAtTimestamp {

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

  private[reassignment] def awaitSynchronizerTime[T[X] <: ReassignmentTag[X]: SameReassignmentType](
      psid: T[PhysicalSynchronizerId],
      timestamp: T[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, UnknownPhysicalSynchronizer, Unit] =
    reassignmentSubmissionFor(psid.unwrap) match {
      case Some(handle) =>
        handle.timeTracker.requestTick(timestamp.unwrap, immediately = true).discard
        EitherT.right(handle.timeTracker.awaitTick(timestamp.unwrap).getOrElse(Future.unit))
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
      timestamp: T[CantonTimestamp],
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
      handle.timeTracker.requestTick(timestamp.unwrap, immediately = true).discard
      cryptoApi.awaitTimestamp(timestamp.unwrap)
    }).toRight(UnknownPhysicalSynchronizer(synchronizerId.unwrap, "When waiting for timestamp"))

  /** Similar to [[awaitTimestamp]] but lifted into an [[EitherT]]
    *
    * @param onImmediate
    *   A callback that will be invoked if no wait was actually needed
    */
  private[reassignment] def awaitTimestamp[T[X] <: ReassignmentTag[X]: SameReassignmentType](
      synchronizerId: T[PhysicalSynchronizerId],
      staticSynchronizerParameters: T[StaticSynchronizerParameters],
      timestamp: T[CantonTimestamp],
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
      targetTopology: Target[TopologySnapshot],
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
          targetTopology,
        )
        .mapK(FutureUnlessShutdown.outcomeK)
        .semiflatMap(Predef.identity)
        .onShutdown(Left(ApplicationShutdown))
    } yield submissionResult
  }

  private[reassignment] def getStaticSynchronizerParameter[T[_]: SingletonTraverse](
      psid: T[PhysicalSynchronizerId]
  ): Either[UnknownPhysicalSynchronizer, T[StaticSynchronizerParameters]] =
    psid.traverseSingleton { (_, synchronizerId) =>
      staticSynchronizerParametersGetter
        .staticSynchronizerParameters(synchronizerId)
        .toRight(
          UnknownPhysicalSynchronizer(synchronizerId, "getting static synchronizer parameters")
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
      timestamp: T[CantonTimestamp],
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
        _.traverseSingleton((_, syncCrypto) => syncCrypto.snapshot(timestamp.unwrap))
      )

  private def awaitTimestampAndGetTaggedCryptoSnapshot[T[X] <: ReassignmentTag[X]
    : SameReassignmentType: SingletonTraverse](
      targetSynchronizerId: T[PhysicalSynchronizerId],
      staticSynchronizerParameters: T[StaticSynchronizerParameters],
      timestamp: T[CantonTimestamp],
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

  private def getRecentTopologyTimestamp[T[X] <: ReassignmentTag[X]
    : SameReassignmentType: SingletonTraverse](
      psid: T[PhysicalSynchronizerId]
  )(implicit
      traceContext: TraceContext
  ): Either[UnknownPhysicalSynchronizer, T[CantonTimestamp]] = for {
    staticSynchronizerParameters <- getStaticSynchronizerParameter(psid)
    topoClient <- getTopologyClient(psid, staticSynchronizerParameters)
  } yield topoClient.map(_.currentSnapshotApproximation.ipsSnapshot.timestamp)

  override def maybeAwaitTopologySnapshot(
      targetPSId: Target[PhysicalSynchronizerId],
      requestedTimestamp: Target[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ReassignmentProcessorError,
    Option[Target[TopologySnapshot]],
  ] = for {
    staticSynchronizerParameters <- EitherT.fromEither[FutureUnlessShutdown](
      getStaticSynchronizerParameter(targetPSId)
    )

    recentTimestamp <- EitherT.fromEither[FutureUnlessShutdown](
      getRecentTopologyTimestamp(targetPSId)
    )
    timestampUpperBound = recentTimestamp.map(_ + targetTimestampForwardTolerance)
    topology <-
      if (requestedTimestamp <= timestampUpperBound) {
        awaitTimestampAndGetTaggedCryptoSnapshot(
          targetPSId,
          staticSynchronizerParameters,
          requestedTimestamp,
        ).map(_.map(_.ipsSnapshot)).map(Some(_))
      } else {
        logger.info(
          s"Not loading target topology at timestamp $requestedTimestamp because it is more than $targetTimestampForwardTolerance ahead of our local target timestamp of $recentTimestamp."
        )
        EitherT.right[ReassignmentProcessorError](FutureUnlessShutdown.pure(None))
      }
  } yield topology

  private def getTopologyClient[T[X] <: ReassignmentTag[X]
    : SameReassignmentType: SingletonTraverse](
      psid: T[PhysicalSynchronizerId],
      staticSynchronizerParameters: T[StaticSynchronizerParameters],
  ): Either[UnknownPhysicalSynchronizer, T[SynchronizerCryptoClient]] =
    psid
      .traverseSingleton { case (_, synchronizerId) =>
        syncCryptoApi.forSynchronizer(synchronizerId, staticSynchronizerParameters.unwrap)
      }
      .toRight(UnknownPhysicalSynchronizer(psid.unwrap, "when getting topology client"))

  private[reassignment] def getRecentTopologySnapshot(
      targetSynchronizerId: Target[PhysicalSynchronizerId],
      staticSynchronizerParameters: Target[StaticSynchronizerParameters],
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ReassignmentProcessorError,
    Target[TopologySnapshot],
  ] =
    for {
      timestamp <- EitherT.fromEither[FutureUnlessShutdown](
        getRecentTopologyTimestamp(targetSynchronizerId)
      )
      // Since events are stored before they are processed, we wait just to be sure.
      targetCrypto <- awaitTimestampAndGetTaggedCryptoSnapshot(
        targetSynchronizerId,
        staticSynchronizerParameters,
        timestamp,
      )
    } yield targetCrypto.map(_.ipsSnapshot)

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
      reassignmentsConfig: ReassignmentsConfig,
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

    new ReassignmentCoordination(
      reassignmentStoreFor = reassignmentStoreFor,
      reassignmentSubmissionFor = submissionHandles,
      pendingUnassignments = pendingUnassignments,
      staticSynchronizerParametersGetter = syncPersistentStateManager,
      syncCryptoApi = syncCryptoApi,
      targetTimestampForwardTolerance =
        reassignmentsConfig.targetTimestampForwardTolerance.toInternal,
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
      sourceTopology: Source[TopologySnapshot],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentProcessorError, FutureUnlessShutdown[
    UnassignmentProcessingSteps.SubmissionResult
  ]]

  def submitAssignments(
      submitterMetadata: ReassignmentSubmitterMetadata,
      reassignmentId: ReassignmentId,
      targetTopology: Target[TopologySnapshot],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentProcessorError, FutureUnlessShutdown[
    AssignmentProcessingSteps.SubmissionResult
  ]]
}
