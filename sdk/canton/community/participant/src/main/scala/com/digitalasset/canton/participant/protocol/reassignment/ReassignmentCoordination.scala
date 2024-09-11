// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.instances.future.catsStdInstancesForFuture
import cats.syntax.functor.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, SyncCryptoApiProvider}
import com.digitalasset.canton.data.{CantonTimestamp, ReassignmentSubmitterMetadata}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.{
  ApplicationShutdown,
  ReassignmentProcessorError,
  ReassignmentStoreFailed,
  UnknownDomain,
}
import com.digitalasset.canton.participant.store.ReassignmentStore
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManager
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.DeliveredUnassignmentResult
import com.digitalasset.canton.sequencing.protocol.TimeProof
import com.digitalasset.canton.time.DomainTimeTracker
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.version.Reassignment.TargetProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

class ReassignmentCoordination(
    reassignmentStoreFor: TargetDomainId => Either[ReassignmentProcessorError, ReassignmentStore],
    recentTimeProofFor: RecentTimeProofProvider,
    inSubmissionById: DomainId => Option[ReassignmentSubmissionHandle],
    val protocolVersionFor: Traced[DomainId] => Option[ProtocolVersion],
    syncCryptoApi: SyncCryptoApiProvider,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  private[reassignment] def awaitDomainTime(domain: DomainId, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, UnknownDomain, Unit] =
    inSubmissionById(domain) match {
      case Some(handle) =>
        handle.timeTracker.requestTick(timestamp, immediately = true)
        EitherT.right(handle.timeTracker.awaitTick(timestamp).map(_.void).getOrElse(Future.unit))
      case None =>
        EitherT.leftT(
          UnknownDomain(domain, s"Unable to find domain when awaiting domain time $timestamp.")
        )
    }

  /** Returns a future that completes when a snapshot can be taken on the given domain for the given timestamp.
    *
    * This is used when an assignment blocks for the identity state at the unassignment. For more general uses,
    * `awaitTimestamp` should be preferred as it triggers the progression of time on `domain` by requesting a tick.
    */
  private[reassignment] def awaitUnassignmentTimestamp(
      domain: SourceDomainId,
      staticDomainParameters: StaticDomainParameters,
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): Either[UnknownDomain, Future[Unit]] =
    syncCryptoApi
      .forDomain(domain.unwrap, staticDomainParameters)
      .toRight(UnknownDomain(domain.unwrap, "When assignment waits for unassignment timestamp"))
      .map(_.awaitTimestamp(timestamp).getOrElse(Future.unit))

  /** Returns a future that completes when it is safe to take an identity snapshot for the given `timestamp` on the given `domain`.
    * [[scala.None$]] indicates that this point has already been reached before the call.
    * [[scala.Left$]] if the `domain` is unknown or the participant is not connected to the domain.
    */
  private[reassignment] def awaitTimestamp(
      domain: DomainId,
      staticDomainParameters: StaticDomainParameters,
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): Either[ReassignmentProcessorError, Option[Future[Unit]]] =
    OptionUtil
      .zipWith(syncCryptoApi.forDomain(domain, staticDomainParameters), inSubmissionById(domain)) {
        (cryptoApi, handle) =>
          // we request a tick immediately only if the clock is a SimClock
          handle.timeTracker.requestTick(timestamp, immediately = true)
          cryptoApi.awaitTimestamp(timestamp)
      }
      .toRight(UnknownDomain(domain, "When waiting for timestamp"))

  /** Similar to [[awaitTimestamp]] but lifted into an [[EitherT]]
    *
    * @param onImmediate A callback that will be invoked if no wait was actually needed
    */
  private[reassignment] def awaitTimestamp(
      domain: DomainId,
      staticDomainParameters: StaticDomainParameters,
      timestamp: CantonTimestamp,
      onImmediate: => Future[Unit],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentProcessorError, Unit] =
    for {
      timeout <- EitherT.fromEither[Future](
        awaitTimestamp(domain, staticDomainParameters, timestamp)
      )
      _ <- EitherT.right[ReassignmentProcessorError](timeout.getOrElse(onImmediate))
    } yield ()

  private[reassignment] def awaitTimestampUS(
      domain: DomainId,
      staticDomainParameters: StaticDomainParameters,
      timestamp: CantonTimestamp,
      onImmediate: => Future[Unit],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
    for {
      wait <- EitherT.fromEither[FutureUnlessShutdown](
        awaitTimestamp(domain, staticDomainParameters, timestamp)
      )
      _ <- EitherT.right(FutureUnlessShutdown.outcomeF(wait.getOrElse(onImmediate)))
    } yield ()

  /** Submits an assignment. Used by the [[UnassignmentProcessingSteps]] to automatically trigger the submission of
    * an assignment after the exclusivity timeout.
    */
  private[reassignment] def assign(
      targetDomain: TargetDomainId,
      submitterMetadata: ReassignmentSubmitterMetadata,
      reassignmentId: ReassignmentId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentProcessorError, AssignmentProcessingSteps.SubmissionResult] = {
    logger.debug(s"Triggering automatic assignment of reassignment `$reassignmentId`")

    for {
      inSubmission <- EitherT.fromEither[Future](
        inSubmissionById(targetDomain.unwrap).toRight(
          UnknownDomain(targetDomain.unwrap, "When submitting assignment")
        )
      )
      submissionResult <- inSubmission
        .submitAssignment(
          submitterMetadata,
          reassignmentId,
        )
        .mapK(FutureUnlessShutdown.outcomeK)
        .semiflatMap(Predef.identity)
        .onShutdown(Left(ApplicationShutdown))
    } yield submissionResult
  }

  /** Returns a [[crypto.DomainSnapshotSyncCryptoApi]] for the given `domain` at the given timestamp.
    * The returned future fails with [[java.lang.IllegalArgumentException]] if the `domain` has not progressed far enough
    * such that it can compute the snapshot. Use [[awaitTimestamp]] to ensure progression to `timestamp`.
    */
  private[reassignment] def cryptoSnapshot(
      domain: DomainId,
      staticDomainParameters: StaticDomainParameters,
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentProcessorError, DomainSnapshotSyncCryptoApi] =
    EitherT
      .fromEither[Future](
        syncCryptoApi
          .forDomain(domain, staticDomainParameters)
          .toRight(
            UnknownDomain(domain, "When getting crypto snapshot"): ReassignmentProcessorError
          )
      )
      .semiflatMap(_.snapshot(timestamp))

  private[reassignment] def awaitTimestampAndGetCryptoSnapshot(
      domain: DomainId,
      staticDomainParameters: StaticDomainParameters,
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, DomainSnapshotSyncCryptoApi] =
    for {
      _ <- awaitTimestampUS(
        domain,
        staticDomainParameters,
        timestamp,
        Future.unit,
      )
      snapshot <- cryptoSnapshot(domain, staticDomainParameters, timestamp).mapK(
        FutureUnlessShutdown.outcomeK
      )
    } yield snapshot

  private[reassignment] def getTimeProofAndSnapshot(
      targetDomain: TargetDomainId,
      staticDomainParameters: StaticDomainParameters,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ReassignmentProcessorError,
    (TimeProof, DomainSnapshotSyncCryptoApi),
  ] =
    for {
      timeProof <- recentTimeProofFor.get(targetDomain, staticDomainParameters)
      // Since events are stored before they are processed, we wait just to be sure.
      targetCrypto <- awaitTimestampAndGetCryptoSnapshot(
        targetDomain.unwrap,
        staticDomainParameters,
        timeProof.timestamp,
      )
    } yield (timeProof, targetCrypto)

  /** Stores the given reassignment data on the target domain. */
  private[reassignment] def addUnassignmentRequest(
      reassignmentData: ReassignmentData
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
    for {
      reassignmentStore <- EitherT.fromEither[FutureUnlessShutdown](
        reassignmentStoreFor(reassignmentData.targetDomain)
      )
      _ <- reassignmentStore
        .addReassignment(reassignmentData)
        .leftMap[ReassignmentProcessorError](
          ReassignmentStoreFailed(reassignmentData.reassignmentId, _)
        )
    } yield ()

  /** Adds the unassignment result to the reassignment stored on the given domain. */
  private[reassignment] def addUnassignmentResult(
      domain: TargetDomainId,
      unassignmentResult: DeliveredUnassignmentResult,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
    for {
      reassignmentStore <- EitherT.fromEither[FutureUnlessShutdown](reassignmentStoreFor(domain))
      _ <- reassignmentStore
        .addUnassignmentResult(unassignmentResult)
        .leftMap[ReassignmentProcessorError](
          ReassignmentStoreFailed(unassignmentResult.reassignmentId, _)
        )
    } yield ()

  /** Removes the given [[com.digitalasset.canton.protocol.ReassignmentId]] from the given [[com.digitalasset.canton.topology.DomainId]]'s [[store.ReassignmentStore]]. */
  private[reassignment] def deleteReassignment(
      targetDomain: TargetDomainId,
      reassignmentId: ReassignmentId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentProcessorError, Unit] =
    for {
      reassignmentStore <- EitherT.fromEither[Future](reassignmentStoreFor(targetDomain))
      _ <- EitherT.right[ReassignmentProcessorError](
        reassignmentStore.deleteReassignment(reassignmentId)
      )
    } yield ()
}

object ReassignmentCoordination {
  def apply(
      reassignmentTimeProofFreshnessProportion: NonNegativeInt,
      syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
      submissionHandles: DomainId => Option[ReassignmentSubmissionHandle],
      syncCryptoApi: SyncCryptoApiProvider,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): ReassignmentCoordination = {
    def domainDataFor(domain: TargetDomainId): Either[UnknownDomain, ReassignmentStore] =
      syncDomainPersistentStateManager
        .get(domain.unwrap)
        .map(_.reassignmentStore)
        .toRight(UnknownDomain(domain.unwrap, "looking for persistent state"))

    val domainProtocolVersionGetter: Traced[DomainId] => Option[ProtocolVersion] =
      (tracedDomainId: Traced[DomainId]) =>
        syncDomainPersistentStateManager.protocolVersionFor(tracedDomainId.value)

    val recentTimeProofProvider = new RecentTimeProofProvider(
      submissionHandles,
      syncCryptoApi,
      loggerFactory,
      reassignmentTimeProofFreshnessProportion,
    )

    new ReassignmentCoordination(
      reassignmentStoreFor = domainDataFor,
      recentTimeProofFor = recentTimeProofProvider,
      inSubmissionById = submissionHandles,
      protocolVersionFor = domainProtocolVersionGetter,
      syncCryptoApi = syncCryptoApi,
      loggerFactory = loggerFactory,
    )
  }
}

trait ReassignmentSubmissionHandle {
  def timeTracker: DomainTimeTracker

  def submitUnassignment(
      submitterMetadata: ReassignmentSubmitterMetadata,
      contractId: LfContractId,
      targetDomain: TargetDomainId,
      targetProtocolVersion: TargetProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentProcessorError, FutureUnlessShutdown[
    UnassignmentProcessingSteps.SubmissionResult
  ]]

  def submitAssignment(
      submitterMetadata: ReassignmentSubmitterMetadata,
      reassignmentId: ReassignmentId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentProcessorError, FutureUnlessShutdown[
    AssignmentProcessingSteps.SubmissionResult
  ]]
}
