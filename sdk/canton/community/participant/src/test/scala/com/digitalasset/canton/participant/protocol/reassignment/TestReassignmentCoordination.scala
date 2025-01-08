// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.functor.*
import com.digitalasset.canton.crypto.{SyncCryptoApiProvider, SynchronizerSnapshotSyncCryptoApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.{
  ReassignmentProcessorError,
  UnknownDomain,
}
import com.digitalasset.canton.participant.store.memory.InMemoryReassignmentStore
import com.digitalasset.canton.protocol.ExampleTransactionFactory.*
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.time.TimeProofTestUtil
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId, TestingTopology}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.{ReassignmentTag, SameReassignmentType, SingletonTraverse}
import com.digitalasset.canton.{BaseTest, LfPackageId}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.MockitoSugar.mock

import scala.concurrent.{ExecutionContext, Future}

private[reassignment] object TestReassignmentCoordination {
  def apply(
      domains: Set[Target[SynchronizerId]],
      timeProofTimestamp: CantonTimestamp,
      snapshotOverride: Option[SynchronizerSnapshotSyncCryptoApi] = None,
      awaitTimestampOverride: Option[Option[Future[Unit]]] = None,
      loggerFactory: NamedLoggerFactory,
      packages: Seq[LfPackageId] = Seq.empty,
  )(implicit ec: ExecutionContext): ReassignmentCoordination = {

    val recentTimeProofProvider = mock[RecentTimeProofProvider]
    when(
      recentTimeProofProvider.get(
        any[Target[SynchronizerId]],
        any[Target[StaticSynchronizerParameters]],
      )(
        any[TraceContext]
      )
    )
      .thenReturn(EitherT.pure(TimeProofTestUtil.mkTimeProof(timeProofTimestamp)))

    val reassignmentStores =
      domains.map(domain => domain -> new InMemoryReassignmentStore(domain, loggerFactory)).toMap
    val assignmentBySubmission = { (_: SynchronizerId) => None }
    val protocolVersionGetter = (_: Traced[SynchronizerId]) =>
      Some(BaseTest.testedStaticSynchronizerParameters)

    new ReassignmentCoordination(
      reassignmentStoreFor = id =>
        reassignmentStores.get(id).toRight(UnknownDomain(id.unwrap, "not found")),
      recentTimeProofFor = recentTimeProofProvider,
      reassignmentSubmissionFor = assignmentBySubmission,
      staticSynchronizerParameterFor = protocolVersionGetter,
      syncCryptoApi = defaultSyncCryptoApi(domains.toSeq.map(_.unwrap), packages, loggerFactory),
      loggerFactory,
    ) {

      override def awaitUnassignmentTimestamp(
          sourceDomain: Source[SynchronizerId],
          staticSynchronizerParameters: Source[StaticSynchronizerParameters],
          timestamp: CantonTimestamp,
      )(implicit
          traceContext: TraceContext
      ): EitherT[Future, UnknownDomain, Unit] =
        awaitTimestampOverride match {
          case None =>
            super.awaitUnassignmentTimestamp(sourceDomain, staticSynchronizerParameters, timestamp)
          case Some(overridden) => EitherT.right(overridden.getOrElse(Future.unit))
        }

      override def awaitTimestamp[T[X] <: ReassignmentTag[X]: SameReassignmentType](
          synchronizerId: T[SynchronizerId],
          staticSynchronizerParameters: T[StaticSynchronizerParameters],
          timestamp: CantonTimestamp,
      )(implicit
          traceContext: TraceContext
      ): Either[ReassignmentProcessorError, Option[Future[Unit]]] =
        awaitTimestampOverride match {
          case None =>
            super.awaitTimestamp(synchronizerId, staticSynchronizerParameters, timestamp)
          case Some(overridden) => Right(overridden)
        }

      override def cryptoSnapshot[T[X] <: ReassignmentTag[
        X
      ]: SameReassignmentType: SingletonTraverse](
          synchronizerId: T[SynchronizerId],
          staticSynchronizerParameters: T[StaticSynchronizerParameters],
          timestamp: CantonTimestamp,
      )(implicit
          traceContext: TraceContext
      ): EitherT[Future, ReassignmentProcessorError, T[SynchronizerSnapshotSyncCryptoApi]] =
        snapshotOverride match {
          case None => super.cryptoSnapshot(synchronizerId, staticSynchronizerParameters, timestamp)
          case Some(cs) =>
            EitherT.pure[Future, ReassignmentProcessorError](synchronizerId.map(_ => cs))
        }
    }
  }

  private def defaultSyncCryptoApi(
      domains: Seq[SynchronizerId],
      packages: Seq[LfPackageId],
      loggerFactory: NamedLoggerFactory,
  ): SyncCryptoApiProvider =
    TestingTopology(synchronizers = domains.toSet)
      .withReversedTopology(defaultTopology)
      .withPackages(defaultTopology.keys.map(_ -> packages).toMap)
      .build(loggerFactory)
      .forOwner(submittingParticipant)

  private val observerParticipant1: ParticipantId = ParticipantId("observerParticipant1")
  private val observerParticipant2: ParticipantId = ParticipantId("observerParticipant2")

  private val defaultTopology = Map(
    submittingParticipant -> Map(submitter -> Submission),
    signatoryParticipant -> Map(signatory -> Submission),
    observerParticipant1 -> Map(observer -> Confirmation),
    observerParticipant2 -> Map(observer -> Observation),
  )

}
