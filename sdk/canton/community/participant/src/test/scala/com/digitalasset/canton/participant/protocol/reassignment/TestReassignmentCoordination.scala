// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.functor.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{
  SyncCryptoApiParticipantProvider,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.protocol.ReassignmentSynchronizer
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.{
  ReassignmentProcessorError,
  UnknownSynchronizer,
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

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

private[reassignment] object TestReassignmentCoordination {
  val pendingUnassignments: TrieMap[Source[SynchronizerId], ReassignmentSynchronizer] =
    TrieMap.empty[Source[SynchronizerId], ReassignmentSynchronizer]

  def apply(
      synchronizers: Set[Target[SynchronizerId]],
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
      synchronizers
        .map(synchronizer =>
          synchronizer -> new InMemoryReassignmentStore(synchronizer, loggerFactory)
        )
        .toMap
    val assignmentBySubmission = { (_: SynchronizerId) => None }
    val protocolVersionGetter = (_: Traced[SynchronizerId]) =>
      Some(BaseTest.testedStaticSynchronizerParameters)

    val reassignmentSynchronizer = { (id: Source[SynchronizerId]) =>
      pendingUnassignments.getOrElse(
        id, {
          val reassignmentSynchronizer =
            new ReassignmentSynchronizer(id, loggerFactory, new ProcessingTimeout)
          pendingUnassignments.put(id, reassignmentSynchronizer)
          reassignmentSynchronizer
        },
      )
    }

    new ReassignmentCoordination(
      reassignmentStoreFor = id =>
        reassignmentStores.get(id).toRight(UnknownSynchronizer(id.unwrap, "not found")),
      recentTimeProofFor = recentTimeProofProvider,
      reassignmentSubmissionFor = assignmentBySubmission,
      pendingUnassignments = reassignmentSynchronizer.map(Option(_)),
      staticSynchronizerParameterFor = protocolVersionGetter,
      syncCryptoApi =
        defaultSyncCryptoApi(synchronizers.toSeq.map(_.unwrap), packages, loggerFactory),
      loggerFactory,
    ) {

      override def awaitUnassignmentTimestamp(
          sourceSynchronizer: Source[SynchronizerId],
          staticSynchronizerParameters: Source[StaticSynchronizerParameters],
          timestamp: CantonTimestamp,
      )(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, UnknownSynchronizer, Unit] =
        awaitTimestampOverride match {
          case None =>
            super.awaitUnassignmentTimestamp(
              sourceSynchronizer,
              staticSynchronizerParameters,
              timestamp,
            )
          case Some(overridden) =>
            EitherT.right(overridden.fold(FutureUnlessShutdown.unit)(FutureUnlessShutdown.outcomeF))
        }

      override def awaitTimestamp[T[X] <: ReassignmentTag[X]: SameReassignmentType](
          synchronizerId: T[SynchronizerId],
          staticSynchronizerParameters: T[StaticSynchronizerParameters],
          timestamp: CantonTimestamp,
      )(implicit
          traceContext: TraceContext
      ): Either[ReassignmentProcessorError, Option[FutureUnlessShutdown[Unit]]] =
        awaitTimestampOverride match {
          case None =>
            super.awaitTimestamp(synchronizerId, staticSynchronizerParameters, timestamp)
          case Some(overridden) =>
            Right(overridden.map(FutureUnlessShutdown.outcomeF))
        }

      override def cryptoSnapshot[
          T[X] <: ReassignmentTag[X]: SameReassignmentType: SingletonTraverse
      ](
          synchronizerId: T[SynchronizerId],
          staticSynchronizerParameters: T[StaticSynchronizerParameters],
          timestamp: CantonTimestamp,
      )(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, T[
        SynchronizerSnapshotSyncCryptoApi
      ]] =
        snapshotOverride match {
          case None => super.cryptoSnapshot(synchronizerId, staticSynchronizerParameters, timestamp)
          case Some(cs) =>
            EitherT.pure[FutureUnlessShutdown, ReassignmentProcessorError](
              synchronizerId.map(_ => cs)
            )
        }
    }
  }

  private def defaultSyncCryptoApi(
      synchronizers: Seq[SynchronizerId],
      packages: Seq[LfPackageId],
      loggerFactory: NamedLoggerFactory,
  ): SyncCryptoApiParticipantProvider =
    TestingTopology(synchronizers = synchronizers.toSet)
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
