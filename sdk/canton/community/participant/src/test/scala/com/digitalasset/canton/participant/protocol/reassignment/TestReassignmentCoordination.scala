// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.functor.*
import com.digitalasset.canton.BaseTest.testedProtocolVersion
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
import com.digitalasset.canton.participant.sync.StaticSynchronizerParametersGetter
import com.digitalasset.canton.protocol.ExampleTransactionFactory.*
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.topology.{
  ParticipantId,
  PhysicalSynchronizerId,
  SynchronizerId,
  TestingTopology,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.{ReassignmentTag, SameReassignmentType, SingletonTraverse}
import com.digitalasset.canton.{BaseTest, LfPackageId}

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.DurationConverters.*

private[reassignment] object TestReassignmentCoordination {

  private val pendingUnassignments: TrieMap[Source[SynchronizerId], ReassignmentSynchronizer] =
    TrieMap.empty[Source[SynchronizerId], ReassignmentSynchronizer]

  def apply(
      synchronizers: Set[Target[PhysicalSynchronizerId]],
      timeProofTimestamp: CantonTimestamp,
      snapshotOverride: Option[SynchronizerSnapshotSyncCryptoApi] = None,
      awaitTimestampOverride: Option[Option[Future[Unit]]] = None,
      loggerFactory: NamedLoggerFactory,
      packages: Seq[LfPackageId] = Seq.empty,
      targetTimestampForwardTolerance: FiniteDuration = 30.seconds,
  )(implicit ec: ExecutionContext): ReassignmentCoordination = {

    val reassignmentStores =
      synchronizers
        .map(synchronizer =>
          synchronizer.map(_.logical) -> new InMemoryReassignmentStore(
            synchronizer.map(_.logical),
            loggerFactory,
          )
        )
        .toMap
    val assignmentBySubmission = { (_: PhysicalSynchronizerId) => None }

    val staticSynchronizerParametersGetter = new StaticSynchronizerParametersGetter {
      override def staticSynchronizerParameters(
          synchronizerId: PhysicalSynchronizerId
      ): Option[StaticSynchronizerParameters] = Some(
        BaseTest.defaultStaticSynchronizerParametersWith(
          topologyChangeDelay = NonNegativeFiniteDuration.Zero,
          testedProtocolVersion,
        )
      )

      override def latestKnownPSId(synchronizerId: SynchronizerId): Option[PhysicalSynchronizerId] =
        ???
    }

    val reassignmentSynchronizer = { (id: Source[SynchronizerId]) =>
      pendingUnassignments.getOrElse(
        id, {
          val reassignmentSynchronizer =
            new ReassignmentSynchronizer(loggerFactory, new ProcessingTimeout)
          pendingUnassignments.put(id, reassignmentSynchronizer)
          reassignmentSynchronizer
        },
      )
    }

    new ReassignmentCoordination(
      reassignmentStoreFor = id =>
        reassignmentStores.get(id).toRight(UnknownSynchronizer(id.unwrap, "not found")),
      reassignmentSubmissionFor = assignmentBySubmission,
      pendingUnassignments = reassignmentSynchronizer.map(Option(_)),
      staticSynchronizerParametersGetter = staticSynchronizerParametersGetter,
      syncCryptoApi = defaultSyncCryptoApi(
        synchronizers.toSeq.map(_.unwrap),
        packages,
        loggerFactory,
        timeProofTimestamp,
      ),
      targetTimestampForwardTolerance =
        NonNegativeFiniteDuration.tryCreate(targetTimestampForwardTolerance.toJava),
      loggerFactory,
    ) {

      override def awaitTimestamp[T[X] <: ReassignmentTag[X]: SameReassignmentType](
          synchronizerId: T[PhysicalSynchronizerId],
          staticSynchronizerParameters: T[StaticSynchronizerParameters],
          timestamp: T[CantonTimestamp],
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
          synchronizerId: T[PhysicalSynchronizerId],
          staticSynchronizerParameters: T[StaticSynchronizerParameters],
          timestamp: T[CantonTimestamp],
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
      synchronizers: Seq[PhysicalSynchronizerId],
      packages: Seq[LfPackageId],
      loggerFactory: NamedLoggerFactory,
      approximateTimestamp: CantonTimestamp,
  ): SyncCryptoApiParticipantProvider =
    TestingTopology(synchronizers = synchronizers.toSet)
      .withReversedTopology(defaultTopology)
      .withPackages(defaultTopology.keys.map(_ -> packages).toMap)
      .build(loggerFactory)
      .forOwner(
        owner = submittingParticipant,
        currentSnapshotApproximationTimestamp = approximateTimestamp,
      )

  private val observerParticipant1: ParticipantId = ParticipantId("observerParticipant1")
  private val observerParticipant2: ParticipantId = ParticipantId("observerParticipant2")

  private val defaultTopology = Map(
    submittingParticipant -> Map(submitter -> Submission),
    signatoryParticipant -> Map(signatory -> Submission),
    observerParticipant1 -> Map(observer -> Confirmation),
    observerParticipant2 -> Map(observer -> Observation),
  )

}
