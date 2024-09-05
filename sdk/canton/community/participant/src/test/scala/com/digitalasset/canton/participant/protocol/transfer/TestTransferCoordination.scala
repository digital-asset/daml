// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.EitherT
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, SyncCryptoApiProvider}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.protocol.transfer.ReassignmentProcessingSteps.{
  ReassignmentProcessorError,
  UnknownDomain,
}
import com.digitalasset.canton.participant.store.memory.InMemoryReassignmentStore
import com.digitalasset.canton.protocol.ExampleTransactionFactory.*
import com.digitalasset.canton.protocol.{SourceDomainId, StaticDomainParameters, TargetDomainId}
import com.digitalasset.canton.time.TimeProofTestUtil
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.topology.{DomainId, ParticipantId, TestingTopology}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.{BaseTest, LfPackageId}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.MockitoSugar.mock

import scala.concurrent.{ExecutionContext, Future}

private[transfer] object TestTransferCoordination {
  def apply(
      domains: Set[TargetDomainId],
      timeProofTimestamp: CantonTimestamp,
      snapshotOverride: Option[DomainSnapshotSyncCryptoApi] = None,
      awaitTimestampOverride: Option[Option[Future[Unit]]] = None,
      loggerFactory: NamedLoggerFactory,
      packages: Seq[LfPackageId] = Seq.empty,
  )(implicit ec: ExecutionContext): ReassignmentCoordination = {

    val recentTimeProofProvider = mock[RecentTimeProofProvider]
    when(
      recentTimeProofProvider.get(any[TargetDomainId], any[StaticDomainParameters])(
        any[TraceContext]
      )
    )
      .thenReturn(EitherT.pure(TimeProofTestUtil.mkTimeProof(timeProofTimestamp)))

    val reassignmentStores =
      domains.map(domain => domain -> new InMemoryReassignmentStore(domain, loggerFactory)).toMap
    val assignmentBySubmission = { (_: DomainId) => None }
    val protocolVersionGetter = (_: Traced[DomainId]) => Some(BaseTest.testedProtocolVersion)

    new ReassignmentCoordination(
      reassignmentStoreFor = id =>
        reassignmentStores.get(id).toRight(UnknownDomain(id.unwrap, "not found")),
      recentTimeProofFor = recentTimeProofProvider,
      inSubmissionById = assignmentBySubmission,
      protocolVersionFor = protocolVersionGetter,
      syncCryptoApi = defaultSyncCryptoApi(domains.toSeq.map(_.unwrap), packages, loggerFactory),
      loggerFactory,
    ) {

      override def awaitUnassignmentTimestamp(
          sourceDomain: SourceDomainId,
          staticDomainParameters: StaticDomainParameters,
          timestamp: CantonTimestamp,
      )(implicit
          traceContext: TraceContext
      ): Either[ReassignmentProcessingSteps.UnknownDomain, Future[Unit]] =
        awaitTimestampOverride match {
          case None =>
            super.awaitUnassignmentTimestamp(sourceDomain, staticDomainParameters, timestamp)
          case Some(overridden) => Right(overridden.getOrElse(Future.unit))
        }

      override def awaitTimestamp(
          domainId: DomainId,
          staticDomainParameters: StaticDomainParameters,
          timestamp: CantonTimestamp,
      )(implicit
          traceContext: TraceContext
      ): Either[ReassignmentProcessorError, Option[Future[Unit]]] =
        awaitTimestampOverride match {
          case None =>
            super.awaitTimestamp(domainId, staticDomainParameters, timestamp)
          case Some(overridden) => Right(overridden)
        }

      override def cryptoSnapshot(
          domain: DomainId,
          staticDomainParameters: StaticDomainParameters,
          timestamp: CantonTimestamp,
      )(implicit
          traceContext: TraceContext
      ): EitherT[Future, ReassignmentProcessorError, DomainSnapshotSyncCryptoApi] =
        snapshotOverride match {
          case None => super.cryptoSnapshot(domain, staticDomainParameters, timestamp)
          case Some(cs) => EitherT.pure[Future, ReassignmentProcessorError](cs)
        }
    }
  }

  private def defaultSyncCryptoApi(
      domains: Seq[DomainId],
      packages: Seq[LfPackageId],
      loggerFactory: NamedLoggerFactory,
  ): SyncCryptoApiProvider =
    TestingTopology(domains = domains.toSet)
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
