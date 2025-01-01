// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, DomainSyncCryptoClient}
import com.digitalasset.canton.data.ViewType.AssignmentViewType
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, PromiseUnlessShutdownFactory}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.ReassignmentProcessorError
import com.digitalasset.canton.participant.protocol.submission.{
  InFlightSubmissionDomainTracker,
  SeedGenerator,
}
import com.digitalasset.canton.participant.protocol.{
  ProtocolProcessor,
  SerializableContractAuthenticator,
}
import com.digitalasset.canton.participant.store.SyncDomainEphemeralState
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.Target
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

class AssignmentProcessor(
    synchronizerId: Target[SynchronizerId],
    override val participantId: ParticipantId,
    damle: DAMLe,
    staticDomainParameters: Target[StaticDomainParameters],
    reassignmentCoordination: ReassignmentCoordination,
    inFlightSubmissionDomainTracker: InFlightSubmissionDomainTracker,
    ephemeral: SyncDomainEphemeralState,
    domainCrypto: DomainSyncCryptoClient,
    seedGenerator: SeedGenerator,
    sequencerClient: SequencerClient,
    override protected val timeouts: ProcessingTimeout,
    targetProtocolVersion: Target[ProtocolVersion],
    loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    override val testingConfig: TestingConfigInternal,
    promiseFactory: PromiseUnlessShutdownFactory,
)(implicit ec: ExecutionContext)
    extends ProtocolProcessor[
      AssignmentProcessingSteps.SubmissionParam,
      AssignmentProcessingSteps.SubmissionResult,
      AssignmentViewType,
      ReassignmentProcessorError,
    ](
      new AssignmentProcessingSteps(
        synchronizerId,
        participantId,
        damle,
        reassignmentCoordination,
        seedGenerator,
        SerializableContractAuthenticator(domainCrypto.pureCrypto),
        staticDomainParameters,
        targetProtocolVersion,
        loggerFactory,
      ),
      inFlightSubmissionDomainTracker,
      ephemeral,
      domainCrypto,
      sequencerClient,
      synchronizerId.unwrap,
      targetProtocolVersion.unwrap,
      loggerFactory,
      futureSupervisor,
      promiseFactory,
    ) {
  override protected def metricsContextForSubmissionParam(
      submissionParam: AssignmentProcessingSteps.SubmissionParam
  ): MetricsContext =
    MetricsContext(
      "application-id" -> submissionParam.submitterMetadata.applicationId,
      "type" -> "assignment",
    )

  override protected def preSubmissionValidations(
      params: AssignmentProcessingSteps.SubmissionParam,
      cryptoSnapshot: DomainSnapshotSyncCryptoApi,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = EitherT.pure(())
}
