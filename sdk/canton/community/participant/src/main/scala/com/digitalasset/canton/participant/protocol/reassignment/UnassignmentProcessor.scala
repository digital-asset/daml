// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.crypto.{
  SynchronizerSnapshotSyncCryptoApi,
  SynchronizerSyncCryptoClient,
}
import com.digitalasset.canton.data.ViewType.UnassignmentViewType
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
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.Source
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

class UnassignmentProcessor(
    synchronizerId: Source[SynchronizerId],
    override val participantId: ParticipantId,
    damle: DAMLe,
    staticSynchronizerParameters: Source[StaticSynchronizerParameters],
    reassignmentCoordination: ReassignmentCoordination,
    inFlightSubmissionDomainTracker: InFlightSubmissionDomainTracker,
    ephemeral: SyncDomainEphemeralState,
    synchronizerCrypto: SynchronizerSyncCryptoClient,
    seedGenerator: SeedGenerator,
    sequencerClient: SequencerClient,
    override protected val timeouts: ProcessingTimeout,
    sourceProtocolVersion: Source[ProtocolVersion],
    loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    override val testingConfig: TestingConfigInternal,
    promiseFactory: PromiseUnlessShutdownFactory,
)(implicit ec: ExecutionContext)
    extends ProtocolProcessor[
      UnassignmentProcessingSteps.SubmissionParam,
      UnassignmentProcessingSteps.SubmissionResult,
      UnassignmentViewType,
      ReassignmentProcessorError,
    ](
      new UnassignmentProcessingSteps(
        synchronizerId,
        participantId,
        damle,
        reassignmentCoordination,
        seedGenerator,
        staticSynchronizerParameters,
        SerializableContractAuthenticator(synchronizerCrypto.pureCrypto),
        sourceProtocolVersion,
        loggerFactory,
      ),
      inFlightSubmissionDomainTracker,
      ephemeral,
      synchronizerCrypto,
      sequencerClient,
      synchronizerId.unwrap,
      sourceProtocolVersion.unwrap,
      loggerFactory,
      futureSupervisor,
      promiseFactory,
    ) {
  override protected def metricsContextForSubmissionParam(
      submissionParam: UnassignmentProcessingSteps.SubmissionParam
  ): MetricsContext =
    MetricsContext(
      "application-id" -> submissionParam.submitterMetadata.applicationId,
      "type" -> "unassignment",
    )

  override protected def preSubmissionValidations(
      params: UnassignmentProcessingSteps.SubmissionParam,
      cryptoSnapshot: SynchronizerSnapshotSyncCryptoApi,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = EitherT.pure(())
}
