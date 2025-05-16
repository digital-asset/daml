// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.crypto.{SynchronizerCryptoClient, SynchronizerSnapshotSyncCryptoApi}
import com.digitalasset.canton.data.ViewType.AssignmentViewType
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, PromiseUnlessShutdownFactory}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.ReassignmentProcessorError
import com.digitalasset.canton.participant.protocol.submission.{
  InFlightSubmissionSynchronizerTracker,
  SeedGenerator,
}
import com.digitalasset.canton.participant.protocol.{ContractAuthenticator, ProtocolProcessor}
import com.digitalasset.canton.participant.sync.SyncEphemeralState
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.Target
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

class AssignmentProcessor(
    synchronizerId: Target[PhysicalSynchronizerId],
    override val participantId: ParticipantId,
    staticSynchronizerParameters: Target[StaticSynchronizerParameters],
    reassignmentCoordination: ReassignmentCoordination,
    inFlightSubmissionSynchronizerTracker: InFlightSubmissionSynchronizerTracker,
    ephemeral: SyncEphemeralState,
    synchronizerCrypto: SynchronizerCryptoClient,
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
        reassignmentCoordination,
        seedGenerator,
        ContractAuthenticator(synchronizerCrypto.pureCrypto),
        staticSynchronizerParameters,
        targetProtocolVersion,
        loggerFactory,
      ),
      inFlightSubmissionSynchronizerTracker,
      ephemeral,
      synchronizerCrypto,
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
      "user-id" -> submissionParam.submitterMetadata.userId,
      "type" -> "assignment",
    )

  override protected def preSubmissionValidations(
      params: AssignmentProcessingSteps.SubmissionParam,
      cryptoSnapshot: SynchronizerSnapshotSyncCryptoApi,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = EitherT.pure(())
}
