// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.data.ViewType.AssignmentViewType
import com.digitalasset.canton.lifecycle.PromiseUnlessShutdownFactory
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.protocol.ProtocolProcessor
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.ReassignmentProcessorError
import com.digitalasset.canton.participant.protocol.submission.{
  InFlightSubmissionSynchronizerTracker,
  SeedGenerator,
}
import com.digitalasset.canton.participant.sync.SyncEphemeralState
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.util.ContractValidator
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
    contractValidator: ContractValidator,
    seedGenerator: SeedGenerator,
    sequencerClient: SequencerClient,
    clock: Clock,
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
        synchronizerCrypto,
        seedGenerator,
        contractValidator,
        staticSynchronizerParameters,
        clock,
        targetProtocolVersion,
        loggerFactory,
      ),
      inFlightSubmissionSynchronizerTracker,
      ephemeral,
      synchronizerCrypto,
      sequencerClient,
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
}
