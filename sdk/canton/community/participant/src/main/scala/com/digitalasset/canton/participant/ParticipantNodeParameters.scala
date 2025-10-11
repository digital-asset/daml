// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveNumeric}
import com.digitalasset.canton.config.StartupMemoryCheckConfig.ReportingLevel
import com.digitalasset.canton.environment.{CantonNodeParameters, HasGeneralCantonNodeParameters}
import com.digitalasset.canton.participant.admin.AdminWorkflowConfig
import com.digitalasset.canton.participant.config.*
import com.digitalasset.canton.participant.sync.CommandProgressTrackerConfig
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.time
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

final case class ParticipantNodeParameters(
    general: CantonNodeParameters.General,
    activationFrequencyForWarnAboutConsistencyChecks: Long,
    adminWorkflow: AdminWorkflowConfig,
    maxUnzippedDarSize: Int,
    stores: ParticipantStoreConfig,
    protocolConfig: ParticipantProtocolConfig,
    ledgerApiServerParameters: LedgerApiServerParametersConfig,
    engine: CantonEngineConfig,
    journalGarbageCollectionDelay: time.NonNegativeFiniteDuration,
    disableUpgradeValidation: Boolean,
    enableStrictDarValidation: Boolean,
    commandProgressTracking: CommandProgressTrackerConfig,
    unsafeOnlinePartyReplication: Option[UnsafeOnlinePartyReplicationConfig],
    automaticallyPerformLogicalSynchronizerUpgrade: Boolean,
    reassignmentsConfig: ReassignmentsConfig,
    doNotAwaitOnCheckingIncomingCommitments: Boolean,
    disableOptionalTopologyChecks: Boolean,
) extends CantonNodeParameters
    with HasGeneralCantonNodeParameters {
  override def dontWarnOnDeprecatedPV: Boolean = protocolConfig.dontWarnOnDeprecatedPV
  override def alphaVersionSupport: Boolean = protocolConfig.alphaVersionSupport
  override def betaVersionSupport: Boolean = protocolConfig.betaVersionSupport
}

object ParticipantNodeParameters {
  @VisibleForTesting
  def forTestingOnly(testedProtocolVersion: ProtocolVersion) = ParticipantNodeParameters(
    general = CantonNodeParameters.General.Impl(
      tracing = TracingConfig(TracingConfig.Propagation.Disabled),
      delayLoggingThreshold = time.NonNegativeFiniteDuration.tryOfMillis(5000),
      enableAdditionalConsistencyChecks = true,
      loggingConfig = LoggingConfig(api = ApiLoggingConfig(messagePayloads = true)),
      processingTimeouts = DefaultProcessingTimeouts.testing,
      enablePreviewFeatures = false,
      // TODO(i15561): Revert back to `false` once there is a stable Daml 3 protocol version
      nonStandardConfig = true,
      cachingConfigs = CachingConfigs(),
      batchingConfig = BatchingConfig(
        maxPruningBatchSize = PositiveNumeric.tryCreate(10),
        aggregator = BatchAggregatorConfig.defaultsForTesting,
      ),
      sequencerClient = SequencerClientConfig(),
      dbMigrateAndStart = false,
      exitOnFatalFailures = true,
      watchdog = None,
      startupMemoryCheckConfig = StartupMemoryCheckConfig(ReportingLevel.Warn),
    ),
    activationFrequencyForWarnAboutConsistencyChecks = 1000L,
    adminWorkflow = AdminWorkflowConfig(
      bongTestMaxLevel = NonNegativeInt.tryCreate(10)
    ),
    maxUnzippedDarSize = 10,
    stores = ParticipantStoreConfig(),
    protocolConfig = ParticipantProtocolConfig(
      Some(testedProtocolVersion),
      // TODO(i15561): Revert back to `false` once there is a stable Daml 3 protocol version
      alphaVersionSupport = true,
      betaVersionSupport = true,
      dontWarnOnDeprecatedPV = false,
    ),
    ledgerApiServerParameters = LedgerApiServerParametersConfig(),
    engine = CantonEngineConfig(),
    journalGarbageCollectionDelay = time.NonNegativeFiniteDuration.Zero,
    disableUpgradeValidation = false,
    enableStrictDarValidation = false,
    commandProgressTracking = CommandProgressTrackerConfig(),
    unsafeOnlinePartyReplication = None,
    automaticallyPerformLogicalSynchronizerUpgrade = true,
    reassignmentsConfig = ReassignmentsConfig(
      targetTimestampForwardTolerance = NonNegativeFiniteDuration.ofSeconds(30)
    ),
    doNotAwaitOnCheckingIncomingCommitments = false,
    disableOptionalTopologyChecks = false,
  )
}
