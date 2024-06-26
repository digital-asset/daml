// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveNumeric}
import com.digitalasset.canton.config.{
  ApiLoggingConfig,
  BatchAggregatorConfig,
  BatchingConfig,
  CachingConfigs,
  DefaultProcessingTimeouts,
  LoggingConfig,
}
import com.digitalasset.canton.environment.{CantonNodeParameters, HasGeneralCantonNodeParameters}
import com.digitalasset.canton.participant.admin.AdminWorkflowConfig
import com.digitalasset.canton.participant.config.{
  CantonEngineConfig,
  LedgerApiServerParametersConfig,
  ParticipantProtocolConfig,
  ParticipantStoreConfig,
  PartyNotificationConfig,
}
import com.digitalasset.canton.participant.sync.CommandProgressTrackerConfig
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.version.ProtocolVersion

final case class ParticipantNodeParameters(
    general: CantonNodeParameters.General,
    partyChangeNotification: PartyNotificationConfig,
    adminWorkflow: AdminWorkflowConfig,
    maxUnzippedDarSize: Int,
    stores: ParticipantStoreConfig,
    transferTimeProofFreshnessProportion: NonNegativeInt,
    protocolConfig: ParticipantProtocolConfig,
    ledgerApiServerParameters: LedgerApiServerParametersConfig,
    excludeInfrastructureTransactions: Boolean,
    engine: CantonEngineConfig,
    journalGarbageCollectionDelay: NonNegativeFiniteDuration,
    disableUpgradeValidation: Boolean,
    allowForUnauthenticatedContractIds: Boolean,
    commandProgressTracking: CommandProgressTrackerConfig,
) extends CantonNodeParameters
    with HasGeneralCantonNodeParameters {
  override def dontWarnOnDeprecatedPV: Boolean = protocolConfig.dontWarnOnDeprecatedPV
  override def devVersionSupport: Boolean = protocolConfig.devVersionSupport
  override def betaVersionSupport: Boolean = protocolConfig.betaVersionSupport
}

object ParticipantNodeParameters {
  def forTestingOnly(testedProtocolVersion: ProtocolVersion) = ParticipantNodeParameters(
    general = CantonNodeParameters.General.Impl(
      tracing = TracingConfig(TracingConfig.Propagation.Disabled),
      delayLoggingThreshold = NonNegativeFiniteDuration.tryOfMillis(5000),
      enableAdditionalConsistencyChecks = true,
      loggingConfig = LoggingConfig(api = ApiLoggingConfig(messagePayloads = true)),
      logQueryCost = None,
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
      useUnifiedSequencer = false,
      watchdog = None,
    ),
    partyChangeNotification = PartyNotificationConfig.Eager,
    adminWorkflow = AdminWorkflowConfig(
      bongTestMaxLevel = NonNegativeInt.tryCreate(10)
    ),
    maxUnzippedDarSize = 10,
    stores = ParticipantStoreConfig(),
    transferTimeProofFreshnessProportion = NonNegativeInt.tryCreate(3),
    protocolConfig = ParticipantProtocolConfig(
      Some(testedProtocolVersion),
      // TODO(i15561): Revert back to `false` once there is a stable Daml 3 protocol version
      devVersionSupport = true,
      betaVersionSupport = true,
      dontWarnOnDeprecatedPV = false,
    ),
    ledgerApiServerParameters = LedgerApiServerParametersConfig(),
    excludeInfrastructureTransactions = true,
    engine = CantonEngineConfig(),
    journalGarbageCollectionDelay = NonNegativeFiniteDuration.Zero,
    disableUpgradeValidation = false,
    allowForUnauthenticatedContractIds = false,
    commandProgressTracking = CommandProgressTrackerConfig(),
  )
}
