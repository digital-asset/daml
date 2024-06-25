// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import com.digitalasset.canton.config
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
    uniqueContractKeys: Boolean,
    ledgerApiServerParameters: LedgerApiServerParametersConfig,
    excludeInfrastructureTransactions: Boolean,
    engine: CantonEngineConfig,
    allowForUnauthenticatedContractIds: Boolean,
    disableUpgradeValidation: Boolean,
    commandProgressTracking: CommandProgressTrackerConfig,
) extends CantonNodeParameters
    with HasGeneralCantonNodeParameters {
  override def dontWarnOnDeprecatedPV: Boolean = protocolConfig.dontWarnOnDeprecatedPV
  override def devVersionSupport: Boolean = protocolConfig.devVersionSupport
  override def betaVersionSupport: Boolean = protocolConfig.betaVersionSupport
  override def initialProtocolVersion: ProtocolVersion = protocolConfig.initialProtocolVersion

}

object ParticipantNodeParameters {
  def forTestingOnly(testedProtocolVersion: ProtocolVersion) = ParticipantNodeParameters(
    general = CantonNodeParameters.General.Impl(
      tracing = TracingConfig(TracingConfig.Propagation.Disabled),
      delayLoggingThreshold = NonNegativeFiniteDuration.tryOfMillis(5000),
      enableAdditionalConsistencyChecks = true,
      loggingConfig = LoggingConfig(api = ApiLoggingConfig(messagePayloads = Some(true))),
      logQueryCost = None,
      processingTimeouts = DefaultProcessingTimeouts.testing,
      enablePreviewFeatures = false,
      nonStandardConfig = false,
      cachingConfigs = CachingConfigs(),
      batchingConfig = BatchingConfig(),
      sequencerClient = SequencerClientConfig(),
      dbMigrateAndStart = false,
      skipTopologyManagerSignatureValidation = false,
      exitOnFatalFailures = true,
      watchdog = None,
    ),
    partyChangeNotification = PartyNotificationConfig.Eager,
    adminWorkflow = AdminWorkflowConfig(
      bongTestMaxLevel = 10,
      retries = 10,
      submissionTimeout = config.NonNegativeFiniteDuration.ofHours(1),
    ),
    maxUnzippedDarSize = 10,
    stores = ParticipantStoreConfig(
      maxPruningBatchSize = PositiveNumeric.tryCreate(10),
      acsPruningInterval = config.NonNegativeFiniteDuration.ofSeconds(30),
      dbBatchAggregationConfig = BatchAggregatorConfig.defaultsForTesting,
    ),
    transferTimeProofFreshnessProportion = NonNegativeInt.tryCreate(3),
    protocolConfig = ParticipantProtocolConfig(
      Some(testedProtocolVersion),
      devVersionSupport = false,
      betaVersionSupport = false,
      dontWarnOnDeprecatedPV = false,
      initialProtocolVersion = testedProtocolVersion,
    ),
    uniqueContractKeys = false,
    ledgerApiServerParameters = LedgerApiServerParametersConfig(),
    excludeInfrastructureTransactions = true,
    engine = CantonEngineConfig(),
    allowForUnauthenticatedContractIds = false,
    disableUpgradeValidation = true,
    commandProgressTracking = CommandProgressTrackerConfig(),
  )
}
