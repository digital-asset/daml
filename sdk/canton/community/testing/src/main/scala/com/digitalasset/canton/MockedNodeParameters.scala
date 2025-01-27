// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.StartupMemoryCheckConfig.ReportingLevel
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TracingConfig

object MockedNodeParameters {
  def cantonNodeParameters(
      _processingTimeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing,
      _cachingConfigs: CachingConfigs = CachingConfigs.testing,
      _batchingConfig: BatchingConfig = BatchingConfig(),
      _loggingConfig: LoggingConfig = LoggingConfig(),
      _enableAdditionalConsistencyChecks: Boolean = true,
      _nonStandardConfig: Boolean = false,
  ): CantonNodeParameters = new CantonNodeParameters {
    override def delayLoggingThreshold: NonNegativeFiniteDuration = ???

    override def enablePreviewFeatures: Boolean = ???

    override def enableAdditionalConsistencyChecks: Boolean = _enableAdditionalConsistencyChecks

    override def processingTimeouts: ProcessingTimeout = _processingTimeouts

    override def tracing: TracingConfig = ???

    override def sequencerClient: SequencerClientConfig = ???

    override def cachingConfigs: CachingConfigs = _cachingConfigs

    override def batchingConfig: BatchingConfig = _batchingConfig

    override def nonStandardConfig: Boolean = _nonStandardConfig

    override def loggingConfig: LoggingConfig = _loggingConfig

    override def sessionSigningKeys: SessionSigningKeysConfig = ???

    override def alphaVersionSupport: Boolean = ???

    override def betaVersionSupport: Boolean = ???

    override def dontWarnOnDeprecatedPV: Boolean = ???

    override def dbMigrateAndStart: Boolean = false

    override def exitOnFatalFailures: Boolean = true

    override def watchdog: Option[WatchdogConfig] = None

    override def startupMemoryCheckConfig: StartupMemoryCheckConfig = StartupMemoryCheckConfig(
      ReportingLevel.Warn
    )
  }
}
