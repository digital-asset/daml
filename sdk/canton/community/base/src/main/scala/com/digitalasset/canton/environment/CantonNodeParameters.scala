// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import com.digitalasset.canton.config.{
  BatchingConfig,
  CachingConfigs,
  LoggingConfig,
  ProcessingTimeout,
  QueryCostMonitoringConfig,
  WatchdogConfig,
}
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TracingConfig

trait CantonNodeParameters extends CantonNodeParameters.General with CantonNodeParameters.Protocol

object CantonNodeParameters {
  trait General {
    def tracing: TracingConfig
    def delayLoggingThreshold: NonNegativeFiniteDuration
    def logQueryCost: Option[QueryCostMonitoringConfig]
    def loggingConfig: LoggingConfig
    def enableAdditionalConsistencyChecks: Boolean
    def enablePreviewFeatures: Boolean
    def processingTimeouts: ProcessingTimeout
    def sequencerClient: SequencerClientConfig
    def cachingConfigs: CachingConfigs
    def batchingConfig: BatchingConfig
    def nonStandardConfig: Boolean
    def dbMigrateAndStart: Boolean
    def exitOnFatalFailures: Boolean
    def watchdog: Option[WatchdogConfig]
  }
  object General {
    final case class Impl(
        override val tracing: TracingConfig,
        override val delayLoggingThreshold: NonNegativeFiniteDuration,
        override val logQueryCost: Option[QueryCostMonitoringConfig],
        override val loggingConfig: LoggingConfig,
        override val enableAdditionalConsistencyChecks: Boolean,
        override val enablePreviewFeatures: Boolean,
        override val processingTimeouts: ProcessingTimeout,
        override val sequencerClient: SequencerClientConfig,
        override val cachingConfigs: CachingConfigs,
        override val batchingConfig: BatchingConfig,
        override val nonStandardConfig: Boolean,
        override val dbMigrateAndStart: Boolean,
        override val exitOnFatalFailures: Boolean,
        override val watchdog: Option[WatchdogConfig],
    ) extends CantonNodeParameters.General
  }
  trait Protocol {
    def alphaVersionSupport: Boolean
    def betaVersionSupport: Boolean
    def dontWarnOnDeprecatedPV: Boolean
  }

  object Protocol {
    final case class Impl(
        alphaVersionSupport: Boolean,
        betaVersionSupport: Boolean,
        dontWarnOnDeprecatedPV: Boolean,
    ) extends CantonNodeParameters.Protocol
  }
}

trait HasGeneralCantonNodeParameters extends CantonNodeParameters.General {

  protected def general: CantonNodeParameters.General

  override def tracing: TracingConfig = general.tracing
  override def delayLoggingThreshold: NonNegativeFiniteDuration = general.delayLoggingThreshold
  override def logQueryCost: Option[QueryCostMonitoringConfig] = general.logQueryCost
  override def loggingConfig: LoggingConfig = general.loggingConfig
  override def enableAdditionalConsistencyChecks: Boolean =
    general.enableAdditionalConsistencyChecks
  override def enablePreviewFeatures: Boolean = general.enablePreviewFeatures
  override def processingTimeouts: ProcessingTimeout = general.processingTimeouts
  override def sequencerClient: SequencerClientConfig = general.sequencerClient
  override def cachingConfigs: CachingConfigs = general.cachingConfigs
  override def batchingConfig: BatchingConfig = general.batchingConfig
  override def nonStandardConfig: Boolean = general.nonStandardConfig
  override def dbMigrateAndStart: Boolean = general.dbMigrateAndStart
  override def exitOnFatalFailures: Boolean = general.exitOnFatalFailures
  override def watchdog: Option[WatchdogConfig] = general.watchdog
}

trait HasProtocolCantonNodeParameters extends CantonNodeParameters.Protocol {

  protected def protocol: CantonNodeParameters.Protocol

  def alphaVersionSupport: Boolean = protocol.alphaVersionSupport
  def betaVersionSupport: Boolean = protocol.betaVersionSupport
  def dontWarnOnDeprecatedPV: Boolean = protocol.dontWarnOnDeprecatedPV
}
