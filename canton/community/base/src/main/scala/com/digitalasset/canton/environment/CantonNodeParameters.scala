// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import com.digitalasset.canton.config.{
  CachingConfigs,
  LoggingConfig,
  ProcessingTimeout,
  QueryCostMonitoringConfig,
}
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.version.ProtocolVersion

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
    def nonStandardConfig: Boolean
    def dbMigrateAndStart: Boolean
    def skipTopologyManagerSignatureValidation: Boolean

  }
  object General {
    final case class Impl(
        tracing: TracingConfig,
        delayLoggingThreshold: NonNegativeFiniteDuration,
        logQueryCost: Option[QueryCostMonitoringConfig],
        loggingConfig: LoggingConfig,
        enableAdditionalConsistencyChecks: Boolean,
        enablePreviewFeatures: Boolean,
        processingTimeouts: ProcessingTimeout,
        sequencerClient: SequencerClientConfig,
        cachingConfigs: CachingConfigs,
        nonStandardConfig: Boolean,
        dbMigrateAndStart: Boolean,
        skipTopologyManagerSignatureValidation: Boolean,
    ) extends CantonNodeParameters.General
  }
  trait Protocol {
    def devVersionSupport: Boolean
    def dontWarnOnDeprecatedPV: Boolean

    /** The initial protocol version before connected to any domain, e.g., when creating the initial topology transactions. */
    def initialProtocolVersion: ProtocolVersion

  }
  object Protocol {
    final case class Impl(
        devVersionSupport: Boolean,
        dontWarnOnDeprecatedPV: Boolean,
        initialProtocolVersion: ProtocolVersion,
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
  override def nonStandardConfig: Boolean = general.nonStandardConfig
  override def dbMigrateAndStart: Boolean = general.dbMigrateAndStart
  override def skipTopologyManagerSignatureValidation: Boolean =
    general.skipTopologyManagerSignatureValidation

}

trait HasProtocolCantonNodeParameters extends CantonNodeParameters.Protocol {

  protected def protocol: CantonNodeParameters.Protocol

  def devVersionSupport: Boolean = protocol.devVersionSupport
  def dontWarnOnDeprecatedPV: Boolean = protocol.dontWarnOnDeprecatedPV
  def initialProtocolVersion: ProtocolVersion = protocol.initialProtocolVersion

}
