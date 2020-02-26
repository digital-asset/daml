// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import akka.stream.Materializer
import com.codahale.metrics.{MetricRegistry, SharedMetricRegistries}
import com.digitalasset.ledger.api.auth.{AuthService, AuthServiceWildcard}
import com.digitalasset.logging.LoggingContext
import com.digitalasset.platform.apiserver.{ApiServerConfig, TimeServiceBackend}
import com.digitalasset.platform.configuration.{CommandConfiguration, SubmissionConfiguration}
import com.digitalasset.platform.indexer.{IndexerConfig, IndexerStartupMode}
import com.digitalasset.resources.ResourceOwner
import scopt.OptionParser

import scala.concurrent.ExecutionContext

trait LedgerFactory[T <: KeyValueLedger, ExtraConfig] {
  val defaultExtraConfig: ExtraConfig

  def extraConfigParser(parser: OptionParser[Config[ExtraConfig]]): Unit

  def owner(config: Config[ExtraConfig], participantConfig: ParticipantConfig)(
      implicit executionContext: ExecutionContext,
      materializer: Materializer,
      logCtx: LoggingContext,
  ): ResourceOwner[T]

  def manipulateConfig(config: Config[ExtraConfig]): Config[ExtraConfig] =
    config

  def indexerConfig(config: ParticipantConfig): IndexerConfig =
    IndexerConfig(
      config.participantId,
      jdbcUrl = config.serverJdbcUrl,
      startupMode = IndexerStartupMode.MigrateAndStart,
      allowExistingSchema = config.allowExistingSchemaForIndex,
    )

  def indexerMetricRegistry(config: ParticipantConfig): MetricRegistry =
    SharedMetricRegistries.getOrCreate(s"indexer-${config.participantId}")

  def apiServerConfig(
      participantConfig: ParticipantConfig,
      config: Config[ExtraConfig]): ApiServerConfig =
    ApiServerConfig(
      participantId = participantConfig.participantId,
      archiveFiles = config.archiveFiles.map(_.toFile).toList,
      port = participantConfig.port,
      address = participantConfig.address,
      jdbcUrl = participantConfig.serverJdbcUrl,
      tlsConfig = config.tlsConfig,
      maxInboundMessageSize = Config.DefaultMaxInboundMessageSize,
      portFile = participantConfig.portFile,
    )

  def apiServerMetricRegistry(config: ParticipantConfig): MetricRegistry =
    SharedMetricRegistries.getOrCreate(s"ledger-api-server-${config.participantId}")

  def commandConfig(config: Config[ExtraConfig]): CommandConfiguration =
    CommandConfiguration.default

  def submissionConfig(config: Config[ExtraConfig]): SubmissionConfiguration =
    SubmissionConfiguration.default

  def timeServiceBackend(config: Config[ExtraConfig]): Option[TimeServiceBackend] = None

  def authService(config: Config[ExtraConfig]): AuthService =
    AuthServiceWildcard
}

object LedgerFactory {

  abstract class SimpleLedgerFactory[T <: KeyValueLedger] extends LedgerFactory[T, Unit] {
    override final val defaultExtraConfig: Unit = ()

    override final def extraConfigParser(parser: OptionParser[Config[Unit]]): Unit =
      ()
  }
}
