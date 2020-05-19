// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import akka.stream.Materializer
import com.codahale.metrics.SharedMetricRegistries
import com.daml.ledger.api.auth.{AuthService, AuthServiceWildcard}
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.v1.{ReadService, WriteService}
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.{ApiServerConfig, TimeServiceBackend}
import com.daml.platform.configuration.{
  CommandConfiguration,
  LedgerConfiguration,
  PartyConfiguration
}
import com.daml.platform.indexer.{IndexerConfig, IndexerStartupMode}
import com.daml.resources.ResourceOwner
import scopt.OptionParser

trait ConfigProvider[ExtraConfig] {
  val defaultExtraConfig: ExtraConfig

  def extraConfigParser(parser: OptionParser[Config[ExtraConfig]]): Unit

  def manipulateConfig(config: Config[ExtraConfig]): Config[ExtraConfig] =
    config

  def indexerConfig(
      participantConfig: ParticipantConfig,
      config: Config[ExtraConfig]): IndexerConfig =
    IndexerConfig(
      participantConfig.participantId,
      jdbcUrl = participantConfig.serverJdbcUrl,
      startupMode = IndexerStartupMode.MigrateAndStart,
      eventsPageSize = config.eventsPageSize,
      allowExistingSchema = participantConfig.allowExistingSchemaForIndex,
    )

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
      eventsPageSize = config.eventsPageSize,
      portFile = participantConfig.portFile,
      seeding = config.seeding,
    )

  def commandConfig(
      participantConfig: ParticipantConfig,
      config: Config[ExtraConfig]): CommandConfiguration = {
    val defaultMaxCommandsInFlight = CommandConfiguration.default.maxCommandsInFlight

    CommandConfiguration.default.copy(
      maxCommandsInFlight =
        participantConfig.maxCommandsInFlight.getOrElse(defaultMaxCommandsInFlight),
    )
  }

  def partyConfig(config: Config[ExtraConfig]): PartyConfiguration =
    PartyConfiguration.default

  def ledgerConfig(config: Config[ExtraConfig]): LedgerConfiguration =
    LedgerConfiguration.defaultRemote

  def timeServiceBackend(config: Config[ExtraConfig]): Option[TimeServiceBackend] = None

  def authService(config: Config[ExtraConfig]): AuthService =
    AuthServiceWildcard

  def createMetrics(
      participantConfig: ParticipantConfig,
      config: Config[ExtraConfig],
  ): Metrics =
    new Metrics(SharedMetricRegistries.getOrCreate(participantConfig.participantId))
}

trait ReadServiceOwner[+RS <: ReadService, ExtraConfig] extends ConfigProvider[ExtraConfig] {
  def readServiceOwner(
      config: Config[ExtraConfig],
      participantConfig: ParticipantConfig,
      engine: Engine,
  )(implicit materializer: Materializer, logCtx: LoggingContext): ResourceOwner[RS]
}

trait WriteServiceOwner[+WS <: WriteService, ExtraConfig] extends ConfigProvider[ExtraConfig] {
  def writeServiceOwner(
      config: Config[ExtraConfig],
      participantConfig: ParticipantConfig,
      engine: Engine,
  )(implicit materializer: Materializer, logCtx: LoggingContext): ResourceOwner[WS]
}

trait LedgerFactory[+RWS <: ReadWriteService, ExtraConfig]
    extends ReadServiceOwner[RWS, ExtraConfig]
    with WriteServiceOwner[RWS, ExtraConfig] {

  override final def readServiceOwner(
      config: Config[ExtraConfig],
      participantConfig: ParticipantConfig,
      engine: Engine,
  )(implicit materializer: Materializer, logCtx: LoggingContext): ResourceOwner[RWS] =
    readWriteServiceOwner(config, participantConfig, engine)

  override final def writeServiceOwner(
      config: Config[ExtraConfig],
      participantConfig: ParticipantConfig,
      engine: Engine,
  )(implicit materializer: Materializer, logCtx: LoggingContext): ResourceOwner[RWS] =
    readWriteServiceOwner(config, participantConfig, engine)

  def readWriteServiceOwner(
      config: Config[ExtraConfig],
      participantConfig: ParticipantConfig,
      engine: Engine,
  )(implicit materializer: Materializer, logCtx: LoggingContext): ResourceOwner[RWS]
}

object LedgerFactory {

  abstract class KeyValueLedgerFactory[KVL <: KeyValueLedger]
      extends LedgerFactory[KeyValueParticipantState, Unit] {
    override final val defaultExtraConfig: Unit = ()

    override final def readWriteServiceOwner(
        config: Config[Unit],
        participantConfig: ParticipantConfig,
        engine: Engine,
    )(
        implicit materializer: Materializer,
        logCtx: LoggingContext,
    ): ResourceOwner[KeyValueParticipantState] =
      for {
        readerWriter <- owner(config, participantConfig, engine)
      } yield
        new KeyValueParticipantState(
          readerWriter,
          readerWriter,
          createMetrics(participantConfig, config),
        )

    def owner(
        value: Config[Unit],
        config: ParticipantConfig,
        engine: Engine,
    )(implicit materializer: Materializer, logCtx: LoggingContext): ResourceOwner[KVL]

    override final def extraConfigParser(parser: OptionParser[Config[Unit]]): Unit =
      ()
  }
}
