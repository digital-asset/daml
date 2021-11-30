// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import java.time.Duration
import java.util.concurrent.TimeUnit

import akka.stream.Materializer
import com.codahale.metrics.SharedMetricRegistries
import com.daml.ledger.api.auth.{AuthService, AuthServiceWildcard}
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.kvutils.api.{
  KeyValueLedger,
  KeyValueParticipantState,
  KeyValueParticipantStateReader,
  KeyValueParticipantStateWriter,
}
import com.daml.ledger.participant.state.v2.{ReadService, WritePackagesService, WriteService}
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.{ApiServerConfig, TimeServiceBackend}
import com.daml.platform.configuration.{InitialLedgerConfiguration, PartyConfiguration}
import com.daml.platform.indexer.{IndexerConfig, IndexerStartupMode}
import io.grpc.ServerInterceptor
import scopt.OptionParser

import scala.annotation.{nowarn, unused}
import scala.concurrent.duration.FiniteDuration

@nowarn("msg=parameter value config .* is never used") // possibly used in overrides
trait ConfigProvider[ExtraConfig] {
  val defaultExtraConfig: ExtraConfig

  def extraConfigParser(parser: OptionParser[Config[ExtraConfig]]): Unit

  def manipulateConfig(config: Config[ExtraConfig]): Config[ExtraConfig] =
    config

  def indexerConfig(
      participantConfig: ParticipantConfig,
      config: Config[ExtraConfig],
  ): IndexerConfig =
    IndexerConfig(
      participantConfig.participantId,
      jdbcUrl = participantConfig.serverJdbcUrl,
      startupMode = IndexerStartupMode.MigrateAndStart,
      eventsPageSize = config.eventsPageSize,
      eventsProcessingParallelism = config.eventsProcessingParallelism,
      allowExistingSchema = participantConfig.indexerConfig.allowExistingSchema,
      maxInputBufferSize = participantConfig.indexerConfig.maxInputBufferSize,
      inputMappingParallelism = participantConfig.indexerConfig.ingestionParallelism,
      batchingParallelism = participantConfig.indexerConfig.batchingParallelism,
      submissionBatchSize = participantConfig.indexerConfig.submissionBatchSize,
      tailingRateLimitPerSecond = participantConfig.indexerConfig.tailingRateLimitPerSecond,
      batchWithinMillis = participantConfig.indexerConfig.batchWithinMillis,
      enableCompression = participantConfig.indexerConfig.enableCompression,
    )

  def apiServerConfig(
      participantConfig: ParticipantConfig,
      config: Config[ExtraConfig],
  ): ApiServerConfig =
    ApiServerConfig(
      participantId = participantConfig.participantId,
      archiveFiles = config.archiveFiles.map(_.toFile).toList,
      port = participantConfig.port,
      address = participantConfig.address,
      jdbcUrl = participantConfig.serverJdbcUrl,
      databaseConnectionPoolSize = participantConfig.apiServerDatabaseConnectionPoolSize,
      databaseConnectionTimeout = FiniteDuration(
        participantConfig.apiServerDatabaseConnectionTimeout.toMillis,
        TimeUnit.MILLISECONDS,
      ),
      tlsConfig = config.tlsConfig,
      maxInboundMessageSize = config.maxInboundMessageSize,
      initialLedgerConfiguration = Some(initialLedgerConfig(config)),
      configurationLoadTimeout = config.configurationLoadTimeout,
      eventsPageSize = config.eventsPageSize,
      eventsProcessingParallelism = config.eventsProcessingParallelism,
      portFile = participantConfig.portFile,
      seeding = config.seeding,
      managementServiceTimeout = participantConfig.managementServiceTimeout,
      maxContractStateCacheSize = participantConfig.maxContractStateCacheSize,
      maxContractKeyStateCacheSize = participantConfig.maxContractKeyStateCacheSize,
      enableMutableContractStateCache = config.enableMutableContractStateCache,
      maxTransactionsInMemoryFanOutBufferSize =
        participantConfig.maxTransactionsInMemoryFanOutBufferSize,
      enableInMemoryFanOutForLedgerApi = config.enableInMemoryFanOutForLedgerApi,
      enableSelfServiceErrorCodes = config.enableSelfServiceErrorCodes,
    )

  def partyConfig(@unused config: Config[ExtraConfig]): PartyConfiguration =
    PartyConfiguration.default

  def initialLedgerConfig(config: Config[ExtraConfig]): InitialLedgerConfiguration = {
    InitialLedgerConfiguration(
      configuration = Configuration.reasonableInitialConfiguration.copy(maxDeduplicationTime =
        config.maxDeduplicationDuration.getOrElse(
          Configuration.reasonableInitialConfiguration.maxDeduplicationTime
        )
      ),
      // If a new index database is added to an already existing ledger,
      // a zero delay will likely produce a "configuration rejected" ledger entry,
      // because at startup the indexer hasn't ingested any configuration change yet.
      // Override this setting for distributed ledgers where you want to avoid these superfluous entries.
      delayBeforeSubmitting = Duration.ZERO,
    )
  }

  def timeServiceBackend(@unused config: Config[ExtraConfig]): Option[TimeServiceBackend] = None

  def authService(@unused config: Config[ExtraConfig]): AuthService =
    AuthServiceWildcard

  def interceptors(@unused config: Config[ExtraConfig]): List[ServerInterceptor] =
    List.empty

  def createMetrics(
      participantConfig: ParticipantConfig,
      @unused config: Config[ExtraConfig],
  ): Metrics = {
    val registryName = participantConfig.participantId + participantConfig.shardName
      .map("-" + _)
      .getOrElse("")
    new Metrics(SharedMetricRegistries.getOrCreate(registryName))
  }
}

trait ReadServiceOwner[ExtraConfig] extends ConfigProvider[ExtraConfig] {

  type RS <: ReadService

  def readServiceOwner(
      config: Config[ExtraConfig],
      participantConfig: ParticipantConfig,
      engine: Engine,
  )(implicit materializer: Materializer, loggingContext: LoggingContext): ResourceOwner[RS]
}

trait WriteServiceOwner[ExtraConfig] extends ConfigProvider[ExtraConfig] {

  type WS <: WriteService

  def writePackageOwner(
      config: Config[ExtraConfig],
      participantConfig: ParticipantConfig,
      engine: Engine,
  )(implicit
      materializer: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[WritePackagesService]

  def writeServiceOwner(
      config: Config[ExtraConfig],
      participantConfig: ParticipantConfig,
      engine: Engine,
  )(implicit
      materializer: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[WriteService]
}

trait LedgerFactory[ExtraConfig]
    extends ReadServiceOwner[ExtraConfig]
    with WriteServiceOwner[ExtraConfig] {

  type RWS <: ReadWriteService

  def readWriteServiceOwner(
      config: Config[ExtraConfig],
      participantConfig: ParticipantConfig,
      engine: Engine,
  )(implicit
      materializer: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[RWS]
}

object LedgerFactory {

  abstract class KeyValueLedgerFactory[KVL <: KeyValueLedger, ExtraConfig]
      extends LedgerFactory[ExtraConfig] {

    override type RS = KeyValueParticipantStateReader
    override type WS = KeyValueParticipantStateWriter
    override type RWS = KeyValueParticipantState

    override def readServiceOwner(
        config: Config[ExtraConfig],
        participantConfig: ParticipantConfig,
        engine: Engine,
    )(implicit
        materializer: Materializer,
        loggingContext: LoggingContext,
    ): ResourceOwner[RS] = {
      owner(config, participantConfig, engine).map(ledgerReaderWriter => {
        val metrics = createMetrics(participantConfig, config)
        KeyValueParticipantStateReader(
          ledgerReaderWriter,
          metrics,
          config.enableSelfServiceErrorCodes,
        )
      })
    }

    override def writePackageOwner(
        config: Config[ExtraConfig],
        participantConfig: ParticipantConfig,
        engine: Engine,
    )(implicit
        materializer: Materializer,
        loggingContext: LoggingContext,
    ): ResourceOwner[WritePackagesService] =
      owner(config, participantConfig, engine).map(ledgerReaderWriter => {
        val metrics = createMetrics(participantConfig, config)
        new KeyValueParticipantStateWriter(
          ledgerReaderWriter,
          metrics,
        )
      })

    override def writeServiceOwner(
        config: Config[ExtraConfig],
        participantConfig: ParticipantConfig,
        engine: Engine,
    )(implicit
        materializer: Materializer,
        loggingContext: LoggingContext,
    ): ResourceOwner[WS] =
      owner(config, participantConfig, engine).map(ledgerReaderWriter => {
        val metrics = createMetrics(participantConfig, config)
        new KeyValueParticipantStateWriter(
          ledgerReaderWriter,
          metrics,
        )
      })

    override final def readWriteServiceOwner(
        config: Config[ExtraConfig],
        participantConfig: ParticipantConfig,
        engine: Engine,
    )(implicit
        materializer: Materializer,
        loggingContext: LoggingContext,
    ): ResourceOwner[RWS] =
      for {
        readService <- readServiceOwner(config, participantConfig, engine)
        writeService <- writeServiceOwner(config, participantConfig, engine)
      } yield new KeyValueParticipantState(
        readService,
        writeService,
      )

    def owner(
        config: Config[ExtraConfig],
        participantConfig: ParticipantConfig,
        engine: Engine,
    )(implicit materializer: Materializer, loggingContext: LoggingContext): ResourceOwner[KVL]

    override def extraConfigParser(parser: OptionParser[Config[ExtraConfig]]): Unit =
      ()
  }
}
