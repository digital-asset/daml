// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import akka.stream.Materializer
import com.codahale.metrics.{MetricRegistry, SharedMetricRegistries}
import com.digitalasset.ledger.api.auth.{AuthService, AuthServiceWildcard}
import com.digitalasset.logging.LoggingContext
import com.digitalasset.platform.apiserver.{ApiServerConfig, TimeServiceBackend}
import com.digitalasset.platform.indexer.{IndexerConfig, IndexerStartupMode}
import com.digitalasset.resources.ResourceOwner
import scopt.OptionParser

import scala.concurrent.ExecutionContext

trait LedgerFactory[T <: KeyValueLedger, ExtraConfig] {
  val defaultExtraConfig: ExtraConfig

  def extraConfigParser(parser: OptionParser[Config[ExtraConfig]]): Unit

  def owner(config: Config[ExtraConfig])(
      implicit executionContext: ExecutionContext,
      materializer: Materializer,
      logCtx: LoggingContext,
  ): ResourceOwner[T]

  def manipulateConfig(config: Config[ExtraConfig]): Config[ExtraConfig] =
    config

  def indexerConfig(config: Config[ExtraConfig]): IndexerConfig =
    IndexerConfig(
      config.participantId,
      jdbcUrl = config.serverJdbcUrl,
      startupMode = IndexerStartupMode.MigrateAndStart,
      allowExistingSchema = config.allowExistingSchemaForIndex,
    )

  def indexerMetricRegistry(config: Config[ExtraConfig]): MetricRegistry =
    SharedMetricRegistries.getOrCreate(s"indexer-${config.participantId}")

  def apiServerConfig(config: Config[ExtraConfig]): ApiServerConfig =
    ApiServerConfig(
      participantId = config.participantId,
      archiveFiles = config.archiveFiles.map(_.toFile).toList,
      port = config.port,
      address = config.address,
      jdbcUrl = config.serverJdbcUrl,
      tlsConfig = None,
      maxInboundMessageSize = Config.DefaultMaxInboundMessageSize,
      portFile = config.portFile,
    )

  def apiServerMetricRegistry(config: Config[ExtraConfig]): MetricRegistry =
    SharedMetricRegistries.getOrCreate(s"ledger-api-server-${config.participantId}")

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
