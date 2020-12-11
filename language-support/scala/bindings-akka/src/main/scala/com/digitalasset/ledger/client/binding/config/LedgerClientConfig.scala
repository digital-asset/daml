// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.config

import java.io.File
import java.time.Duration

import com.daml.ledger.client.binding.LedgerClientConfigurationError.MalformedTypesafeConfig
import com.daml.ledger.client.binding.config.LedgerClientConfig.ClientSslConfig
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.typesafe.config.{Config, ConfigFactory}
import io.grpc.netty.GrpcSslContexts
import io.netty.handler.ssl.SslContext
import pureconfig._
import pureconfig.generic.auto._

import scala.util.Try

case class LedgerClientConfig(
    ledgerId: Option[String],
    commandClient: CommandClientConfiguration,
    maxRetryTime: Duration,
    ssl: Option[ClientSslConfig]
) {
  def toBindingConfig(applicationId: String) =
    LedgerClientConfiguration(
      applicationId,
      ledgerIdRequirement,
      commandClient,
      ssl.map(_.sslContext)
    )

  private val ledgerIdRequirement = LedgerIdRequirement(ledgerId)
}

object LedgerClientConfig {

  case class ClientSslConfig(
      clientKeyCertChainFile: File,
      clientKeyFile: File,
      trustedCertsFile: File) {

    def sslContext: SslContext =
      GrpcSslContexts
        .forClient()
        .keyManager(clientKeyCertChainFile, clientKeyFile)
        .trustManager(trustedCertsFile)
        .build()

  }

  def create(config: Config = ConfigFactory.load()): Try[LedgerClientConfig] = {
    wrapError(ConfigSource.fromConfig(config).at("ledger-client").load[LedgerClientConfig])
  }

  private def wrapError[T](
      failuresOrConfig: Either[pureconfig.error.ConfigReaderFailures, T]): Try[T] = {
    failuresOrConfig.left.map(MalformedTypesafeConfig).toTry
  }
}
