// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.config

import java.io.File

import com.digitalasset.ledger.client.configuration.TlsConfiguration
import com.digitalasset.platform.services.time.{TimeModel, TimeProviderType}

import scala.concurrent.duration._

final case class TlsServerConfiguration(
    enabled: Boolean,
    keyCertChainFile: File,
    keyFile: File,
    trustCertCollectionFile: File)

/**
  * Defines the basic configuration for running sandbox
  */
final case class SandboxConfig(
    address: Option[String],
    port: Int,
    damlPackageContainer: DamlPackageContainer,
    timeProviderType: TimeProviderType,
    timeModel: TimeModel,
    commandConfig: CommandConfiguration, //TODO: this should go to the file config
    tlsConfig: Option[TlsConfiguration],
    scenario: Option[String],
    ledgerIdMode: LedgerIdMode,
    jdbcUrl: Option[String]
)

final case class CommandConfiguration(
    inputBufferSize: Int,
    maxParallelSubmissions: Int,
    maxCommandsInFlight: Int,
    limitMaxCommandsInFlight: Boolean,
    historySize: Int,
    retentionPeriod: FiniteDuration,
    commandTtl: FiniteDuration)

object SandboxConfig {

  val DefaultPort = 6865

  def default: SandboxConfig = {
    SandboxConfig(
      None,
      DefaultPort,
      DamlPackageContainer(Nil),
      TimeProviderType.Static,
      TimeModel.reasonableDefault,
      defaultCommandConfig,
      tlsConfig = None,
      scenario = None,
      ledgerIdMode = LedgerIdMode.Random,
      jdbcUrl = None
    )
  }

  lazy val defaultCommandConfig =
    CommandConfiguration(
      inputBufferSize = 512,
      maxParallelSubmissions = 128,
      maxCommandsInFlight = 256,
      limitMaxCommandsInFlight = true,
      historySize = 5000,
      retentionPeriod = 24.hours,
      commandTtl = 20.seconds
    )
}
