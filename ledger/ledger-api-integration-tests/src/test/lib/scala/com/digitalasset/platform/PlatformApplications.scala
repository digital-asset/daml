// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform

import java.io.File
import java.nio.file.Path
import java.time.Duration

import com.digitalasset.platform.sandbox.SandboxApplication
import com.digitalasset.platform.sandbox.config.{
  CommandConfiguration,
  DamlPackageContainer,
  LedgerIdMode,
  SandboxConfig
}
import com.digitalasset.platform.services.time.{TimeModel, TimeProviderType}
import scalaz.NonEmptyList

import scala.concurrent.duration.{FiniteDuration, _}

object PlatformApplications {

  /**
    * Meant to be a simple common denominator for fixture configuration. The constructor is private to avoid using
    * exceptions for validation.
    *
    * In the companion object add more smart constructors with
    * validation if you need other parameters than the ones provided
    * existing smart constructors
    */
  final case class Config private (
      ledgerId: Option[String],
      darFiles: List[Path],
      parties: NonEmptyList[String],
      committerParty: String,
      timeProviderType: TimeProviderType,
      timeModel: TimeModel,
      heartBeatInterval: FiniteDuration = 5.seconds,
      persistenceEnabled: Boolean = false,
      maxNumberOfAcsContracts: Option[Int] = None,
      commandConfiguration: CommandConfiguration = SandboxConfig.defaultCommandConfig) {
    require(
      Duration.ofSeconds(timeModel.minTtl.getSeconds) == timeModel.minTtl &&
        Duration.ofSeconds(timeModel.maxTtl.getSeconds) == timeModel.maxTtl,
      "Max TTL's granularity is subsecond. Ledger Server does not support subsecond granularity for this configuration - please use whole seconds."
    )

    def getLedgerId: String =
      ledgerId.getOrElse(
        throw new IllegalStateException("Attempted to access ledger ID, but none is configured."))

    def withDarFile(path: Path) = copy(darFiles = List(path))

    def withDarFiles(path: List[Path]) = copy(darFiles = path)

    def withTimeProvider(tpt: TimeProviderType) = copy(timeProviderType = tpt)

    def withLedgerId(id: Option[String]) = copy(ledgerId = id)

    def withParties(p1: String, rest: String*) = copy(parties = NonEmptyList(p1, rest: _*))

    def withCommitterParty(committer: String) = copy(committerParty = committer)

    def withPersistence(enabled: Boolean) = copy(persistenceEnabled = enabled)

    def withHeartBeatInterval(interval: FiniteDuration) = copy(heartBeatInterval = interval)

    def withMaxNumberOfAcsContracts(cap: Int) = copy(maxNumberOfAcsContracts = Some(cap))

    def withCommandConfiguration(cc: CommandConfiguration) = copy(commandConfiguration = cc)
  }

  object Config {
    val defaultLedgerId = "ledger server"

    val defaultDarFile = new File("ledger/sandbox/Test.dar")

    val defaultParties = NonEmptyList("party", "Alice", "Bob")
    val defaultTimeProviderType = TimeProviderType.Static

    def defaultWithLedgerId(ledgerId: Option[String]): Config = {
      val darFiles = List(defaultDarFile)
      new Config(
        ledgerId,
        darFiles.map(_.toPath),
        defaultParties,
        "committer",
        defaultTimeProviderType,
        TimeModel.reasonableDefault
      )
    }

    def defaultWithTimeProvider(timeProviderType: TimeProviderType) =
      defaultWithLedgerId(Some(defaultLedgerId)).withTimeProvider(timeProviderType)

    def default: Config = defaultWithLedgerId(Some(defaultLedgerId))
  }

  def sandboxApplication(config: Config, jdbcUrl: Option[String]) = {
    val selectedPort = 0

    SandboxApplication(
      SandboxConfig(
        address = None,
        port = selectedPort,
        damlPackageContainer = DamlPackageContainer(config.darFiles.map(_.toFile)),
        timeProviderType = config.timeProviderType,
        timeModel = config.timeModel,
        commandConfig = config.commandConfiguration,
        scenario = None,
        tlsConfig = None,
        ledgerIdMode =
          config.ledgerId.fold[LedgerIdMode](LedgerIdMode.Random)(LedgerIdMode.PreDefined),
        jdbcUrl = jdbcUrl
      )
    )
  }
}
