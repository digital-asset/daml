// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform

import java.io.File
import java.nio.file.Path
import java.time.Duration

import ch.qos.logback.classic.Level
import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.tls.TlsConfiguration
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.sandbox.config.{CommandConfiguration, SandboxConfig}
import com.digitalasset.platform.services.time.{TimeModel, TimeProviderType}

import scala.concurrent.duration.{FiniteDuration, _}
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.platform.apitesting.TestParties

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
      ledgerId: LedgerIdMode,
      darFiles: List[Path],
      parties: List[String],
      committerParty: String,
      timeProviderType: TimeProviderType,
      timeModel: TimeModel,
      commandSubmissionTtlScaleFactor: Double = 1.0,
      heartBeatInterval: FiniteDuration = 5.seconds,
      persistenceEnabled: Boolean = false,
      maxNumberOfAcsContracts: Option[Int] = None,
      commandConfiguration: CommandConfiguration = SandboxConfig.defaultCommandConfig,
      uniqueCommandIdentifiers: Boolean = true,
      uniquePartyIdentifiers: Boolean = true,
      remoteApiEndpoint: Option[RemoteApiEndpointMode] = None) {
    require(
      Duration.ofSeconds(timeModel.minTtl.getSeconds) == timeModel.minTtl &&
        Duration.ofSeconds(timeModel.maxTtl.getSeconds) == timeModel.maxTtl,
      "Max TTL's granularity is subsecond. Ledger Server does not support subsecond granularity for this configuration - please use whole seconds."
    )

    def withDarFile(path: Path) = copy(darFiles = List(path))

    def withDarFiles(path: List[Path]) = copy(darFiles = path)

    def withTimeProvider(tpt: TimeProviderType) = copy(timeProviderType = tpt)

    def withLedgerIdMode(mode: LedgerIdMode): Config = copy(ledgerId = mode)

    def withUniquePartyIdentifiers(uniqueIdentifiers: Boolean): Config =
      copy(uniquePartyIdentifiers = uniqueIdentifiers)

    def withUniqueCommandIdentifiers(uniqueIdentifiers: Boolean): Config =
      copy(uniqueCommandIdentifiers = uniqueIdentifiers)

    def withParties(p1: String, rest: String*) = copy(parties = p1 +: rest.toList)

    def withCommitterParty(committer: String) = copy(committerParty = committer)

    def withPersistence(enabled: Boolean) = copy(persistenceEnabled = enabled)

    def withHeartBeatInterval(interval: FiniteDuration) = copy(heartBeatInterval = interval)

    def withCommandSubmissionTtlScaleFactor(factor: Double) =
      copy(commandSubmissionTtlScaleFactor = factor)

    def withMaxNumberOfAcsContracts(cap: Int) = copy(maxNumberOfAcsContracts = Some(cap))

    def withCommandConfiguration(cc: CommandConfiguration) = copy(commandConfiguration = cc)

    def withRemoteApiEndpoint(endpoint: RemoteApiEndpointMode) =
      copy(remoteApiEndpoint = Some(endpoint))
  }

  final case class RemoteApiEndpoint(
      host: String,
      port: Integer,
      tlsConfig: Option[TlsConfiguration]) {
    def withHost(host: String) = copy(host = host)
    def withPort(port: Int) = copy(port = port)
    def withTlsConfig(tlsConfig: Option[TlsConfiguration]) = copy(tlsConfig = tlsConfig)
  }

  object RemoteApiEndpoint {
    def default: RemoteApiEndpoint = RemoteApiEndpoint("localhost", 6865, None)
  }

  object Config {
    val defaultLedgerId: LedgerId = LedgerId(Ref.LedgerString.assertFromString("ledger-server"))
    val defaultDarFile = new File(rlocation("ledger/sandbox/Test.dar"))
    val defaultParties = TestParties.AllParties
    val defaultTimeProviderType = TimeProviderType.Static

    def default: Config = {
      val darFiles = List(defaultDarFile)
      new Config(
        LedgerIdMode.Static(defaultLedgerId),
        darFiles.map(_.toPath),
        defaultParties,
        "committer",
        defaultTimeProviderType,
        TimeModel.reasonableDefault
      )
    }
  }

  def sandboxConfig(config: Config, jdbcUrl: Option[String]) = {
    val selectedPort = 0

    SandboxConfig(
      address = None,
      port = selectedPort,
      None,
      damlPackages = config.darFiles.map(_.toFile),
      timeProviderType = config.timeProviderType,
      timeModel = config.timeModel,
      commandConfig = config.commandConfiguration,
      scenario = None,
      tlsConfig = None,
      ledgerIdMode = config.ledgerId,
      jdbcUrl = jdbcUrl,
      eagerPackageLoading = false,
      logLevel = Level.INFO
    )
  }
}
