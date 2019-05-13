// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform

import java.io.File
import java.nio.file.Path
import java.time.Duration

import com.digitalasset.ledger.api.tls.TlsConfiguration
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
      ledgerId: RequestedLedgerAPIMode,
      darFiles: List[Path],
      parties: NonEmptyList[String],
      committerParty: String,
      timeProviderType: TimeProviderType,
      timeModel: TimeModel,
      heartBeatInterval: FiniteDuration = 5.seconds,
      persistenceEnabled: Boolean = false,
      maxNumberOfAcsContracts: Option[Int] = None,
      commandConfiguration: CommandConfiguration = SandboxConfig.defaultCommandConfig,
      // TODO(gleber): move these options into RemoteAPIProxy-specific configuration.
                                  // FIXME: make these three into a small 3-tuple-like case class
      host: Option[String] = None,
      port: Option[Int] = None,
      tlsConfig: Option[TlsConfiguration] = None) {
    require(
      Duration.ofSeconds(timeModel.minTtl.getSeconds) == timeModel.minTtl &&
        Duration.ofSeconds(timeModel.maxTtl.getSeconds) == timeModel.maxTtl,
      "Max TTL's granularity is subsecond. Ledger Server does not support subsecond granularity for this configuration - please use whole seconds."
    )

    @SuppressWarnings(Array("org.wartremover.warts.StringPlusAny"))
    def assertStaticLedgerId: String =
      ledgerId match {
        case RequestedLedgerAPIMode.Static(ledgerId) => ledgerId
        case _ =>
          throw new IllegalArgumentException("Unsupported ledger id config: " + ledgerId)
      }

    def withDarFile(path: Path) = copy(darFiles = List(path))

    def withDarFiles(path: List[Path]) = copy(darFiles = path)

    def withTimeProvider(tpt: TimeProviderType) = copy(timeProviderType = tpt)

    // FIXME: make it into one function taking one value
    def withLedgerId(id: String) = copy(ledgerId = RequestedLedgerAPIMode.Static(id))
    def withRandomLedgerId() = copy(ledgerId = RequestedLedgerAPIMode.Random())
    def withDynamicLedgerId() = copy(ledgerId = RequestedLedgerAPIMode.Dynamic())

    def withParties(p1: String, rest: String*) = copy(parties = NonEmptyList(p1, rest: _*))

    def withCommitterParty(committer: String) = copy(committerParty = committer)

    def withPersistence(enabled: Boolean) = copy(persistenceEnabled = enabled)

    def withHeartBeatInterval(interval: FiniteDuration) = copy(heartBeatInterval = interval)

    def withMaxNumberOfAcsContracts(cap: Int) = copy(maxNumberOfAcsContracts = Some(cap))

    def withCommandConfiguration(cc: CommandConfiguration) = copy(commandConfiguration = cc)

    def withHost(host: String) = copy(host = Some(host))
    def withPort(port: Int) = copy(port = Some(port))
    def withTlsConfig(tlsConfig: TlsConfiguration) = copy(tlsConfig = Some(tlsConfig))
    def withTlsConfigOption(tlsConfig: Option[TlsConfiguration]) = copy(tlsConfig = tlsConfig)
  }

  object Config {
    val defaultLedgerId = "ledger server"

    val defaultDarFile = new File("ledger/sandbox/Test.dar")

    val defaultParties = NonEmptyList("party", "Alice", "Bob")
    val defaultTimeProviderType = TimeProviderType.Static

    def defaultWithLedgerId(ledgerId: String): Config =
      default.withLedgerId(ledgerId)

    def defaultWithTimeProvider(timeProviderType: TimeProviderType) =
      default.withTimeProvider(timeProviderType)

    def default: Config = {
      val darFiles = List(defaultDarFile)
      new Config(
        RequestedLedgerAPIMode.Static(defaultLedgerId),
        darFiles.map(_.toPath),
        defaultParties,
        "committer",
        defaultTimeProviderType,
        TimeModel.reasonableDefault
      )
    }
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
        ledgerIdMode = config.ledgerId match {
          case RequestedLedgerAPIMode.Static(id) =>
            LedgerIdMode.Predefined(id)
          case RequestedLedgerAPIMode.Random() =>
            LedgerIdMode.Random
          case RequestedLedgerAPIMode.Dynamic() =>
            LedgerIdMode.Random
        },
        jdbcUrl = jdbcUrl
      )
    )
  }
}

sealed abstract class RequestedLedgerAPIMode extends Product with Serializable

object RequestedLedgerAPIMode {
  final case class Static(ledgerId: String) extends RequestedLedgerAPIMode
  final case class Random() extends RequestedLedgerAPIMode
  final case class Dynamic() extends RequestedLedgerAPIMode
}
