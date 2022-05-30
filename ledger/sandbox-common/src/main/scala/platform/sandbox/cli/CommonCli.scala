// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.cli

import java.io.File
import java.time.Duration
import com.daml.buildinfo.BuildInfo
import com.daml.jwt.JwtVerifierConfigurationCli
import com.daml.ledger.api.auth.AuthServiceJWT
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.tls.TlsVersion.TlsVersion
import com.daml.ledger.api.tls.{SecretsUrl, TlsConfiguration}
import com.daml.lf.data.Ref
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.configuration.CommandConfiguration
import com.daml.platform.configuration.Readers._
import com.daml.platform.sandbox.cli.CommonCli._
import com.daml.platform.sandbox.config.{LedgerName, SandboxConfig}
import com.daml.platform.services.time.TimeProviderType
import com.daml.platform.usermanagement.UserManagementConfig
import com.daml.ports.Port
import io.netty.handler.ssl.ClientAuth
import scalaz.syntax.tag._
import scopt.OptionParser

import scala.util.Try

// [[SandboxConfig]] should not expose Options for mandatory fields as such validations should not
// leave this class. Due to the limitations of scopt, we either use nulls or use the mutable builder
// instead.
class CommonCli(name: LedgerName) {

  val parser: OptionParser[SandboxConfig] =
    new OptionParser[SandboxConfig](name.unwrap.toLowerCase()) {
      head(s"$name version ${BuildInfo.Version}")

      opt[Unit]("dev-mode-unsafe")
        .optional()
        .action((_, config) => config.copy(engineMode = SandboxConfig.EngineMode.Dev))
        .text(
          "Turns on development mode. Development mode allows development versions of Daml-LF language."
        )
        .hidden()

      arg[File]("<archive>...")
        .optional()
        .unbounded()
        .validate(f => Either.cond(checkIfZip(f), (), s"Invalid dar file: ${f.getName}"))
        .action((f, c) => c.copy(damlPackages = f :: c.damlPackages))
        .text(
          "Daml archives to load in .dar format. Only Daml-LF v1 Archives are currently supported. Can be mixed in with optional arguments."
        )

      opt[String]('a', "address")
        .optional()
        .action((x, c) => c.copy(address = Some(x)))
        .text(s"Service host. Defaults to binding on localhost.")

      opt[Int]('p', "port")
        .optional()
        .action((x, c) => c.copy(port = Port(x)))
        .validate(x => Port.validate(x).toEither.left.map(_.getMessage))
        .text(s"Service port. Defaults to ${SandboxConfig.DefaultPort}.")

      opt[File]("port-file")
        .optional()
        .action((f, c) => c.copy(portFile = Some(f.toPath)))
        .text(
          "File to write the allocated port number to. Used to inform clients in CI about the allocated port."
        )

      opt[String]("ledgerid")
        .optional()
        .action((id, c) =>
          c.copy(
            ledgerIdMode = LedgerIdMode.Static(LedgerId(Ref.LedgerString.assertFromString(id)))
          )
        )
        .text(s"Ledger ID. If missing, a random unique ledger ID will be used.")

      opt[String]("participant-id")
        .optional()
        .action((id, c) => c.copy(participantId = Ref.ParticipantId.assertFromString(id)))
        .text(s"Participant ID. Defaults to '${SandboxConfig.DefaultParticipantId}'.")

      opt[Unit]('s', "static-time")
        .optional()
        .action((_, c) => setTimeProviderType(c, TimeProviderType.Static))
        .text("Use static time. When not specified, wall-clock-time is used.")

      opt[Unit]('w', "wall-clock-time")
        .optional()
        .action((_, c) => setTimeProviderType(c, TimeProviderType.WallClock))
        .text("Use wall clock time (UTC). This is the default.")

      // TODO(#577): Remove this flag.
      opt[Unit]("no-parity")
        .optional()
        .action { (_, config) =>
          config
        }
        .text("Legacy flag with no effect.")

      opt[String]("pem")
        .optional()
        .text("TLS: The pem file to be used as the private key.")
        .action((path, config) =>
          config.copy(tlsConfig =
            config.tlsConfig
              .fold(Some(TlsConfiguration(enabled = true, None, Some(new File(path)), None)))(c =>
                Some(c.copy(keyFile = Some(new File(path))))
              )
          )
        )

      opt[String]("tls-secrets-url")
        .optional()
        .text(
          "TLS: URL of a secrets service that provides parameters needed to decrypt the private key. Required when private key is encrypted (indicated by '.enc' filename suffix)."
        )
        .action((url, config) =>
          config.withTlsConfig(c => c.copy(secretsUrl = Some(SecretsUrl.fromString(url))))
        )

      checkConfig(c =>
        c.tlsConfig.fold(success) { tlsConfig =>
          if (
            tlsConfig.keyFile.isDefined
            && tlsConfig.keyFile.get.getName.endsWith(".enc")
            && tlsConfig.secretsUrl.isEmpty
          ) {
            failure(
              "You need to provide a secrets server URL if the server's private key is an encrypted file."
            )
          } else {
            success
          }
        }
      )

      opt[String]("crt")
        .optional()
        .text(
          "TLS: The crt file to be used as the cert chain. Required if any other TLS parameters are set."
        )
        .action((path: String, config: SandboxConfig) =>
          config.copy(tlsConfig =
            config.tlsConfig
              .fold(Some(TlsConfiguration(enabled = true, Some(new File(path)), None, None)))(c =>
                Some(c.copy(keyCertChainFile = Some(new File(path))))
              )
          )
        )

      opt[String]("cacrt")
        .optional()
        .text("TLS: The crt file to be used as the trusted root CA.")
        .action((path, config) =>
          config.copy(tlsConfig =
            config.tlsConfig
              .fold(Some(TlsConfiguration(enabled = true, None, None, Some(new File(path)))))(c =>
                Some(c.copy(trustCertCollectionFile = Some(new File(path))))
              )
          )
        )

      opt[ClientAuth]("client-auth")
        .optional()
        .text(
          "TLS: The client authentication mode. Must be one of none, optional or require. Defaults to required."
        )
        .action((clientAuth, config) =>
          config.copy(tlsConfig =
            config.tlsConfig
              .fold(
                Some(
                  TlsConfiguration(
                    enabled = true,
                    keyCertChainFile = None,
                    keyFile = None,
                    trustCertCollectionFile = None,
                    clientAuth = clientAuth,
                  )
                )
              )(c => Some(c.copy(clientAuth = clientAuth)))
          )
        )

      opt[TlsVersion]("min-tls-version")
        .optional()
        .text(
          "TLS: Indicates the minimum TLS version to enable. If specified must be either '1.2' or '1.3'."
        )
        .action((tlsVersion, config) =>
          config.withTlsConfig(c => c.copy(minimumServerProtocolVersion = Some(tlsVersion)))
        )

      opt[Boolean]("cert-revocation-checking")
        .optional()
        .text(
          "TLS: enable/disable certificate revocation checks with the OCSP. Disabled by default."
        )
        .action((checksEnabled, config) =>
          config.copy(
            tlsConfig = config.tlsConfig
              .fold(
                Some(TlsConfiguration.Empty.copy(enableCertRevocationChecking = checksEnabled))
              )(config => Some(config.copy(enableCertRevocationChecking = checksEnabled)))
          )
        )

      opt[Int]("max-inbound-message-size")
        .optional()
        .action((x, c) => c.copy(maxInboundMessageSize = x))
        .text(
          s"Max inbound message size in bytes. Defaults to ${SandboxConfig.DefaultMaxInboundMessageSize}."
        )

      opt[Int]("maxInboundMessageSize")
        .optional()
        .action((x, c) => c.copy(maxInboundMessageSize = x))
        .text("This flag is deprecated -- please use --max-inbound-message-size.")

      opt[String]("jdbcurl")
        .optional()
        .text("This flag is deprecated -- please use --sql-backend-jdbcurl.")
        .action((url, config) => config.copy(jdbcUrl = Some(url)))

      com.daml.cliopts.Logging.logLevelParse(this)((f, c) => c.copy(logLevel = f(c.logLevel)))

      JwtVerifierConfigurationCli.parse(this)((v, c) =>
        c.copy(authService = Some(AuthServiceJWT(v)))
      )

      opt[Int]("events-page-size")
        .optional()
        .text(
          s"Number of events fetched from the index for every round trip when serving streaming calls. Default is ${SandboxConfig.DefaultEventsPageSize}."
        )
        .action((eventsPageSize, config) => config.copy(eventsPageSize = eventsPageSize))

      opt[Int]("buffers-prefetching-parallelism")
        .optional()
        .text(
          s"Number of events fetched/decoded in parallel for populating the Ledger API internal buffers. Default is ${SandboxConfig.DefaultEventsProcessingParallelism}."
        )
        .action((eventsProcessingParallelism, config) =>
          config.copy(eventsProcessingParallelism = eventsProcessingParallelism)
        )

      opt[Int]("acs-id-page-size")
        .optional()
        .text(
          s"Number of contract ids fetched from the index for every round trip when serving ACS calls. Default is ${SandboxConfig.DefaultAcsIdPageSize}."
        )
        .action((acsIdPageSize, config) => config.copy(acsIdPageSize = acsIdPageSize))

      opt[Int]("acs-id-fetching-parallelism")
        .optional()
        .text(
          s"Number of contract id pages fetched in parallel when serving ACS calls. Default is ${SandboxConfig.DefaultAcsIdFetchingParallelism}."
        )
        .action((acsIdFetchingParallelism, config) =>
          config.copy(acsIdFetchingParallelism = acsIdFetchingParallelism)
        )

      opt[Int]("acs-contract-fetching-parallelism")
        .optional()
        .text(
          s"Number of event pages fetched in parallel when serving ACS calls. Default is ${SandboxConfig.DefaultAcsContractFetchingParallelism}."
        )
        .action((acsContractFetchingParallelism, config) =>
          config.copy(acsContractFetchingParallelism = acsContractFetchingParallelism)
        )

      opt[Int]("acs-global-parallelism-limit")
        .optional()
        .text(
          s"Maximum number of concurrent ACS queries to the index database. Default is ${SandboxConfig.DefaultAcsGlobalParallelism}."
        )
        .action((acsContractFetchingParallelism, config) =>
          config.copy(acsContractFetchingParallelism = acsContractFetchingParallelism)
        )

      opt[Int]("max-commands-in-flight")
        .optional()
        .action((value, config) =>
          config.copy(commandConfig = config.commandConfig.copy(maxCommandsInFlight = value))
        )
        .text(
          s"Maximum number of submitted commands for which the CommandService is waiting to be completed in parallel, for each distinct set of parties, as specified by the `act_as` property of the command. Reaching this limit will cause new submissions to wait in the queue before being submitted. Default is ${CommandConfiguration.Default.maxCommandsInFlight}."
        )

      opt[Int]("input-buffer-size")
        .optional()
        .action((value, config) =>
          config.copy(commandConfig = config.commandConfig.copy(inputBufferSize = value))
        )
        .text(
          s"Maximum number of commands waiting to be submitted for each distinct set of parties, as specified by the `act_as` property of the command. Reaching this limit will cause the server to signal backpressure using the ``RESOURCE_EXHAUSTED`` gRPC status code. Default is ${CommandConfiguration.Default.inputBufferSize}."
        )

      opt[Long]("max-lf-value-translation-cache-entries")
        .optional()
        .text(
          s"The maximum size of the cache used to deserialize Daml-LF values, in number of allowed entries. By default, nothing is cached."
        )
        .action((maximumLfValueTranslationCacheEntries, config) =>
          config.copy(
            lfValueTranslationEventCacheConfiguration =
              config.lfValueTranslationEventCacheConfiguration
                .copy(maximumSize = maximumLfValueTranslationCacheEntries),
            lfValueTranslationContractCacheConfiguration =
              config.lfValueTranslationContractCacheConfiguration
                .copy(maximumSize = maximumLfValueTranslationCacheEntries),
          )
        )

      opt[Boolean]("stack-traces")
        .hidden()
        .optional()
        .action((enabled, config) => config.copy(stackTraces = enabled))
        .text("Enable/disable stack traces. Default is to enable them.")

      opt[Long]("max-ledger-time-skew")
        .optional()
        .action { (value, config) =>
          val duration = Duration.ofSeconds(value)
          config.copy(timeModel = config.timeModel.copy(minSkew = duration, maxSkew = duration))
        }
        .text(
          s"Maximum skew (in seconds) between the ledger time and the record time. Default is ${SandboxConfig.defaultConfig.timeModel.maxSkew.getSeconds}."
        )

      opt[Duration]("management-service-timeout")
        .hidden()
        .optional()
        .action((value, config) => config.copy(managementServiceTimeout = value))
        .text(
          s"The timeout used for requests by management services of the Ledger API. The default is set to ${SandboxConfig.DefaultManagementServiceTimeout.getSeconds} seconds."
        )

      // TODO append-only: cleanup
      opt[Unit]("enable-append-only-schema")
        .optional()
        .action((_, config) => config)
        .text(
          "Deprecated parameter. The append-only index database with parallel ingestion is now always enabled."
        )

      // TODO append-only: cleanup
      opt[Unit]("enable-compression")
        .optional()
        .action((_, config) => config.copy(enableCompression = true))
        .text(
          s"Enables application-side compression of Daml-LF values stored in the database using the append-only schema." +
            " By default, compression is disabled."
        )

      opt[Duration]("max-deduplication-duration")
        .optional()
        .hidden()
        .action((maxDeduplicationDuration, config) =>
          config
            .copy(maxDeduplicationDuration = Some(maxDeduplicationDuration))
        )
        .text(
          "Maximum command deduplication duration."
        )

      opt[Boolean]("enable-user-management")
        .optional()
        .text(
          "Whether to enable participant user management."
        )
        .action((enabled, config: SandboxConfig) =>
          config.withUserManagementConfig(_.copy(enabled = enabled))
        )

      opt[Int]("user-management-cache-expiry")
        .optional()
        .text(
          s"Defaults to ${UserManagementConfig.DefaultCacheExpiryAfterWriteInSeconds} seconds. " +
            "Used to set expiry time for user management cache. " +
            "Also determines the maximum delay for propagating user management state changes which is double its value."
        )
        .action((value, config: SandboxConfig) =>
          config.withUserManagementConfig(_.copy(cacheExpiryAfterWriteInSeconds = value))
        )

      opt[Int]("user-management-max-cache-size")
        .optional()
        .text(
          s"Defaults to ${UserManagementConfig.DefaultMaxCacheSize} entries. " +
            "Determines the maximum in-memory cache size for user management state."
        )
        .action((value, config: SandboxConfig) =>
          config.withUserManagementConfig(_.copy(maxCacheSize = value))
        )

      opt[Int]("user-management-max-users-page-size")
        .optional()
        .text(
          s"Maximum number of users that the server can return in a single response. " +
            s"Defaults to ${UserManagementConfig.DefaultMaxUsersPageSize} entries."
        )
        .action((value, config: SandboxConfig) =>
          config.withUserManagementConfig(_.copy(maxUsersPageSize = value))
        )
      checkConfig(c => {
        val v = c.userManagementConfig.maxUsersPageSize
        if (v == 0 || v >= 100) {
          success
        } else {
          failure(
            s"user-management-max-users-page-size must be either 0 or greater than 99, was: $v"
          )
        }
      })

      com.daml.cliopts.Metrics.metricsReporterParse(this)(
        (setter, config) => config.copy(metricsReporter = setter(config.metricsReporter)),
        (setter, config) =>
          config.copy(metricsReportingInterval = setter(config.metricsReportingInterval)),
      )

      help("help").text("Print the usage text")
    }

  def withContractIdSeeding(
      defaultConfig: SandboxConfig,
      seedingModes: Seeding*
  ): CommonCli = {
    val seedingModesMap =
      seedingModes.map(mode => (mode.name, mode)).toMap
    val allSeedingModeNames = seedingModesMap.keys.mkString(", ")
    val defaultSeedingModeName = defaultConfig.seeding.name
    parser
      .opt[String]("contract-id-seeding")
      .optional()
      .text(
        s"""Set the seeding mode of contract IDs. Possible values are $allSeedingModeNames. "$defaultSeedingModeName" mode is not compatible with development mode. Default is "$defaultSeedingModeName"."""
      )
      .validate(v =>
        Either.cond(
          seedingModesMap.contains(v.toLowerCase),
          (),
          s"seeding mode must be one of $allSeedingModeNames",
        )
      )
      .action((text, config) => config.copy(seeding = seedingModesMap(text)))
    this
  }

  def withEarlyAccess: CommonCli = {
    parser
      .opt[Unit]("early-access-unsafe")
      .optional()
      .action((_, c) =>
        if (c.engineMode == SandboxConfig.EngineMode.Stable) {
          c.copy(engineMode = SandboxConfig.EngineMode.EarlyAccess)
        } else {
          c
        }
      )
      .text(
        "Enable preview version of the next Daml-LF language. Should not be used in production."
      )
    this
  }

  def withDevEngine: CommonCli = {
    parser
      .opt[Unit]("daml-lf-dev-mode-unsafe")
      .optional()
      .hidden()
      .action((_, c) =>
        if (c.engineMode == SandboxConfig.EngineMode.Stable) {
          c.copy(engineMode = SandboxConfig.EngineMode.Dev)
        } else {
          c
        }
      )
      .text(
        "Enable the development version of the Daml-LF language. Highly unstable. Should not be used in production."
      )
    this
  }

}

object CommonCli {

  private def setTimeProviderType(
      config: SandboxConfig,
      timeProviderType: TimeProviderType,
  ): SandboxConfig = {
    if (config.timeProviderType.exists(_ != timeProviderType)) {
      throw new IllegalStateException(
        "Static time mode (`-s`/`--static-time`) and wall-clock time mode (`-w`/`--wall-clock-time`) are mutually exclusive. The time mode must be unambiguous."
      )
    }
    config.copy(timeProviderType = Some(timeProviderType))
  }

  private def checkIfZip(f: File): Boolean = {
    import java.io.RandomAccessFile
    Try {
      val raf = new RandomAccessFile(f, "r")
      val n = raf.readInt
      raf.close()
      n == 0x504b0304 // non-empty, non-spanned ZIPs are always beginning with this
    }.getOrElse(false)
  }

}
