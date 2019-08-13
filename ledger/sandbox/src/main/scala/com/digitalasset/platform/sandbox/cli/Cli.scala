// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.cli

import java.io.File
import java.time.Duration

import ch.qos.logback.classic.Level
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.tls.TlsConfiguration
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.sandbox.BuildInfo
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.services.time.TimeProviderType
import scopt.Read
import com.digitalasset.ledger.api.domain.LedgerId

import scala.util.Try

// NOTE:
// The config object should not expose Options for mandatory fields as such
// validations should not leave this class. Due to limitations of SCOPT as far I
// see we either use nulls or use the mutable builder instead.
object Cli {

  private implicit val durationRead: Read[Duration] = new Read[Duration] {
    override def arity: Int = 1

    override val reads: String => Duration = Duration.parse
  }

  // format: off
  private val cmdArgParser = new scopt.OptionParser[SandboxConfig]("sandbox") {
    head(s"Sandbox version ${BuildInfo.Version}")

    opt[Int]('p', "port")
      .action((x, c) => c.copy(port = x))
      .text(s"Sandbox service port. Defaults to ${SandboxConfig.DefaultPort}.")

    opt[File]("port-file")
      .optional()
      .action((f, c) => c.copy(portFile = Some(f)))
      .text("File to write the allocated port number to. Used to inform clients in CI about the allocated port.")

    opt[String]('a', "address")
      .action((x, c) => c.copy(address = Some(x)))
      .text("Sandbox service host. Defaults to binding on all addresses.")

    // TODO remove in next major release.
    opt[Unit]("dalf")
      .optional()
      .text(
        "This argument is present for backwards compatibility. DALF and DAR archives are now identified by their extensions.")

    opt[Unit]('s', "static-time")
      .action { (_, c) =>
        assertTimeModeIsDefault(c)
        c.copy(timeProviderType = TimeProviderType.Static)
      }
      .text("Use static time, configured with TimeService through gRPC.")

    opt[Unit]('w', "wall-clock-time")
      .action { (t, c) =>
        assertTimeModeIsDefault(c)
        c.copy(timeProviderType = TimeProviderType.WallClock)
      }
      .text("Use wall clock time (UTC). When not provided, static time is used.")

    // TODO(#577): Remove this flag.
    opt[Unit]("no-parity")
      .action { (_, config) =>
        config
      }
      .text("Legacy flag with no effect.")

    opt[String](name = "scenario")
      .action((x, c) => c.copy(scenario = Some(x)))
      .text(
        "If set, the sandbox will execute the given scenario on startup and store all the contracts created by it. " +
          "Note that when using --postgres-backend the scenario will be ran only if starting from a fresh database, _not_ when resuming from an existing one. " +
          "Two identifier formats are supported: Module.Name:Entity.Name (preferred) and Module.Name.Entity.Name (deprecated, will print a warning when used)." +
          "Also note that instructing the sandbox to load a scenario will have the side effect of loading _all_ the .dar files provided eagerly (see --eager-package-loading).")

    arg[File]("<archive>...")
      .optional()
      .unbounded()
      .validate(f => Either.cond(checkIfZip(f), (), s"Invalid dar file: ${f.getName}"))
      .action((f, c) => c.copy(damlPackages = f :: c.damlPackages))
      .text("DAML archives to load in .dar format. Only DAML-LF v1 Archives are currently supported.")

    opt[String]("pem")
      .optional()
      .text("TLS: The pem file to be used as the private key.")
      .action((path, config) =>
        config.copy(tlsConfig =
          config.tlsConfig.fold(Some(TlsConfiguration(true, None, Some(new File(path)), None)))(c =>
            Some(c.copy(keyFile = Some(new File(path)))))))

    opt[String]("crt")
      .optional()
      .text("TLS: The crt file to be used as the cert chain. Required if any other TLS parameters are set.")
      .action((path: String, config: SandboxConfig) =>
        config.copy(
          tlsConfig =
            config.tlsConfig.fold(Some(TlsConfiguration(true, Some(new File(path)), None, None)))(c =>
              Some(c.copy(keyCertChainFile = Some(new File(path)))))))

    opt[String]("cacrt")
      .optional()
      .text("TLS: The crt file to be used as the the trusted root CA.")
      .action((path, config) =>
        config.copy(tlsConfig =
          config.tlsConfig.fold(Some(TlsConfiguration(true, None, None, Some(new File(path)))))(c =>
            Some(c.copy(trustCertCollectionFile = Some(new File(path)))))))

    opt[Int]("maxInboundMessageSize")
      .action((x, c) => c.copy(maxInboundMessageSize = x))
      .text(s"Max inbound message size in bytes. Defaults to ${SandboxConfig.DefaultMaxInboundMessageSize}.")

    opt[String]("jdbcurl")
      .optional()
      .text("This flag is deprecated -- please use --sql-backend-jdbcurl.")
      .action((url, config) => config.copy(jdbcUrl = Some(url)))

    opt[String]("sql-backend-jdbcurl")
      .optional()
      .text("The JDBC connection URL to a Postgres database containing the username and password as well. If present, the Sandbox will use the database to persist its data.")
      .action((url, config) => config.copy(jdbcUrl = Some(url)))

    //TODO (robert): Think about all implications of allowing users to set the ledger ID.
    opt[String]("ledgerid")
      .optional()
        .action((id, c) => c.copy(ledgerIdMode = LedgerIdMode.Static(LedgerId(Ref.LedgerString.assertFromString(id)))))
      .text("Sandbox ledger ID. If missing, a random unique ledger ID will be used. Only useful with persistent stores.")

    val knownLevels = Set("ERROR", "WARN", "INFO", "DEBUG", "TRACE")

    opt[String]("log-level")
      .optional()
      .validate(l => Either.cond(knownLevels.contains(l.toUpperCase), (), s"Unrecognized logging level $l"))
      .action((level, c) => c.copy(logLevel = Level.toLevel(level.toUpperCase)))
      .text("Default logging level to use. Available values are INFO, TRACE, DEBUG, WARN, and ERROR. Defaults to INFO.")

    opt[Unit]("eager-package-loading")
        .optional()
        .text("Whether to load all the packages in the .dar files provided eagerly, rather than when needed as the commands come.")
        .action( (_, config) => config.copy(eagerPackageLoading = true))

    help("help").text("Print the usage text")

    checkConfig(c => {
      if (c.scenario.isDefined && c.timeProviderType == TimeProviderType.WallClock)
        failure("Wallclock mode (-w / --wall-clock-time) and scenario initialisation (--scenario) may not be used together.")
      else success
    })
  }
  // format: on
  private def assertTimeModeIsDefault(c: SandboxConfig): Unit = {
    if (c.timeProviderType != TimeProviderType.default)
      throw new IllegalArgumentException(
        "Error: -w and -o options may not be used together (time mode must be unambiguous).")
  }

  private def checkIfZip(f: File): Boolean = {
    import java.io.RandomAccessFile
    Try {
      val raf = new RandomAccessFile(f, "r")
      val n = raf.readInt
      raf.close()
      (n == 0x504B0304) //non-empty, non-spanned ZIPs are always beginning with this
    }.getOrElse(false)
  }

  def parse(args: Array[String]): Option[SandboxConfig] =
    cmdArgParser.parse(args, SandboxConfig.default)
}
