// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.cli

import java.io.File
import java.time.Duration

import ch.qos.logback.classic.Level
import com.auth0.jwt.algorithms.Algorithm
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.jwt.{ECDSAVerifier, HMAC256Verifier, JwksVerifier, RSA256Verifier}
import com.digitalasset.ledger.api.auth.AuthServiceJWT
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.tls.TlsConfiguration
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.configuration.BuildInfo
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.services.time.TimeProviderType
import scopt.{OptionParser, Read}

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

  private val KnownLogLevels = Set("ERROR", "WARN", "INFO", "DEBUG", "TRACE")

  // format: off
  private val cmdArgParser: OptionParser[SandboxConfig] = new OptionParser[SandboxConfig](
    "sandbox") {
    head(s"Sandbox version ${BuildInfo.Version}")

    arg[File]("<archive>...")
      .optional()
      .unbounded()
      .validate(f => Either.cond(checkIfZip(f), (), s"Invalid dar file: ${f.getName}"))
      .action((f, c) => c.copy(damlPackages = f :: c.damlPackages))
      .text("DAML archives to load in .dar format. Only DAML-LF v1 Archives are currently supported. Can be mixed in with optional arguments.")

    opt[Int]('p', "port")
      .action((x, c) => c.copy(port = x))
      .text(s"Sandbox service port. Defaults to ${SandboxConfig.DefaultPort}.")

    opt[File]("port-file")
      .optional()
      .action((f, c) => c.copy(portFile = Some(f.toPath)))
      .text("File to write the allocated port number to. Used to inform clients in CI about the allocated port.")

    opt[String]('a', "address")
      .action((x, c) => c.copy(address = Some(x)))
      .text("Sandbox service host. Defaults to binding on localhost.")

    // TODO remove in next major release.
    opt[Unit]("dalf")
      .optional()
      .text(
        "This argument is present for backwards compatibility. DALF and DAR archives are now identified by their extensions.")

    opt[Unit]('s', "static-time")
      .action { (_, c) => setTimeProviderType(c, TimeProviderType.Static) }
      .text("Use static time, configured with TimeService through gRPC.")

    opt[Unit]('w', "wall-clock-time")
      .action { (_, c) => setTimeProviderType(c, TimeProviderType.WallClock) }
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

    opt[String]("pem")
      .optional()
      .text("TLS: The pem file to be used as the private key.")
      .action((path, config) =>
        config.copy(tlsConfig =
          config.tlsConfig
            .fold(Some(TlsConfiguration(enabled = true, None, Some(new File(path)), None)))(c =>
            Some(c.copy(keyFile = Some(new File(path)))))))

    opt[String]("crt")
      .optional()
      .text("TLS: The crt file to be used as the cert chain. Required if any other TLS parameters are set.")
      .action((path: String, config: SandboxConfig) =>
        config.copy(
          tlsConfig =
            config.tlsConfig
              .fold(Some(TlsConfiguration(enabled = true, Some(new File(path)), None, None)))(c =>
              Some(c.copy(keyCertChainFile = Some(new File(path)))))))

    opt[String]("cacrt")
      .optional()
      .text("TLS: The crt file to be used as the the trusted root CA.")
      .action((path, config) =>
        config.copy(tlsConfig =
          config.tlsConfig
            .fold(Some(TlsConfiguration(enabled = true, None, None, Some(new File(path)))))(c =>
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

    opt[String]("log-level")
      .optional()
      .validate(l => Either
        .cond(KnownLogLevels.contains(l.toUpperCase), (), s"Unrecognized logging level $l"))
      .action((level, c) => c.copy(logLevel = Level.toLevel(level.toUpperCase)))
      .text("Default logging level to use. Available values are INFO, TRACE, DEBUG, WARN, and ERROR. Defaults to INFO.")

    opt[Unit]("eager-package-loading")
        .optional()
        .text("Whether to load all the packages in the .dar files provided eagerly, rather than when needed as the commands come.")
        .action( (_, config) => config.copy(eagerPackageLoading = true))

    opt[Long]("max-ttl-seconds")
        .optional()
        .validate(v => Either.cond(v > 0, (), "Max TTL must be a positive number"))
        .text("The maximum TTL allowed for commands in seconds")
        .action( (maxTtl, config) => config.copy(timeModel = config.timeModel.copy(maxTtl = Duration.ofSeconds(maxTtl))))

    opt[String]("auth-jwt-hs256-unsafe")
      .optional()
      .hidden()
      .validate(v => Either.cond(v.length > 0, (), "HMAC secret must be a non-empty string"))
      .text("[UNSAFE] Enables JWT-based authorization with shared secret HMAC256 signing: USE THIS EXCLUSIVELY FOR TESTING")
      .action( (secret, config) => config.copy(authService = Some(AuthServiceJWT(HMAC256Verifier(secret).valueOr(err => sys.error(s"Failed to create HMAC256 verifier: $err"))))))

    opt[String]("auth-jwt-rs256-crt")
      .optional()
      .validate(v => Either.cond(v.length > 0, (), "Certificate file path must be a non-empty string"))
      .text("Enables JWT-based authorization, where the JWT is signed by RSA256 with a public key loaded from the given X509 certificate file (.crt)")
      .action( (path, config) => config.copy(authService = Some(AuthServiceJWT(RSA256Verifier.fromCrtFile(path).valueOr(err => sys.error(s"Failed to create RSA256 verifier: $err"))))))

    opt[String]("auth-jwt-ec256-crt")
      .optional()
      .validate(v => Either.cond(v.length > 0, (), "Certificate file path must be a non-empty string"))
      .text("Enables JWT-based authorization, where the JWT is signed by ECDSA256 with a public key loaded from the given X509 certificate file (.crt)")
      .action( (path, config) => config.copy(
        authService = Some(AuthServiceJWT(
          ECDSAVerifier.fromCrtFile(path, Algorithm.ECDSA256(_, null)).valueOr(err => sys.error(s"Failed to create ECDSA256 verifier: $err"))))))

    opt[String]("auth-jwt-ec512-crt")
      .optional()
      .validate(v => Either.cond(v.length > 0, (), "Certificate file path must be a non-empty string"))
      .text("Enables JWT-based authorization, where the JWT is signed by ECDSA512 with a public key loaded from the given X509 certificate file (.crt)")
      .action( (path, config) => config.copy(
        authService = Some(AuthServiceJWT(
          ECDSAVerifier.fromCrtFile(path, Algorithm.ECDSA512(_, null)).valueOr(err => sys.error(s"Failed to create ECDSA512 verifier: $err"))))))


    opt[String]("auth-jwt-rs256-jwks")
      .optional()
      .validate(v => Either.cond(v.length > 0, (), "JWK server URL must be a non-empty string"))
      .text("Enables JWT-based authorization, where the JWT is signed by RSA256 with a public key loaded from the given JWKS URL")
      .action( (url, config) => config.copy(authService = Some(AuthServiceJWT(JwksVerifier(url)))))

    opt[Unit]("sortable-contract-ids")
      .hidden()
      .optional()
      .text("(Experimental) use new sortable contract ids")
      .action( (_, config) => config.copy(useSortableCid = true))

    help("help").text("Print the usage text")

    checkConfig(c => {
      if (c.scenario.isDefined && c.timeProviderType.contains(TimeProviderType.WallClock))
        failure(
          "Wall-clock time mode (`-w`/`--wall-clock-time`) and scenario initialization (`--scenario`) may not be used together.")
      else success
    })

    private def setTimeProviderType(
      config: SandboxConfig,
      timeProviderType: TimeProviderType,
    ): SandboxConfig = {
      if (config.timeProviderType.exists(_ != timeProviderType)) {
        throw new IllegalStateException(
          "Static time mode (`-s`/`--static-time`) and wall-clock time mode (`-w`/`--wall-clock-time`) are mutually exclusive. The time mode must be unambiguous.")
      }
      config.copy(timeProviderType = Some(timeProviderType))
    }
  }
  // format: on

  private def checkIfZip(f: File): Boolean = {
    import java.io.RandomAccessFile
    Try {
      val raf = new RandomAccessFile(f, "r")
      val n = raf.readInt
      raf.close()
      n == 0x504B0304 //non-empty, non-spanned ZIPs are always beginning with this
    }.getOrElse(false)
  }

  def parse(args: Array[String]): Option[SandboxConfig] =
    cmdArgParser.parse(args, SandboxConfig.default)
}
