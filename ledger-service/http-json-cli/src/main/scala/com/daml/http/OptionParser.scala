// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.http.dbbackend.{JdbcConfig, DBConfig}
import com.daml.ledger.api.tls.TlsConfigurationCli
import com.typesafe.scalalogging.StrictLogging
import scopt.{Read, RenderingMode}

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Try

class OptionParser(getEnvVar: String => Option[String])(implicit
    supportedJdbcDriverNames: DBConfig.SupportedJdbcDriverNames
) extends scopt.OptionParser[Config]("http-json-binary")
    with StrictLogging {

  private def setJdbcConfig(
      config: Config,
      jdbcConfig: JdbcConfig,
  ): Config = {
    if (config.jdbcConfig.exists(_ != jdbcConfig)) {
      throw new IllegalStateException(
        "--query-store-jdbc-config and --query-store-jdbc-config-env are mutually exclusive."
      )
    }
    config.copy(jdbcConfig = Some(jdbcConfig))
  }

  private def parseJdbcConfig(s: String): Either[String, JdbcConfig] =
    for {
      m <- Try(implicitly[Read[Map[String, String]]].reads(s)).toEither.left.map(_ =>
        s"Failed to parse $s into a comma-separated key-value map"
      )
      conf <- JdbcConfig.create(m)
    } yield conf

  private def parseJdbcConfigEnvVar(
      envVar: String,
      getEnvVar: String => Option[String],
  ): Either[String, JdbcConfig] =
    for {
      v <- getEnvVar(envVar).toRight(s"Environment variable $envVar was not set")
      conf <- parseJdbcConfig(v)
    } yield conf

  override def renderingMode: RenderingMode = RenderingMode.OneColumn

  head("HTTP JSON API daemon")

  help("help").text("Print this usage text")

  opt[String]("ledger-host")
    .action((x, c) => c.copy(ledgerHost = x))
    .required()
    .text("Ledger host name or IP address")

  opt[Int]("ledger-port")
    .action((x, c) => c.copy(ledgerPort = x))
    .required()
    .text("Ledger port number")

  import com.daml.cliopts

  cliopts.Http.serverParse(this, serviceName = "HTTP JSON API")(
    address = (f, c) => c copy (address = f(c.address)),
    httpPort = (f, c) => c copy (httpPort = f(c.httpPort)),
    defaultHttpPort = None,
    portFile = Some((f, c) => c copy (portFile = f(c.portFile))),
  )

  opt[String]("application-id")
    .foreach(x =>
      logger.warn(
        s"Command-line option '--application-id' is deprecated. Please do NOT specify it. " +
          s"Application ID: '$x' provided in the command-line is NOT used, using Application ID from JWT."
      )
    )
    .optional()
    .hidden()

  TlsConfigurationCli.parse(this, colSpacer = "        ")((f, c) =>
    c copy (tlsConfig = f(c.tlsConfig))
  )

  opt[Duration]("package-reload-interval")
    .action((x, c) => c.copy(packageReloadInterval = FiniteDuration(x.length, x.unit)))
    .optional()
    .text(
      s"Optional interval to poll for package updates. Examples: 500ms, 5s, 10min, 1h, 1d. " +
        s"Defaults to ${Config.Empty.packageReloadInterval: FiniteDuration}"
    )

  opt[Int]("package-max-inbound-message-size")
    .action((x, c) => c.copy(packageMaxInboundMessageSize = Some(x)))
    .optional()
    .text(
      s"Optional max inbound message size in bytes used for uploading and downloading package updates." +
        s" Defaults to the `max-inbound-message-size` setting."
    )

  opt[Int]("max-inbound-message-size")
    .action((x, c) => c.copy(maxInboundMessageSize = x))
    .optional()
    .text(
      s"Optional max inbound message size in bytes. Defaults to ${Config.Empty.maxInboundMessageSize: Int}."
    )

  opt[JdbcConfig]("query-store-jdbc-config")
    .action((x, c) => setJdbcConfig(c, x))
    .optional()
    .valueName(JdbcConfig.usage)
    .text(
      s"Optional query store JDBC configuration string." +
        " Query store is a search index, use it if you need to query large active contract sets. " +
        JdbcConfig.help
    )

  opt[String]("query-store-jdbc-config-env")
    .action((env, c) =>
      setJdbcConfig(c, parseJdbcConfigEnvVar(env, getEnvVar).fold(sys.error(_), identity(_)))
    )
    .validate(parseJdbcConfigEnvVar(_, getEnvVar).map(_ => ()))
    .optional()
    .valueName("<ENVIRONMENT VARIABLE>")
    .text(
      s"Optional name of an environment variable containing the query store JDBC configuration string in the same format used by --query-store-jdbc-config." +
        " --query-store-jdbc-config-env and --query-store-jdbc-config are mutually exclusive."
    )

  opt[StaticContentConfig]("static-content")
    .action((x, c) => c.copy(staticContentConfig = Some(x)))
    .optional()
    .valueName(StaticContentConfig.usage)
    .text(
      s"DEV MODE ONLY (not recommended for production). Optional static content configuration string. "
        + StaticContentConfig.help
    )

  opt[Unit]("allow-insecure-tokens")
    .action((_, c) => c copy (allowNonHttps = true))
    .text(
      "DEV MODE ONLY (not recommended for production). Allow connections without a reverse proxy providing HTTPS."
    )

  opt[String]("access-token-file")
    .foreach(_ =>
      logger.warn(
        s"The '--access-token-file' command line option is deprecated and has no effect. The authorization for the retrieval of packages required to serve a request is performed with the JWT in the request itself."
      )
    )
    .text(
      s"provide the path from which the access token will be read, required to interact with an authenticated ledger, no default"
    )
    .optional()

  opt[WebsocketConfig]("websocket-config")
    .action((x, c) => c.copy(wsConfig = Some(x)))
    .optional()
    .valueName(WebsocketConfig.usage)
    .text(s"Optional websocket configuration string. ${WebsocketConfig.help}")

  cliopts.Logging.logLevelParse(this)((f, c) => c.copy(logLevel = f(c.logLevel)))
  cliopts.Logging.logEncoderParse(this)((f, c) => c.copy(logEncoder = f(c.logEncoder)))
  cliopts.Metrics.metricsReporterParse(this)(
    (f, c) => c.copy(metricsReporter = f(c.metricsReporter)),
    (f, c) => c.copy(metricsReportingInterval = f(c.metricsReportingInterval)),
  )

}
