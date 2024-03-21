// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.oauth2

import org.apache.pekko.http.scaladsl.model.Uri
import com.daml.auth.middleware.oauth2.Config.{
  DefaultCookieSecure,
  DefaultHttpPort,
  DefaultLoginTimeout,
  DefaultMaxLoginRequests,
}
import com.daml.cliopts
import com.daml.jwt.{JwtVerifierBase, JwtVerifierConfigurationCli}
import com.daml.metrics.api.reporters.MetricsReporter
import com.typesafe.scalalogging.StrictLogging
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderFailures
import scopt.OptionParser

import java.io.File
import java.nio.file.{Path, Paths}

import scala.concurrent.duration
import scala.concurrent.duration.FiniteDuration
import scalaz.syntax.std.option._

private[oauth2] final case class Cli(
    configFile: Option[File] = None,
    // Host and port the middleware listens on
    address: String = cliopts.Http.defaultAddress,
    port: Int = DefaultHttpPort,
    portFile: Option[Path] = None,
    // The URI to which the OAuth2 server will redirect after a completed login flow.
    // Must map to the `/cb` endpoint of the auth middleware.
    callbackUri: Option[Uri] = None,
    maxLoginRequests: Int = DefaultMaxLoginRequests,
    loginTimeout: FiniteDuration = DefaultLoginTimeout,
    cookieSecure: Boolean = DefaultCookieSecure,
    // OAuth2 server endpoints
    oauthAuth: Uri,
    oauthToken: Uri,
    // OAuth2 server request templates
    oauthAuthTemplate: Option[Path],
    oauthTokenTemplate: Option[Path],
    oauthRefreshTemplate: Option[Path],
    // OAuth2 client properties
    clientId: String,
    clientSecret: SecretString,
    // Token verification
    tokenVerifier: JwtVerifierBase,
    metricsReporter: Option[MetricsReporter] = None,
    metricsReportingInterval: FiniteDuration = FiniteDuration(10, duration.SECONDS),
) extends StrictLogging {

  def loadFromConfigFile: Option[Either[ConfigReaderFailures, FileConfig]] = {
    configFile.map(cf => ConfigSource.file(cf).load[FileConfig])
  }

  def loadFromCliArgs: Config = {
    val cfg = Config(
      address,
      port,
      portFile,
      callbackUri,
      maxLoginRequests,
      loginTimeout,
      cookieSecure,
      oauthAuth,
      oauthToken,
      oauthAuthTemplate,
      oauthTokenTemplate,
      oauthRefreshTemplate,
      clientId,
      clientSecret,
      tokenVerifier,
      metricsReporter,
      metricsReportingInterval,
      Seq.empty,
    )
    cfg.validate()
    cfg
  }

  def loadConfig: Option[Config] = {
    loadFromConfigFile.cata(
      {
        case Right(cfg) => Some(cfg.toConfig())
        case Left(ex) =>
          logger.error(
            s"Error loading oauth2-middleware config from file $configFile",
            ex.prettyPrint(),
          )
          None
      }, {
        logger.warn("Using cli opts for running oauth2-middleware is deprecated")
        Some(loadFromCliArgs)
      },
    )
  }
}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
private[oauth2] object Cli {

  private[oauth2] val Default =
    Cli(
      configFile = None,
      address = cliopts.Http.defaultAddress,
      port = DefaultHttpPort,
      portFile = None,
      callbackUri = None,
      maxLoginRequests = DefaultMaxLoginRequests,
      loginTimeout = DefaultLoginTimeout,
      cookieSecure = DefaultCookieSecure,
      oauthAuth = null,
      oauthToken = null,
      oauthAuthTemplate = None,
      oauthTokenTemplate = None,
      oauthRefreshTemplate = None,
      clientId = null,
      clientSecret = null,
      tokenVerifier = null,
    )

  private val parser: OptionParser[Cli] = new scopt.OptionParser[Cli]("oauth-middleware") {
    help('h', "help").text("Print usage")
    opt[Option[File]]('c', "config")
      .text(
        "This is the recommended way to provide an app config file, the remaining cli-args are deprecated"
      )
      .valueName("<file>")
      .action((file, cli) => cli.copy(configFile = file))

    cliopts.Http.serverParse(this, serviceName = "OAuth2 Middleware")(
      address = (f, c) => c.copy(address = f(c.address)),
      httpPort = (f, c) => c.copy(port = f(c.port)),
      defaultHttpPort = Some(DefaultHttpPort),
      portFile = Some((f, c) => c.copy(portFile = f(c.portFile))),
    )

    opt[String]("callback")
      .action((x, c) => c.copy(callbackUri = Some(Uri(x))))
      .text(
        "URI to the auth middleware's callback endpoint `/cb`. By default constructed from the incoming login request."
      )

    opt[Int]("max-pending-login-requests")
      .action((x, c) => c.copy(maxLoginRequests = x))
      .text(
        "Maximum number of simultaneously pending login requests. Requests will be denied when exceeded until earlier requests have been completed or timed out."
      )

    opt[Boolean]("cookie-secure")
      .action((x, c) => c.copy(cookieSecure = x))
      .text(
        "Enable the Secure attribute on the cookie that stores the token. Defaults to true. Only disable this for testing and development purposes."
      )

    opt[Long]("login-request-timeout")
      .action((x, c) => c.copy(loginTimeout = FiniteDuration(x, duration.SECONDS)))
      .text(
        "Login request timeout. Requests will be evicted if the callback endpoint receives no corresponding request in time."
      )

    opt[String]("oauth-auth")
      .action((x, c) => c.copy(oauthAuth = Uri(x)))
      .text("URI of the OAuth2 authorization endpoint")

    opt[String]("oauth-token")
      .action((x, c) => c.copy(oauthToken = Uri(x)))
      .text("URI of the OAuth2 token endpoint")

    opt[String]("oauth-auth-template")
      .action((x, c) => c.copy(oauthAuthTemplate = Some(Paths.get(x))))
      .text("OAuth2 authorization request Jsonnet template")

    opt[String]("oauth-token-template")
      .action((x, c) => c.copy(oauthTokenTemplate = Some(Paths.get(x))))
      .text("OAuth2 token request Jsonnet template")

    opt[String]("oauth-refresh-template")
      .action((x, c) => c.copy(oauthRefreshTemplate = Some(Paths.get(x))))
      .text("OAuth2 refresh request Jsonnet template")

    opt[String]("id")
      .hidden()
      .action((x, c) => c.copy(clientId = x))
      .withFallback(() => sys.env.getOrElse("DAML_CLIENT_ID", ""))

    opt[String]("secret")
      .hidden()
      .action((x, c) => c.copy(clientSecret = SecretString(x)))
      .withFallback(() => sys.env.getOrElse("DAML_CLIENT_SECRET", ""))

    cliopts.Metrics.metricsReporterParse(this)(
      (f, c) => c.copy(metricsReporter = f(c.metricsReporter)),
      (f, c) => c.copy(metricsReportingInterval = f(c.metricsReportingInterval)),
    )

    JwtVerifierConfigurationCli.parse(this)((v, c) => c.copy(tokenVerifier = v))

    checkConfig { cfg =>
      if (cfg.configFile.isEmpty && cfg.tokenVerifier == null)
        Left("You must specify one of the --auth-jwt-* flags for token verification.")
      else
        Right(())
    }

    checkConfig { cfg =>
      if (cfg.configFile.isEmpty && (cfg.clientId.isEmpty || cfg.clientSecret.value.isEmpty))
        Left("Environment variable DAML_CLIENT_ID AND DAML_CLIENT_SECRET must not be empty")
      else
        Right(())
    }

    checkConfig { cfg =>
      if (cfg.configFile.isEmpty && (cfg.oauthAuth == null || cfg.oauthToken == null))
        Left("oauth-auth and oauth-token values must not be empty")
      else
        Right(())
    }

    checkConfig { cfg =>
      val cliOptionsAreDefined =
        cfg.oauthToken != null || cfg.oauthAuth != null || cfg.tokenVerifier != null
      if (cfg.configFile.isDefined && cliOptionsAreDefined) {
        Left("Found both config file and cli opts for the app, please provide only one of them")
      } else Right(())
    }

    override def showUsageOnError: Option[Boolean] = Some(true)
  }

  def parse(args: Array[String]): Option[Cli] = parser.parse(args, Default)

  def parseConfig(args: Array[String]): Option[Config] = {
    val cli = parse(args)
    cli.flatMap(_.loadConfig)
  }
}
