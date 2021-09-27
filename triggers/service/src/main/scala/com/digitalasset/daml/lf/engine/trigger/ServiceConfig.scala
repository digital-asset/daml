// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import java.nio.file.{Path, Paths}
import java.time.Duration

import akka.http.scaladsl.model.Uri
import com.daml.cliopts
import com.daml.platform.services.time.TimeProviderType
import com.daml.auth.middleware.api.{Client => AuthClient}
import com.daml.dbutils.{DBConfig, JdbcConfig}

import scala.concurrent.duration
import scala.concurrent.duration.FiniteDuration

private[trigger] final case class ServiceConfig(
    // For convenience, we allow passing DARs on startup
    // as opposed to uploading them dynamically.
    darPaths: List[Path],
    address: String,
    httpPort: Int,
    ledgerHost: String,
    ledgerPort: Int,
    authInternalUri: Option[Uri],
    authExternalUri: Option[Uri],
    authBothUri: Option[Uri],
    authRedirectToLogin: AuthClient.RedirectToLogin,
    authCallbackUri: Option[Uri],
    maxInboundMessageSize: Int,
    minRestartInterval: FiniteDuration,
    maxRestartInterval: FiniteDuration,
    maxAuthCallbacks: Int,
    authCallbackTimeout: FiniteDuration,
    maxHttpEntityUploadSize: Long,
    httpEntityUploadTimeout: FiniteDuration,
    timeProviderType: TimeProviderType,
    commandTtl: Duration,
    init: Boolean,
    jdbcConfig: Option[JdbcConfig],
    portFile: Option[Path],
    allowExistingSchema: Boolean,
)

private[trigger] object ServiceConfig {
  private val DefaultHttpPort: Int = 8088
  val DefaultMaxInboundMessageSize: Int = RunnerConfig.DefaultMaxInboundMessageSize
  private val DefaultMinRestartInterval: FiniteDuration = FiniteDuration(5, duration.SECONDS)
  val DefaultMaxRestartInterval: FiniteDuration = FiniteDuration(60, duration.SECONDS)
  // Adds up to ~1GB with DefaultMaxInboundMessagesSize
  val DefaultMaxAuthCallbacks: Int = 250
  val DefaultAuthCallbackTimeout: FiniteDuration = FiniteDuration(1, duration.MINUTES)
  val DefaultMaxHttpEntityUploadSize: Long = RunnerConfig.DefaultMaxInboundMessageSize.toLong
  val DefaultHttpEntityUploadTimeout: FiniteDuration = FiniteDuration(1, duration.MINUTES)

  implicit val redirectToLoginRead: scopt.Read[AuthClient.RedirectToLogin] = scopt.Read.reads {
    _.toLowerCase match {
      case "yes" => AuthClient.RedirectToLogin.Yes
      case "no" => AuthClient.RedirectToLogin.No
      case "auto" => AuthClient.RedirectToLogin.Auto
      case s => throw new IllegalArgumentException(s"'$s' is not one of 'yes', 'no', or 'auto'.")
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements")) // scopt builders
  private class OptionParser(supportedJdbcDriverNames: Set[String])
      extends scopt.OptionParser[ServiceConfig]("trigger-service") {
    head("trigger-service")

    opt[String]("dar")
      .optional()
      .unbounded()
      .action((f, c) => c.copy(darPaths = Paths.get(f) :: c.darPaths))
      .text("Path to the dar file containing the trigger.")

    cliopts.Http.serverParse(this, serviceName = "Trigger")(
      address = (f, c) => c.copy(address = f(c.address)),
      httpPort = (f, c) => c.copy(httpPort = f(c.httpPort)),
      defaultHttpPort = Some(DefaultHttpPort),
      portFile = Some((f, c) => c.copy(portFile = f(c.portFile))),
    )

    opt[String]("ledger-host")
      .required()
      .action((t, c) => c.copy(ledgerHost = t))
      .text("Ledger hostname.")

    opt[Int]("ledger-port")
      .required()
      .action((t, c) => c.copy(ledgerPort = t))
      .text("Ledger port.")

    opt[String]("auth")
      .optional()
      .action((t, c) => c.copy(authBothUri = Some(Uri(t))))
      .text(
        "Sets both the internal and external auth URIs. Incompatible with --auth-internal and --auth-external."
      )

    opt[String]("auth-internal")
      .optional()
      .action((t, c) => c.copy(authInternalUri = Some(Uri(t))))
      .text(
        "Sets the internal auth URIs (used by the trigger service to connect directly to the middleware). Incompatible with --auth."
      )

    opt[String]("auth-external")
      .optional()
      .action((t, c) => c.copy(authExternalUri = Some(Uri(t))))
      .text(
        "Sets the external auth URI (the one returned to the browser). Incompatible with --auth."
      )

    opt[AuthClient.RedirectToLogin]("auth-redirect")
      .optional()
      .action((x, c) => c.copy(authRedirectToLogin = x))
      .text(
        "Redirect to auth middleware login endpoint when unauthorized. One of 'yes', 'no', or 'auto'."
      )

    opt[String]("auth-callback")
      .optional()
      .action((t, c) => c.copy(authCallbackUri = Some(Uri(t))))
      .text(
        "URI to the auth login flow callback endpoint `/cb`. By default constructed from the incoming login request."
      )

    opt[Int]("max-inbound-message-size")
      .action((x, c) => c.copy(maxInboundMessageSize = x))
      .optional()
      .text(
        s"Optional max inbound message size in bytes. Defaults to ${DefaultMaxInboundMessageSize}."
      )

    opt[Long]("min-restart-interval")
      .action((x, c) => c.copy(minRestartInterval = FiniteDuration(x, duration.SECONDS)))
      .optional()
      .text(
        s"Minimum time interval before restarting a failed trigger. Defaults to ${DefaultMinRestartInterval.toSeconds} seconds."
      )

    opt[Long]("max-restart-interval")
      .action((x, c) => c.copy(maxRestartInterval = FiniteDuration(x, duration.SECONDS)))
      .optional()
      .text(
        s"Maximum time interval between restarting a failed trigger. Defaults to ${DefaultMaxRestartInterval.toSeconds} seconds."
      )

    opt[Int]("max-pending-authorizations")
      .action((x, c) => c.copy(maxAuthCallbacks = x))
      .optional()
      .text(
        s"Optional max number of pending authorization requests. Defaults to ${DefaultMaxAuthCallbacks}."
      )

    opt[Long]("authorization-timeout")
      .action((x, c) => c.copy(authCallbackTimeout = FiniteDuration(x, duration.SECONDS)))
      .optional()
      .text(
        s"Optional authorization timeout. Defaults to ${DefaultAuthCallbackTimeout.toSeconds} seconds."
      )

    opt[Long]("max-http-entity-upload-size")
      .action((x, c) => c.copy(maxHttpEntityUploadSize = x))
      .optional()
      .text(s"Optional max HTTP entity upload size. Defaults to ${DefaultMaxHttpEntityUploadSize}.")

    opt[Long]("http-entity-upload-timeout")
      .action((x, c) => c.copy(httpEntityUploadTimeout = FiniteDuration(x, duration.SECONDS)))
      .optional()
      .text(
        s"Optional HTTP entity upload timeout. Defaults to ${DefaultHttpEntityUploadTimeout.toSeconds} seconds."
      )

    opt[Unit]('s', "static-time")
      .optional()
      .action((_, c) => c.copy(timeProviderType = TimeProviderType.Static))
      .text("Use static time. When not specified, wall-clock time is used.")

    opt[Unit]('w', "wall-clock-time")
      .optional()
      .text(
        "[DEPRECATED] Wall-clock time is the default. This flag has no effect. Use `-s` to enable static time."
      )

    opt[Long]("ttl")
      .action { (t, c) =>
        c.copy(commandTtl = Duration.ofSeconds(t))
      }
      .text("TTL in seconds used for commands emitted by the trigger. Defaults to 30s.")

    implicit val jcd: DBConfig.JdbcConfigDefaults = DBConfig.JdbcConfigDefaults(
      supportedJdbcDrivers = supportedJdbcDriverNames,
      defaultDriver = Some("org.postgresql.Driver"),
    )

    opt[Map[String, String]]("jdbc")
      .action { (x, c) =>
        c.copy(jdbcConfig =
          Some(
            JdbcConfig
              .create(x)
              .fold(e => throw new IllegalArgumentException(e), identity)
          )
        )
      }
      .optional()
      .text(JdbcConfig.help())
      .text(
        "JDBC configuration parameters. If omitted the service runs without a database. "
          + JdbcConfig.help()
      )

    opt[Boolean]("allow-existing-schema")
      .action((_, c) => c.copy(allowExistingSchema = true))
      .text(
        "Do not abort if there are existing tables in the database schema. EXPERT ONLY. Defaults to false."
      )

    checkConfig { cfg =>
      if (
        (cfg.authBothUri.nonEmpty && (cfg.authInternalUri.nonEmpty || cfg.authExternalUri.nonEmpty))
        || (cfg.authInternalUri.nonEmpty != cfg.authExternalUri.nonEmpty)
      )
        failure("You must specify either just --auth or both --auth-internal and --auth-external.")
      else
        success
    }

    cmd("init-db")
      .action((_, c) => c.copy(init = true))
      .text("Initialize database and terminate.")

    help("help").text("Print this usage text")

  }

  def parse(args: Array[String], supportedJdbcDriverNames: Set[String]): Option[ServiceConfig] =
    new OptionParser(supportedJdbcDriverNames).parse(
      args,
      ServiceConfig(
        darPaths = Nil,
        address = cliopts.Http.defaultAddress,
        httpPort = DefaultHttpPort,
        ledgerHost = null,
        ledgerPort = 0,
        authInternalUri = None,
        authExternalUri = None,
        authBothUri = None,
        authRedirectToLogin = AuthClient.RedirectToLogin.No,
        authCallbackUri = None,
        maxInboundMessageSize = DefaultMaxInboundMessageSize,
        minRestartInterval = DefaultMinRestartInterval,
        maxRestartInterval = DefaultMaxRestartInterval,
        maxAuthCallbacks = DefaultMaxAuthCallbacks,
        authCallbackTimeout = DefaultAuthCallbackTimeout,
        maxHttpEntityUploadSize = DefaultMaxHttpEntityUploadSize,
        httpEntityUploadTimeout = DefaultHttpEntityUploadTimeout,
        timeProviderType = TimeProviderType.WallClock,
        commandTtl = Duration.ofSeconds(30L),
        init = false,
        jdbcConfig = None,
        portFile = None,
        allowExistingSchema = false,
      ),
    )
}
