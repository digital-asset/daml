// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import java.nio.file.{Path, Paths}
import java.time.Duration

import com.daml.platform.services.time.TimeProviderType

case class ServiceConfig(
    // For convenience, we allow passing in a DAR on startup
    // as opposed to uploading it dynamically.
    darPath: Option[Path],
    httpPort: Int,
    ledgerHost: String,
    ledgerPort: Int,
    timeProviderType: TimeProviderType,
    commandTtl: Duration,
    init: Boolean,
    jdbcConfig: Option[JdbcConfig] = None,
//    initConfig: InitConfig,
)

final case class JdbcConfig(
    driver: String,
    url: String,
    user: String,
    password: String,
//    createSchema: Boolean = false
)
//case class InitConfig(
//    operatorUsername: String,
//    operatorPassword: String,
//    serviceUsername: String,
//    servicePassword: String,
//    databaseName: String,
//)

object ServiceConfig {
  private val parser = new scopt.OptionParser[ServiceConfig]("trigger-service") {
    head("trigger-service")

    opt[String]("dar")
      .optional()
      .action((f, c) => c.copy(darPath = Some(Paths.get(f))))
      .text("Path to the dar file containing the trigger")

    opt[Int]("http-port")
      .optional()
      .action((t, c) => c.copy(httpPort = t))
      .text("Http port")

    opt[String]("ledger-host")
      .required()
      .action((t, c) => c.copy(ledgerHost = t))
      .text("Ledger hostname")

    opt[Int]("ledger-port")
      .required()
      .action((t, c) => c.copy(ledgerPort = t))
      .text("Ledger port")

    opt[Unit]('w', "wall-clock-time")
      .action { (t, c) =>
        c.copy(timeProviderType = TimeProviderType.WallClock)
      }
      .text("Use wall clock time (UTC). When not provided, static time is used.")

    opt[Long]("ttl")
      .action { (t, c) =>
        c.copy(commandTtl = Duration.ofSeconds(t))
      }
      .text("TTL in seconds used for commands emitted by the trigger. Defaults to 30s.")

    opt[String]("jdbcConfig")
        .action((t, c) => c.copy(jdbcConfig = t))
        .text("JDBC config string.")

    cmd("init")
      .action((_, c) => c.copy(init = true))
      .text("Initialize a PostgreSQL database for service recovery.")
  }
  def parse(args: Array[String]): Option[ServiceConfig] =
    parser.parse(
      args,
      ServiceConfig(
        darPath = None,
        httpPort = 8088,
        ledgerHost = null,
        ledgerPort = 0,
        timeProviderType = TimeProviderType.Static,
        commandTtl = Duration.ofSeconds(30L),
        init = false
      ),
    )
}
