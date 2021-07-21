// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import cats.effect.{Blocker, ContextShift, IO}
import com.daml.http.JdbcConfig
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import doobie._

import java.io.Closeable
import java.util.concurrent.Executors.newWorkStealingPool
import javax.sql.DataSource
import scala.concurrent.ExecutionContext

object Connection {

  type T = Transactor.Aux[IO, Unit]

  def connect(cfg: JdbcConfig)(implicit
      cs: ContextShift[IO]
  ): T =
    Transactor
      .fromDriverManager[IO](cfg.driver, cfg.url, cfg.user, cfg.password)(
        IO.ioConcurrentEffect(cs),
        cs,
      )
}

object ConnectionPool {

  // below values are hardcoded for now, can be passed as props later.
  final val MinIdle = 8
  final val IdleTimeout = 10000
  final val PoolSize = 10

  type T = Transactor.Aux[IO, _ <: DataSource with Closeable]

  def connect(cfg: JdbcConfig, poolSize: Int)(implicit
      ec: ExecutionContext,
      cs: ContextShift[IO],
  ): (DataSource with Closeable, T) = {
    val ds = dataSource(cfg, poolSize)
    (
      ds,
      Transactor
        .fromDataSource[IO](
          ds,
          connectEC = ec,
          blocker = Blocker liftExecutorService newWorkStealingPool(poolSize),
        )(IO.ioConcurrentEffect(cs), cs),
    )
  }

  def dataSource(cfg: JdbcConfig, poolSize: Int) = {

    val c = new HikariConfig()
    c.setJdbcUrl(cfg.url)
    c.setUsername(cfg.user)
    c.setPassword(cfg.password)
    c.setMinimumIdle(MinIdle)
    c.setPoolName("json-api-jdbc-pool")
    c.setMaximumPoolSize(poolSize)
    c.setIdleTimeout(IdleTimeout) // ms, minimum according to log, defaults to 600s
    c.setMaximumPoolSize(poolSize)
    new HikariDataSource(c)
  }
}
