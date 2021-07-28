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

  /*
  TODO:
   - The ConnectionPool and JDBCConfig objects are duplicated w.r.t trigger service, this needs to be refactored to a
   generic library in libs-scala
   - below values are hardcoded for now, refactor to be picked up as cli flags/ props later.
   */

  final val MinIdle = 8
  final val IdleTimeout = 10000
  final val ConnectionTimeout = 5000

  type PoolSize = Int
  object PoolSize {
    val Production = 10
    val Integration = 2
  }

  type T = Transactor.Aux[IO, _ <: DataSource with Closeable]

  def connect(cfg: JdbcConfig, poolSize: PoolSize)(implicit
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

  def dataSource(cfg: JdbcConfig, poolSize: PoolSize) = {

    val c = new HikariConfig()
    c.setJdbcUrl(cfg.url)
    c.setUsername(cfg.user)
    c.setAutoCommit(false)
    c.setPassword(cfg.password)
    c.setMinimumIdle(MinIdle)
    c.setPoolName("json-api-jdbc-pool")
    c.setConnectionTimeout(ConnectionTimeout)
    c.setMaximumPoolSize(poolSize)
    c.setIdleTimeout(IdleTimeout) // ms, minimum according to log, defaults to 600s
    new HikariDataSource(c)
  }
}
