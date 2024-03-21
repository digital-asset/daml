// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.dbutils

import cats.effect.{Blocker, ContextShift, IO}
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

  type PoolSize = Int
  object PoolSize {
    val Integration = 2
    val Production = 8
  }

  type T = Transactor.Aux[IO, _ <: DataSource with Closeable]

  def connect(
      c: JdbcConfig
  )(implicit
      ec: ExecutionContext,
      cs: ContextShift[IO],
  ): (DataSource with Closeable, T) = {
    val ds = dataSource(c)
    (
      ds,
      Transactor
        .fromDataSource[IO](
          ds,
          connectEC = ec,
          blocker = Blocker liftExecutorService newWorkStealingPool(c.poolSize),
        )(IO.ioConcurrentEffect(cs), cs),
    )
  }

  private[this] def dataSource(
      jc: JdbcConfig
  ) = {
    import jc._
    val c = new HikariConfig
    c.setJdbcUrl(url)
    c.setUsername(user)
    c.setPassword(password)
    c.setMinimumIdle(jc.minIdle)
    c.setConnectionTimeout(jc.connectionTimeout.toMillis)
    c.setMaximumPoolSize(poolSize)
    c.setIdleTimeout(jc.idleTimeout.toMillis)
    new HikariDataSource(c)
  }
}
