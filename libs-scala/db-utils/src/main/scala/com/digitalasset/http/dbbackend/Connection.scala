// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

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

/*
  TODO below values are hardcoded for now, refactor to be picked up as cli flags/ props later.
 */
object ConnectionPool {

  final val MinIdle = 8
  final val IdleTimeout = 10000L // ms, minimum according to log, defaults to 600s
  final val ConnectionTimeout = 5000L

  type PoolSize = Int
  object PoolSize {
    val Integration = 2
    val Production = 8
  }

  type T = Transactor.Aux[IO, _ <: DataSource with Closeable]

  def connect(
      c: JdbcConfig,
      poolSize: PoolSize,
      minIdle: Int = MinIdle,
      idleTimeout: Long = IdleTimeout,
      connectionTimeout: Long = ConnectionTimeout,
  )(implicit
      ec: ExecutionContext,
      cs: ContextShift[IO],
  ): (DataSource with Closeable, T) = {
    val ds = dataSource(c, poolSize, minIdle, idleTimeout, connectionTimeout)
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

  private[this] def dataSource(
      jc: JdbcConfig,
      poolSize: PoolSize,
      minIdle: Int,
      idleTimeout: Long,
      connectionTimeout: Long,
  ) = {
    import jc._
    val c = new HikariConfig
    c.setJdbcUrl(url)
    c.setUsername(user)
    c.setPassword(password)
    c.setMinimumIdle(minIdle)
    c.setConnectionTimeout(connectionTimeout)
    c.setMaximumPoolSize(poolSize)
    c.setIdleTimeout(idleTimeout)
    new HikariDataSource(c)
  }
}
