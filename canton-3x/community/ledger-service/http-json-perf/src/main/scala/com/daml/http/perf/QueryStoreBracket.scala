// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf

import com.daml.dbutils
import com.daml.dbutils.ConnectionPool
import com.daml.http.dbbackend.{DbStartupMode, JdbcConfig}
import com.daml.http.perf.Config.QueryStoreIndex
import com.daml.http.util.FutureUtil.toFuture
import com.daml.testing.postgresql.PostgresDatabase

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

private[this] final case class QueryStoreBracket[S, D](
    state: () => S,
    start: S => Try[D],
    config: (S, D) => JdbcConfig,
    stop: (S, D) => Try[Unit],
)

private[this] object QueryStoreBracket {
  private def jsonApiJdbcConfig(c: PostgresDatabase): JdbcConfig =
    JdbcConfig(
      dbutils
        .JdbcConfig(
          driver = "org.postgresql.Driver",
          url = c.url,
          user = "test",
          password = "",
          ConnectionPool.PoolSize.Production,
        ),
      startMode = DbStartupMode.CreateOnly,
    )

  type T = QueryStoreBracket[_, _]
  val Postgres: T = QueryStoreBracket[PostgresRunner, PostgresDatabase](
    () => new PostgresRunner(),
    _.start(),
    (_, d) => jsonApiJdbcConfig(d),
    (r, _) => r.stop(),
  )

  import com.daml.testing.oracle, oracle.OracleAround
  val Oracle: T = QueryStoreBracket[OracleRunner, OracleAround.RichOracleUser](
    () => new OracleRunner,
    _.start(),
    _ jdbcConfig _,
    _.stop(_),
  )

  def lookup(q: QueryStoreIndex): Option[T] = q match {
    case QueryStoreIndex.No => None
    case QueryStoreIndex.Postgres => Some(Postgres)
    case QueryStoreIndex.Oracle => Some(Oracle)
  }

  def withJsonApiJdbcConfig[A](
      jsonApiQueryStoreEnabled: QueryStoreIndex
  )(
      fn: Option[JdbcConfig] => Future[A]
  )(implicit
      ec: ExecutionContext
  ): Future[A] = QueryStoreBracket lookup jsonApiQueryStoreEnabled match {
    case Some(b: QueryStoreBracket[s, d]) =>
      import b._
      for {
        dbInstance <- Future.successful(state())
        dbConfig <- toFuture(start(dbInstance))
        jsonApiDbConfig <- Future.successful(config(dbInstance, dbConfig))
        a <- fn(Some(jsonApiDbConfig))
        _ <- Future.successful(
          stop(dbInstance, dbConfig) // XXX ignores resulting Try
        ) // TODO: use something like `lf.data.TryOps.Bracket.bracket`
      } yield a

    case None => fn(None)
  }
}
