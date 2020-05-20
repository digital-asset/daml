// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import cats.effect.{ContextShift, IO}
import cats.syntax.apply._
import cats.syntax.functor._
import doobie._
import doobie.LogHandler
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import doobie.util.log
import scala.concurrent.ExecutionContext

object Connection {

  type T = Transactor.Aux[IO, Unit]

  def connect(jdbcDriver: String, jdbcUrl: String, username: String, password: String)(
    implicit cs: ContextShift[IO]): T =
    Transactor
      .fromDriverManager[IO](jdbcDriver, jdbcUrl, username, password)(IO.ioConcurrentEffect(cs), cs)
}

class TriggerDao(xa: Connection.T) {

  implicit val logHandler: log.LogHandler = doobie.util.log.LogHandler.jdkLogHandler

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def transact[A](query: ConnectionIO[A]): IO[A] =
    query.transact(xa)
}

object TriggerDao {
  def apply(jdbcDriver: String, jdbcUrl: String, username: String, password: String)(
    implicit ec: ExecutionContext): TriggerDao = {
    val cs: ContextShift[IO] = IO.contextShift(ec)
    new TriggerDao(Connection.connect(jdbcDriver, jdbcUrl, username, password)(cs))
  }

  def initialize(implicit log: LogHandler): ConnectionIO[Unit] = {
    val createTriggerTable: Fragment = sql"""
        create table running_triggers(
          trigger_id uuid primary key,
          party text not null,
          trigger_name text not null
        )
      """
    val createDarTable: Fragment = sql"""
        create table dar_packages(
          package_id text primary key,
          package text not null
        )
      """
    (createTriggerTable.update.run
      *> createDarTable.update.run).void
  }
}
