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

  def initialize(serviceDbUser: DbUser)(implicit log: LogHandler): ConnectionIO[Unit] = {
    val createTriggerTable: Fragment = sql"""
        create table running_triggers(
          trigger_id uuid primary key,
          party_token text not null,
          package_id text not null,
          module_name text not null,
          trigger_name text not null
        )
      """
    val createDalfTable: Fragment = sql"""
        create table dalfs(
          package_id text primary key,
          package bytea not null
        )
      """

    // Create database role for the service to run as after initialization.
    // This means the client does not need to know details of the tables we use
    // and permission a user correctly themselves.
    // FIXME(RJR): Validate service username and password against injection attacks
    val service = serviceDbUser.user
    val createServiceRole: Fragment =
      Fragment.const(s"create role $service login password '${serviceDbUser.password}'")
    val grantRunningTriggersAccess: Fragment =
      Fragment.const(s"grant select, insert, delete on table running_triggers to $service")
    val grantDalfAccess: Fragment =
      Fragment.const(s"grant select, insert on table dalfs to $service")
    (createTriggerTable.update.run
      *> createDalfTable.update.run
      *> createServiceRole.update.run
      *> grantRunningTriggersAccess.update.run
      *> grantDalfAccess.update.run).void
  }
}
