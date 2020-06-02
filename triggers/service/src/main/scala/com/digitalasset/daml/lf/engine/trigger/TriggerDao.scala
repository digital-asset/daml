// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import cats.effect.{ContextShift, IO}
import cats.syntax.apply._
import cats.syntax.functor._
import com.daml.jwt.domain.Jwt
import doobie._
import doobie.LogHandler
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import doobie.postgres.implicits._
import doobie.util.log
import java.util.UUID
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

  def transact[A](query: ConnectionIO[A]): IO[A] =
    query.transact(xa)
}

object TriggerDao {
  def apply(c: JdbcConfig)(implicit ec: ExecutionContext): TriggerDao = {
    val cs: ContextShift[IO] = IO.contextShift(ec)
    new TriggerDao(Connection.connect(JdbcConfig.driver, c.url, c.user, c.password)(cs))
  }

  def initialize(implicit log: LogHandler): ConnectionIO[Unit] = {
    // Running trigger table.
    // `trigger_instance` is a UUID generated by the service
    // `party_token` is the JWT token corresponding to the party
    // `full_trigger_name` is the identifier for the trigger in its dalf,
    //  of the form "packageId:moduleName:triggerName"
    val createTriggerTable: Fragment = sql"""
        create table running_triggers(
          trigger_instance uuid primary key,
          party_token text not null,
          full_trigger_name text not null
        )
      """

    // Dalf table with binary package data.
    val createDalfTable: Fragment = sql"""
        create table dalfs(
          package_id text primary key,
          package bytea not null
        )
      """

    // Index for efficiently listing running triggers for a particular party.
    val createPartyIndex: Fragment = sql"""
        create index triggers_by_party on running_triggers(party_token)
      """

    (createTriggerTable.update.run
      *> createDalfTable.update.run
      *> createPartyIndex.update.run).void
  }

  def addRunningTrigger(t: RunningTrigger): ConnectionIO[Unit] = {
    val partyToken = t.jwt.value
    val fullTriggerName = t.triggerName.toString
    val insertTrigger: Fragment = Fragment.const(
      s"insert into running_triggers values ('${t.triggerInstance}', '$partyToken', '$fullTriggerName')"
    )
    insertTrigger.update.run.void
  }

  def getTriggersForParty(partyToken: Jwt): ConnectionIO[Vector[UUID]] = {
    val select = Fragment.const("select trigger_instance from running_triggers")
    val where = Fragment.const(s" where party_token = '${partyToken.value}'")
    val order = Fragment.const(" order by running_triggers")
    val list = select ++ where ++ order
    list.query[UUID].to[Vector]
  }
}
