// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.dbbackend

import cats.effect._
import cats.syntax.apply._
import com.digitalasset.http.domain
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import doobie.util.log
import scalaz.syntax.tag._

import scala.concurrent.ExecutionContext

class ContractDao(xa: Connection.T) {

  private implicit val lh: log.LogHandler = doobie.util.log.LogHandler.jdkLogHandler

  def initialize: ConnectionIO[Unit] =
    Queries.dropAllTablesIfExist *> Queries.initDatabase

  def lastOffset(
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg): ConnectionIO[Option[String]] =
    for {
      tpId <- Queries.surrogateTemplateId(
        templateId.packageId,
        templateId.moduleName,
        templateId.entityName)
      offset <- Queries.lastOffset(party.unwrap, tpId)
    } yield offset

  def updateOffset(
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg,
      newOffset: String): ConnectionIO[Unit] =
    for {
      tpId <- Queries.surrogateTemplateId(
        templateId.packageId,
        templateId.moduleName,
        templateId.entityName)
      _ <- Queries.updateOffset(party.unwrap, tpId, newOffset)
    } yield ()

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def transact[A](query: ConnectionIO[A]): IO[A] =
    query.transact(xa)
}

object ContractDao {
  def apply(jdbcDriver: String, jdbcUrl: String, username: String, password: String)(
      implicit ec: ExecutionContext): ContractDao = {
    val cs: ContextShift[IO] = IO.contextShift(ec)
    new ContractDao(Connection.connect(jdbcDriver, jdbcUrl, username, password)(cs))
  }
}
