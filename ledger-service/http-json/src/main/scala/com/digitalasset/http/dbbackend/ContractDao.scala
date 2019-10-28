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

  def initialize: IO[Unit] =
    transact(
      Queries.dropAllTablesIfExist *>
        Queries.initDatabase: ConnectionIO[Unit]
    )

  def lastOffset(
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg): IO[Option[String]] =
    transact(
      Queries.surrogateTemplateId(
        templateId.packageId,
        templateId.moduleName,
        templateId.entityName) flatMap { surrogateTpId =>
        Queries.lastOffset(party.unwrap, surrogateTpId)
      }: ConnectionIO[Option[String]]
    )

  def updateOffset(
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg,
      newOffset: String): IO[Unit] =
    transact(
      Queries.surrogateTemplateId(
        templateId.packageId,
        templateId.moduleName,
        templateId.entityName) flatMap { surrogateTpId =>
        Queries.updateOffset(party.unwrap, surrogateTpId, newOffset)
      }: ConnectionIO[Unit]
    )

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def transact[A](c: ConnectionIO[A]): IO[A] =
    c.transact(xa)
}

object ContractDao {
  def apply(jdbcDriver: String, jdbcUrl: String, username: String, password: String)(
      implicit ec: ExecutionContext): ContractDao = {
    val cs: ContextShift[IO] = IO.contextShift(ec)
    new ContractDao(Connection.connect(jdbcDriver, jdbcUrl, username, password)(cs))
  }
}
