// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.dbbackend

import cats.effect._
import cats.syntax.apply._
import com.digitalasset.http.domain
import doobie.LogHandler
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import doobie.util.log
import scalaz.syntax.tag._
import doobie.free.{connection => fconn}
import scala.concurrent.ExecutionContext

class ContractDao(xa: Connection.T) {

  implicit val logHandler: log.LogHandler = doobie.util.log.LogHandler.jdkLogHandler

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

  def initialize(implicit log: LogHandler): ConnectionIO[Unit] =
    Queries.dropAllTablesIfExist *> Queries.initDatabase

  def lastOffset(party: domain.Party, templateId: domain.TemplateId.RequiredPkg)(
      implicit log: LogHandler): ConnectionIO[Option[domain.Offset]] =
    for {
      tpId <- Queries.surrogateTemplateId(
        templateId.packageId,
        templateId.moduleName,
        templateId.entityName)
      offset <- Queries.lastOffset(party.unwrap, tpId).map(_.map(domain.Offset(_)))
    } yield offset

  def updateOffset(
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg,
      newOffset: domain.Offset,
      lastOffset: Option[domain.Offset])(implicit log: LogHandler): ConnectionIO[Unit] =
    for {
      tpId <- Queries.surrogateTemplateId(
        templateId.packageId,
        templateId.moduleName,
        templateId.entityName)
      rowCount <- Queries.updateOffset(
        party.unwrap,
        tpId,
        newOffset.unwrap,
        lastOffset.map(_.unwrap))
      _ <- if (rowCount == 1)
        fconn.pure(())
      else
        fconn.raiseError(StaleOffsetException(party, templateId, newOffset, lastOffset))
    } yield ()

  final case class StaleOffsetException(
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg,
      newOffset: domain.Offset,
      lastOffset: Option[domain.Offset])
      extends java.sql.SQLException(
        s"party: $party, templateId: $templateId, newOffset: $newOffset, lastOffset: $lastOffset",
        StaleOffsetException.SqlState
      )

  object StaleOffsetException {
    val SqlState = "STALE_OFFSET_EXCEPTION"
  }
}
