// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import cats.effect._
import cats.syntax.apply._
import com.daml.http.domain
import com.daml.http.json.JsonProtocol.LfValueDatabaseCodec
import doobie.LogHandler
import doobie.free.connection.ConnectionIO
import doobie.free.{connection => fconn}
import doobie.implicits._
import doobie.util.log
import scalaz.syntax.tag._
import spray.json.{JsNull, JsValue}

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

  def selectContracts(
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg,
      predicate: doobie.Fragment)(
      implicit log: LogHandler): ConnectionIO[Vector[domain.ActiveContract[JsValue]]] = {
    import doobie.postgres.implicits._
    for {
      tpId <- Queries.surrogateTemplateId(
        templateId.packageId,
        templateId.moduleName,
        templateId.entityName)

      dbContracts <- Queries.selectContracts(party.unwrap, tpId, predicate).to[Vector]
      domainContracts = dbContracts.map(toDomain(templateId))
    } yield domainContracts
  }

  private def toDomain(templateId: domain.TemplateId.RequiredPkg)(
      a: Queries.DBContract[Unit, JsValue, JsValue, Vector[String]])
    : domain.ActiveContract[JsValue] =
    domain.ActiveContract(
      contractId = domain.ContractId(a.contractId),
      templateId = templateId,
      key = decodeOption(a.key),
      payload = LfValueDatabaseCodec.asLfValueCodec(a.payload),
      signatories = domain.Party.subst(a.signatories),
      observers = domain.Party.subst(a.observers),
      agreementText = a.agreementText
    )

  private def decodeOption(a: JsValue): Option[JsValue] = a match {
    case JsNull => None
    case _ => Some(LfValueDatabaseCodec.asLfValueCodec(a))
  }

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
