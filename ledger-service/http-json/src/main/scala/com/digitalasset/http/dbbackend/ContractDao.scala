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

import scala.collection.compat._
import scala.concurrent.ExecutionContext

class ContractDao(xa: Connection.T) {

  implicit val logHandler: log.LogHandler = doobie.util.log.LogHandler.jdkLogHandler

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

  def lastOffset(parties: Set[domain.Party], templateId: domain.TemplateId.RequiredPkg)(
      implicit log: LogHandler): ConnectionIO[Option[domain.Offset]] = {
    import doobie.postgres.implicits._
    for {
      tpId <- Queries.surrogateTemplateId(
        templateId.packageId,
        templateId.moduleName,
        templateId.entityName)
      offset <- Queries.lastOffset(parties.map(_.unwrap), tpId).map(_.map(domain.Offset(_)))
    } yield offset
  }

  def updateOffset(
      parties: Set[domain.Party],
      templateId: domain.TemplateId.RequiredPkg,
      newOffset: domain.Offset,
      lastOffset: Option[domain.Offset])(implicit log: LogHandler): ConnectionIO[Unit] = {
    import cats.implicits._
    import scalaz.std.list._
    for {
      tpId <- Queries.surrogateTemplateId(
        templateId.packageId,
        templateId.moduleName,
        templateId.entityName)
      rowCount <- Queries.updateOffset(parties.map(_.unwrap).toList, tpId, newOffset.unwrap)
      _ <- if (rowCount == parties.size)
        fconn.pure(())
      else
        fconn.raiseError(StaleOffsetException(parties, templateId, newOffset, lastOffset))
    } yield ()
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def selectContracts(
      parties: Set[domain.Party],
      templateId: domain.TemplateId.RequiredPkg,
      predicate: doobie.Fragment)(
      implicit log: LogHandler): ConnectionIO[Vector[domain.ActiveContract[JsValue]]] = {
    import doobie.postgres.implicits._
    for {
      tpId <- Queries.surrogateTemplateId(
        templateId.packageId,
        templateId.moduleName,
        templateId.entityName)

      dbContracts <- Queries
        .selectContracts(parties.map(_.unwrap).toArray, tpId, predicate)
        .to(Vector)
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
      parties: Set[domain.Party],
      templateId: domain.TemplateId.RequiredPkg,
      newOffset: domain.Offset,
      lastOffset: Option[domain.Offset])
      extends java.sql.SQLException(
        s"parties: $parties, templateId: $templateId, newOffset: $newOffset, lastOffset: $lastOffset",
        StaleOffsetException.SqlState
      )

  object StaleOffsetException {
    val SqlState = "STALE_OFFSET_EXCEPTION"
  }
}
