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
import scalaz.OneAnd
import scalaz.syntax.tag._
import spray.json.{JsNull, JsValue}

import scala.concurrent.ExecutionContext

class ContractDao(xa: Connection.T) {

  implicit val logHandler: log.LogHandler = doobie.util.log.LogHandler.jdkLogHandler

  def transact[A](query: ConnectionIO[A]): IO[A] =
    query.transact(xa)

  def isValid(timeoutSeconds: Int): IO[Boolean] =
    fconn.isValid(timeoutSeconds).transact(xa)
}

object ContractDao {
  def apply(jdbcDriver: String, jdbcUrl: String, username: String, password: String)(
      implicit ec: ExecutionContext): ContractDao = {
    val cs: ContextShift[IO] = IO.contextShift(ec)
    new ContractDao(Connection.connect(jdbcDriver, jdbcUrl, username, password)(cs))
  }

  def initialize(implicit log: LogHandler): ConnectionIO[Unit] =
    Queries.dropAllTablesIfExist *> Queries.initDatabase

  def lastOffset(parties: OneAnd[Set, domain.Party], templateId: domain.TemplateId.RequiredPkg)(
      implicit log: LogHandler): ConnectionIO[Map[domain.Party, domain.Offset]] = {
    import doobie.postgres.implicits._
    for {
      tpId <- surrogateTemplateId(templateId)
      offset <- Queries
        .lastOffset(domain.Party.unsubst(parties), tpId)
    } yield {
      type L[a] = Map[a, domain.Offset]
      domain.Party.subst[L, String](domain.Offset.tag.subst(offset))
    }
  }

  def updateOffset(
      parties: OneAnd[Set, domain.Party],
      templateId: domain.TemplateId.RequiredPkg,
      newOffset: domain.Offset,
      lastOffsets: Map[domain.Party, domain.Offset])(
      implicit log: LogHandler): ConnectionIO[Unit] = {
    import cats.implicits._
    import doobie.postgres.implicits._
    import scalaz.OneAnd._
    import scalaz.std.set._
    import scalaz.syntax.foldable._
    val partyVector = domain.Party.unsubst(parties.toVector)
    val lastOffsetsStr: Map[String, String] =
      domain.Party.unsubst[Map[*, String], String](domain.Offset.tag.unsubst(lastOffsets))
    for {
      tpId <- Queries.surrogateTemplateId(
        templateId.packageId,
        templateId.moduleName,
        templateId.entityName)
      rowCount <- Queries.updateOffset(partyVector, tpId, newOffset.unwrap, lastOffsetsStr)
      _ <- if (rowCount == partyVector.size)
        fconn.pure(())
      else
        fconn.raiseError(StaleOffsetException(parties, templateId, newOffset, lastOffsets))
    } yield ()
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def selectContracts(
      parties: OneAnd[Set, domain.Party],
      templateId: domain.TemplateId.RequiredPkg,
      predicate: doobie.Fragment)(
      implicit log: LogHandler): ConnectionIO[Vector[domain.ActiveContract[JsValue]]] = {
    import doobie.postgres.implicits._
    for {
      tpId <- surrogateTemplateId(templateId)

      dbContracts <- Queries
        .selectContracts(domain.Party.unsubst(parties), tpId, predicate)
        .to[Vector]
      domainContracts = dbContracts.map(toDomain(templateId))
    } yield domainContracts
  }

  private[http] def fetchById(
      parties: OneAnd[Set, domain.Party],
      templateId: domain.TemplateId.RequiredPkg,
      contractId: domain.ContractId)(
      implicit log: LogHandler): ConnectionIO[Option[domain.ActiveContract[JsValue]]] = {
    import doobie.postgres.implicits._
    for {
      tpId <- surrogateTemplateId(templateId)
      dbContracts <- Queries.fetchById(
        domain.Party unsubst parties,
        tpId,
        domain.ContractId unwrap contractId)
    } yield dbContracts.map(toDomain(templateId))
  }

  private[http] def fetchByKey(
      parties: OneAnd[Set, domain.Party],
      templateId: domain.TemplateId.RequiredPkg,
      key: JsValue)(
      implicit log: LogHandler): ConnectionIO[Option[domain.ActiveContract[JsValue]]] = {
    import doobie.postgres.implicits._
    for {
      tpId <- surrogateTemplateId(templateId)
      dbContracts <- Queries.fetchByKey(domain.Party unsubst parties, tpId, key)
    } yield dbContracts.map(toDomain(templateId))
  }

  private[this] def surrogateTemplateId(templateId: domain.TemplateId.RequiredPkg)(
      implicit log: LogHandler) =
    Queries.surrogateTemplateId(templateId.packageId, templateId.moduleName, templateId.entityName)

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
      parties: OneAnd[Set, domain.Party],
      templateId: domain.TemplateId.RequiredPkg,
      newOffset: domain.Offset,
      lastOffset: Map[domain.Party, domain.Offset])
      extends java.sql.SQLException(
        s"parties: $parties, templateId: $templateId, newOffset: $newOffset, lastOffset: $lastOffset",
        StaleOffsetException.SqlState
      )

  object StaleOffsetException {
    val SqlState = "STALE_OFFSET_EXCEPTION"
  }
}
