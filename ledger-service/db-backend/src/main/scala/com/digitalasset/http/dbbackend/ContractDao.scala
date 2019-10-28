// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.dbbackend

import cats.effect._
import cats.syntax.apply._
import com.digitalasset.http.dbbackend.Queries.SurrogateTpId
import com.digitalasset.http.domain
import com.typesafe.scalalogging.StrictLogging
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import scalaz.syntax.tag._

import scala.concurrent.ExecutionContext

class ContractDao(xa: DbConnection.T) extends StrictLogging {

  private implicit val lh = doobie.util.log.LogHandler.jdkLogHandler

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def initialize: IO[Unit] = {
    logger.info(s"Initialzing DB: $xa")
    (Queries.dropAllTablesIfExist *>
      Queries.initDatabase).transact(xa)
  }

  def lastOffset(
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg): IO[Option[String]] = {
    val compositeQuery: ConnectionIO[Option[String]] =
      Queries.surrogateTemplateId(
        templateId.packageId,
        templateId.moduleName,
        templateId.entityName) flatMap { surrogateTpId: SurrogateTpId =>
        Queries.lastOffset(party.unwrap, surrogateTpId)
      }

    compositeQuery.transact(xa)
  }
}

object ContractDao {
  def apply(jdbcDriver: String, jdbcUrl: String, username: String, password: String)(
      implicit ec: ExecutionContext): ContractDao = {
    val cs: ContextShift[IO] = IO.contextShift(ec)
    new ContractDao(DbConnection.connect(jdbcDriver, jdbcUrl, username, password)(cs))
  }
}
