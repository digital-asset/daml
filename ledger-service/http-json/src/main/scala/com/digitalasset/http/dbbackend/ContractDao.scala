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

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def initialize: IO[Unit] = {
    (Queries.dropAllTablesIfExist *>
      Queries.initDatabase).transact(xa)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def lastOffset(
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg): IO[Option[String]] = {
    val compositeQuery: ConnectionIO[Option[String]] =
      Queries.surrogateTemplateId(
        templateId.packageId,
        templateId.moduleName,
        templateId.entityName) flatMap { surrogateTpId =>
        Queries.lastOffset(party.unwrap, surrogateTpId)
      }

    compositeQuery.transact(xa)
  }
}

object ContractDao {
  def apply(jdbcDriver: String, jdbcUrl: String, username: String, password: String)(
      implicit ec: ExecutionContext): ContractDao = {
    val cs: ContextShift[IO] = IO.contextShift(ec)
    new ContractDao(Connection.connect(jdbcDriver, jdbcUrl, username, password)(cs))
  }
}
