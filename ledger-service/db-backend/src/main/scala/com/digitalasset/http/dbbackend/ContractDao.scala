// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.dbbackend

import cats.effect._
import cats.syntax.apply._
import com.typesafe.scalalogging.StrictLogging
import doobie.implicits._

import scala.concurrent.ExecutionContext

class ContractDao(xa: DbConnection.T) extends StrictLogging {

  private implicit val lh = doobie.util.log.LogHandler.jdkLogHandler

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def initialize: IO[Unit] = {
    // TODO(Leo) skip it if tables created
    logger.info(s"Initialzing DB: $xa")
    (Queries.dropAllTablesIfExist *>
      Queries.initDatabase).transact(xa)
  }
}

object ContractDao {
  def apply(jdbcDriver: String, jdbcUrl: String, username: String, password: String)(
      implicit ec: ExecutionContext): ContractDao = {
    val cs: ContextShift[IO] = IO.contextShift(ec)
    new ContractDao(DbConnection.connect(jdbcDriver, jdbcUrl, username, password)(cs))
  }
}
