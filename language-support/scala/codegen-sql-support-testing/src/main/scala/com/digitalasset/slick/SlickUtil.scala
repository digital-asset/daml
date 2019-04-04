// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.slick

import com.digitalasset.ledger.client.binding.ValueRef
import com.digitalasset.ledger.client.binding.encoding.LfEncodable
import slick.jdbc.H2Profile.api._
import slick.jdbc.JdbcProfile
import slick.lifted.TableQuery

import scala.concurrent.Future

object SlickUtil {
  def createTable[A <: ValueRef: LfEncodable](profile: JdbcProfile)(
      db: profile.api.Database,
      table: TableQuery[profile.Table[A]]): Future[Unit] = {
    val create = DBIO.seq(table.schema.create)
    db.run(create)
  }

  def dropTable[A <: ValueRef: LfEncodable](profile: JdbcProfile)(
      db: profile.api.Database,
      table: TableQuery[profile.Table[A]]): Future[Unit] = {
    val drop = DBIO.seq(table.schema.drop)
    db.run(drop)
  }
}
