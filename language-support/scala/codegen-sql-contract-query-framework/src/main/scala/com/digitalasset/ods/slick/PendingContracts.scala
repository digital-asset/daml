// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ods.slick

import com.digitalasset.ledger.api.refinements.ApiTypes.ContractId
import com.digitalasset.ods.slick.implicits.{PlatformSlickImplicits, SlickImplicits}
import slick.jdbc.JdbcProfile

object PendingContracts {

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  class PendingIdSlickTable(val profile: JdbcProfile, val odsId: String)
      extends SlickImplicits
      with PlatformSlickImplicits
      with SlickProfile
      with SlickTableQuery {
    import profile.api._

    case class PendingContractId(id: ContractId)

    override def tableNameSuffix: String = "PendingContractId"

    class PendingIdTable(tag: Tag) extends Table[PendingContractId](tag, tableName) {
      def id = column[ContractId]("ID", O.PrimaryKey)
      def * = id.mapTo[PendingContractId]
    }

    private val baseDeleteSQL = s"""DELETE FROM "$tableName" WHERE "ID" = ?"""

    final def delete(
        contractIds: Iterable[String]): DBIOAction[Array[Int], NoStream, Effect.All] = {
      SimpleDBIO[Array[Int]] { session =>
        val statement = session.connection.prepareStatement(baseDeleteSQL)
        contractIds.foreach { id =>
          statement.setString(1, id)
          statement.addBatch()
        }
        statement.executeBatch()
      }
    }

    def all: TableQuery[PendingIdTable] = TableQuery[PendingIdTable]

    override def schema: profile.SchemaDescription = all.schema
  }
}
