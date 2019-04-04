// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ods.slick

import com.digitalasset.ledger.api.refinements.ApiTypes
import com.digitalasset.ledger.client.binding.Primitive.ContractId
import com.digitalasset.ledger.client.binding.Template
import com.digitalasset.ods.slick.implicits.{PlatformSlickImplicits, SlickImplicits}
import slick.jdbc.JdbcProfile

/**
  * A utility trait that couple together a slick `Table`
  * and the `TableQuery`. [[ContractSlickTableQuery.nonPending]] is the
  * entry point for building queries with automatic filtering
  * of entries corresponding to pending contracts. Methods
  * for changing the state of table are also available, e.g.
  * [[ContractSlickTableQuery.insert]] for adding new elements.
  * The annotation [[com.digitalasset.ods.slick.slickTable]]
  * automatically generates an instance of this interface
  * for a given type.
  */
trait ContractSlickTableQuery[T, Tpl <: Template[Tpl]]
    extends SlickImplicits
    with PlatformSlickImplicits
    with SlickProfile
    with SlickTableQuery {
  val profile: JdbcProfile
  import profile.api._

  type TableType <: ContractTable[T]

  /**
    * Provides access to all records in this table.
    * As the contracts returned may already be under consumption by the Nanobot, it is
    * recommended to use the [[nonPending]] method to access a filtered view of this data.
    * @return Query object that can be used to build queries.
    */
  protected def all: TableQuery[TableType]
  final def schema: profile.SchemaDescription = all.schema

  lazy val pendingContractsTable = new PendingContracts.PendingIdSlickTable(profile, odsId)

  /**
    * Provides access to all non-pending records in this table.
    * These records correspond to the expected state of the ledger, as they do not contain
    * entries that the Nanobot already started consuming.
    * @return Query object that can be used to build queries.
    */
  final def nonPending: profile.api.Query[TableType, T, Seq] = queryInstance

  /**
    * Provides access to all non-pending records in this table.
    * These records correspond to the expected state of the ledger, as they do not contain
    * entries that the Nanobot already started consuming.
    * @return Query object that can be used to build queries.
    */
  @deprecated("Use nonPending instead.", "0.11.23")
  def query: profile.api.Query[TableType, T, Seq] = queryInstance

  private final lazy val queryInstance: Query[TableType, T, Seq] = {
    val pendingQuery = pendingContractsTable.all
    all
      .joinLeft(pendingQuery)
      .on(_.id.asColumnOf[ApiTypes.ContractId] === _.id)
      .filter { case (_, pendingOption) => pendingOption.isEmpty }
      .map { case (original, _) => original }
  }

  final def insert(t: Seq[T]): DBIOAction[Option[Int], NoStream, Effect.Write] =
    all ++= t

  final def delete(contractIds: Seq[String]): DBIOAction[Array[Int], NoStream, Effect.All] = {
    SimpleDBIO[Array[Int]] { session =>
      val statement = session.connection.prepareStatement(baseDeleteSQL)
      contractIds.foreach { id =>
        statement.setString(1, id)
        statement.addBatch()
      }
      statement.executeBatch()
    }
  }

  final lazy val deleteAll: DBIOAction[Int, NoStream, Effect.Write] =
    all.delete

  private lazy val baseDeleteSQL = s"""DELETE FROM "${all.baseTableRow.tableName}" WHERE "ID" = ?"""

  abstract class ContractTable[C](tag: Tag, tableName: String)
      extends profile.Table[C](tag, tableName) {
    def id = column[ContractId[Tpl]]("ID", O.PrimaryKey)
  }
}
