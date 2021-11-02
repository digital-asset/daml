// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Note: package name must correspond exactly to the flyway 'locations' setting, which defaults to
// 'db.migration.postgres' for postgres migrations
package com.daml.platform.db.migration.postgres

import java.sql.{Connection, ResultSet}

import anorm.{BatchSql, NamedParameter}
import com.daml.lf.data.Ref
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value.ContractId
import com.daml.platform.store.serialization.{KeyHasher, ValueSerializer}
import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

import scala.collection.compat.immutable.LazyList

private[migration] class V3__Recompute_Key_Hash extends BaseJavaMigration {

  // the number of contracts proceeded in a batch.
  private val batchSize = 10 * 1000

  def migrate(context: Context): Unit = {
    implicit val conn: Connection = context.getConnection
    updateKeyHashed(loadContractKeys)
  }

  private def loadContractKeys(implicit
      connection: Connection
  ): Iterator[(ContractId, GlobalKey)] = {

    val SQL_SELECT_CONTRACT_KEYS =
      """
      |SELECT
      |  contracts.id as contract_id,
      |  contracts.package_id as package_id,
      |  contracts.name as template_name,
      |  contracts.key as contract_key
      |FROM
      |  contracts
      |WHERE
      |  contracts.key is not null
    """.stripMargin

    val statement = connection.createStatement()
    statement.setFetchSize(batchSize)
    val rows: ResultSet = statement.executeQuery(SQL_SELECT_CONTRACT_KEYS)

    new Iterator[(ContractId, GlobalKey)] {

      var hasNext: Boolean = rows.next()

      def next(): (ContractId, GlobalKey) = {
        val contractId = ContractId.assertFromString(rows.getString("contract_id"))
        val templateId = Ref.Identifier(
          packageId = Ref.PackageId.assertFromString(rows.getString("package_id")),
          qualifiedName = Ref.QualifiedName.assertFromString(rows.getString("template_name")),
        )
        val key = ValueSerializer.deserializeValue(rows.getBinaryStream("contract_key"))

        hasNext = rows.next()
        contractId -> GlobalKey(templateId, key.value)
      }
    }

  }

  private def updateKeyHashed(
      contractKeys: Iterator[(ContractId, GlobalKey)]
  )(implicit conn: Connection): Unit = {

    val SQL_UPDATE_CONTRACT_KEYS_HASH =
      """
        |UPDATE
        |  contract_keys
        |SET
        |  value_hash = {valueHash}
        |WHERE
        |  contract_id = {contractId}
      """.stripMargin

    val statements = contractKeys.map { case (cid, key) =>
      Seq[NamedParameter]("contractId" -> cid.coid, "valueHash" -> KeyHasher.hashKeyString(key))
    }

    statements.to(LazyList).grouped(batchSize).foreach { batch =>
      BatchSql(
        SQL_UPDATE_CONTRACT_KEYS_HASH,
        batch.head,
        batch.tail: _*
      ).execute()
    }
  }

}
