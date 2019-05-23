// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Note: package name must correspond exactly to the flyway 'locations' setting, which defaults to 'db.migration'
package db.migration

import java.sql.{Connection, ResultSet}

import anorm.{BatchSql, NamedParameter}
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.platform.sandbox.stores.ledger.sql.serialisation.{
  KeyHasher,
  ValueSerializer
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.util.Conversions._
import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

class V3__Recompute_Key_Hash extends BaseJavaMigration {

  // the number of contracts proceeded in a batch.
  private val batchSize = 10 * 1000

  def migrate(context: Context): Unit = {
    implicit val conn: Connection = context.getConnection
    updateKeyHashed(loadContractKeys)
  }

  private def loadContractKeys(
      implicit connection: Connection
  ): Iterator[(AbsoluteContractId, GlobalKey)] = {

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

    val rows: ResultSet = connection.createStatement().executeQuery(SQL_SELECT_CONTRACT_KEYS)

    new Iterator[(AbsoluteContractId, GlobalKey)] {

      var hasNext: Boolean = rows.next()

      def next(): (AbsoluteContractId, GlobalKey) = {
        val contractId = AbsoluteContractId(
          Ref.ContractIdString.assertFromString(rows.getString("contract_id")))
        val templateId = Ref.Identifier(
          packageId = Ref.PackageId.assertFromString(rows.getString("package_id")),
          qualifiedName = Ref.QualifiedName.assertFromString(rows.getString("template_name"))
        )
        val key = ValueSerializer
          .deserialiseValue(rows.getBytes("contract_key"))
          .fold(err => throw new IllegalArgumentException(err.errorMessage), identity)

        hasNext = rows.next()

        contractId -> GlobalKey(templateId, key)
      }
    }

  }

  private def updateKeyHashed(contractKeys: Iterator[(AbsoluteContractId, GlobalKey)])(
      implicit conn: Connection): Unit = {

    val SQL_UPDATE_CONTRACT_KEYS_HASH =
      """
        |UPDATE
        |  contract_keys
        |SET
        |  value_hash = {valueHash}
        |WHERE
        |  contract_id = {contractId}
      """.stripMargin

    val statements = contractKeys.map {
      case (cid, key) =>
        Seq[NamedParameter]("contractId" -> cid.coid, "valueHash" -> KeyHasher.hashKeyString(key))
    }

    statements.toStream.grouped(batchSize).foreach { batch =>
      BatchSql(
        SQL_UPDATE_CONTRACT_KEYS_HASH,
        batch.head,
        batch.tail: _*
      ).execute()
    }
  }

}
