// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Note: package name must correspond exactly to the flyway 'locations' setting, which defaults to 'db.migration'
package db.migration

import java.sql.{Connection, ResultSet}

import anorm.{BatchSql, NamedParameter}
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, VersionedValue}
import com.digitalasset.platform.sandbox.stores.ledger.sql.serialisation.{
  KeyHasher,
  ValueSerializer
}
import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

class V3__Recompute_Key_Hash extends BaseJavaMigration {

  def migrate(context: Context): Unit = {
    implicit val conn: Connection = context.getConnection
    updateKeyHashed(loadContractKeys)
  }

  private val SQL_SELECT_CONTRACT_KEYS =
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

  private val SQL_UPDATE_CONTRACT_KEYS_HASH =
    """
      |UPDATE
      |  contract_keys
      |SET
      |  value_hash = {valueHash}
      |WHERE
      |  contract_id = {contractId}
      |""".stripMargin

  def loadContractKeys(
      implicit connection: Connection
  ): ImmArray[(AbsoluteContractId, GlobalKey)] = {

    var stack = FrontStack.empty[(AbsoluteContractId, GlobalKey)]

    val rows: ResultSet = connection.createStatement().executeQuery(SQL_SELECT_CONTRACT_KEYS)

    while (rows.next()) {
      val contractId = AbsoluteContractId(rows.getString("contract_id"))
      val packageId = Ref.PackageId.assertFromString(rows.getString("package_id"))
      val templateName = rows.getString("template_name")
      val templateId = Ref.Identifier(packageId, Ref.QualifiedName.assertFromString(templateName))
      val keyStream = rows.getBytes("contract_key")
      val key: VersionedValue[AbsoluteContractId] = ValueSerializer
        .deserialiseValue(keyStream)
        .fold(err => throw new IllegalArgumentException(err.errorMessage), identity)

      stack = (contractId -> GlobalKey(templateId, key)) +: stack

    }
    stack.toImmArray
  }

  def updateKeyHashed(contractKeys: ImmArray[(AbsoluteContractId, GlobalKey)])(
      implicit conn: Connection): Unit = {
    val statements = contractKeys.map {
      case (cid, key) =>
        Seq[NamedParameter](
          "contractId" -> cid.coid,
          "valueHash" -> KeyHasher.hashKeyString(key)
        )
    }

    statements match {
      case ImmArrayCons(firstStatement, otherStatements) =>
        BatchSql(
          SQL_UPDATE_CONTRACT_KEYS_HASH,
          firstStatement,
          otherStatements.toSeq: _*
        ).execute
        ()
      case _ =>
        ()
    }
  }

}
