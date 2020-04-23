// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package db.migration.postgres

import com.daml.lf.data.Ref
import com.daml.lf.transaction.Node.GlobalKey
import com.daml.platform.store.serialization.ValueSerializer
import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

final class V28__Fix_key_hashes extends BaseJavaMigration {

  private val SELECT_KEYS =
    "select contract_id, template_id, create_key_value from participant_events where create_key_value is not null and create_consumed_at is null"

  private val FIX_HASH =
    "update participant_contracts set create_key_hash = ? where contract_id = ?"

  override def migrate(context: Context): Unit = {
    val conn = context.getConnection
    var selectKeys: java.sql.Statement = null
    var fixHash: java.sql.PreparedStatement = null
    var keysRows: java.sql.ResultSet = null
    try {
      fixHash = conn.prepareStatement(FIX_HASH)

      selectKeys = conn.createStatement()
      keysRows = selectKeys.executeQuery(SELECT_KEYS)

      while (keysRows.next()) {
        val contractId = keysRows.getString("contract_id")
        val rawTemplateId = keysRows.getString("template_id")
        val templateId = Ref.Identifier.assertFromString(rawTemplateId)
        val rawKeyValue = keysRows.getBinaryStream("create_key_value")
        val keyValue = ValueSerializer.deserializeValue(rawKeyValue)
        val key = GlobalKey.assertBuild(templateId, keyValue.value)
        val hashBytes = key.hash.bytes.toInputStream

        fixHash.setBinaryStream(1, hashBytes)
        fixHash.setString(2, contractId)
        fixHash.addBatch()
      }
      val _ = fixHash.executeBatch()

    } finally {
      if (selectKeys != null) {
        selectKeys.close()
      }
      if (fixHash != null) {
        fixHash.close()
      }
      if (keysRows != null) {
        keysRows.close()
      }
    }
  }
}
