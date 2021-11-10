// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.db.migration.postgres

import com.daml.lf.data.Ref
import com.daml.lf.transaction.GlobalKey
import com.daml.platform.db.migration.translation.ValueSerializer
import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

private[migration] final class V32_1__Fix_key_hashes extends BaseJavaMigration {

  private val SELECT_KEYS =
    "select participant_events.contract_id, participant_events.template_id, create_key_value from participant_events inner join participant_contracts on participant_events.contract_id = participant_contracts.contract_id where create_key_value is not null"

  private val FIX_HASH =
    "update participant_contracts set create_key_hash = ? where contract_id = ?"

  private val BATCH_SIZE = 500

  override def migrate(context: Context): Unit = {
    val conn = context.getConnection
    var selectKeys: java.sql.Statement = null
    var fixHash: java.sql.PreparedStatement = null
    var keysRows: java.sql.ResultSet = null
    try {
      fixHash = conn.prepareStatement(FIX_HASH)

      selectKeys = conn.createStatement()
      selectKeys.setFetchSize(BATCH_SIZE)
      keysRows = selectKeys.executeQuery(SELECT_KEYS)

      var currentBatchSize = 0

      while (keysRows.next()) {
        val contractId = keysRows.getString("contract_id")
        val rawTemplateId = keysRows.getString("template_id")
        val templateId = Ref.Identifier.assertFromString(rawTemplateId)
        val rawKeyValue = keysRows.getBinaryStream("create_key_value")
        val keyValue = ValueSerializer.deserializeValue(rawKeyValue)
        val key = GlobalKey.assertBuild(templateId, keyValue.unversioned)
        val hashBytes = key.hash.bytes.toInputStream

        fixHash.setBinaryStream(1, hashBytes)
        fixHash.setString(2, contractId)
        fixHash.addBatch()
        currentBatchSize += 1

        if (currentBatchSize == BATCH_SIZE) {
          val _ = fixHash.executeBatch()
          currentBatchSize = 0
        }
      }

      if (currentBatchSize > 0) {
        val _ = fixHash.executeBatch()
      }

    } finally {
      if (keysRows != null) {
        keysRows.close()
      }
      if (selectKeys != null) {
        selectKeys.close()
      }
      if (fixHash != null) {
        fixHash.close()
      }
    }
  }
}
