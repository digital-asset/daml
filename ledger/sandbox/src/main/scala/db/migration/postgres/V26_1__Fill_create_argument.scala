// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package db.migration.postgres

import java.io.ByteArrayInputStream

import com.daml.platform.store.serialization.{ContractSerializer, ValueSerializer}
import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

class V26_1__Fill_create_argument extends BaseJavaMigration {

  // left join on contracts to make sure to include divulged contracts
  private val SELECT_CONTRACT_DATA =
    """select contract_data.id, contract_data.contract
      |from contract_data
      |left join contracts
      |  on contracts.id = contract_data.id
      |  and contracts.archive_offset is null""".stripMargin

  private val UPDATE_PARTICIPANT_CONTRACTS =
    "update participant_contracts set create_argument = ?, template_id = ? where contract_id = ?"

  override def migrate(context: Context): Unit = {
    val conn = context.getConnection
    var loadContracts: java.sql.Statement = null
    var updateParticipantContracts: java.sql.PreparedStatement = null
    var rows: java.sql.ResultSet = null
    try {
      updateParticipantContracts = conn.prepareStatement(UPDATE_PARTICIPANT_CONTRACTS)
      loadContracts = conn.createStatement()
      rows = loadContracts.executeQuery(SELECT_CONTRACT_DATA)

      while (rows.next()) {
        val contractId = rows.getString("id")
        val contractBytes = rows.getBinaryStream("contract")
        val contract =
          ContractSerializer
            .deserializeContractInstance(contractBytes)
            .getOrElse(sys.error(s"failed to deserialize contract $contractId"))
        val createArgument = contract.arg
        val templateId = contract.template
        val createArgumentBytes =
          new ByteArrayInputStream(
            ValueSerializer.serializeValue(
              createArgument,
              s"failed to serialize create argument for contract $contractId"))
        updateParticipantContracts.setBinaryStream(1, createArgumentBytes)
        updateParticipantContracts.setString(2, templateId.toString)
        updateParticipantContracts.setString(3, contractId)
        updateParticipantContracts.execute()
      }
    } finally {
      if (loadContracts != null) {
        loadContracts.close()
      }
      if (updateParticipantContracts != null) {
        updateParticipantContracts.close()
      }
      if (rows != null) {
        rows.close()
      }
    }
  }

}
