// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package db.migration.postgres.v27_backfill_participant_contracts

import anorm.{BatchSql, NamedParameter}
import com.daml.platform.store.Conversions._

// Copied here (with some modifications) to make it
// safe against future refactoring in production code
/**
  * A table storing a flattened representation of a [[WitnessRelation]]
  */
private[postgres] object V27WitnessesTable {

  private val tableName = "participant_contract_witnesses"
  private val idColumn = "contract_id"
  private val witnessColumn = "contract_witness"

  private val insert =
    s"insert into $tableName($idColumn, $witnessColumn) values ({$idColumn}, {$witnessColumn}) on conflict do nothing"

  final def prepareBatchInsert(witnesses: WitnessRelation[ContractId]): Option[BatchSql] = {
    val flattenedWitnesses = Relation.flatten(witnesses)
    if (flattenedWitnesses.nonEmpty) {
      val ws = flattenedWitnesses.map {
        case (id, party) => Vector[NamedParameter](idColumn -> id, witnessColumn -> party)
      }.toSeq
      Some(BatchSql(insert, ws.head, ws.tail: _*))
    } else {
      None
    }
  }

  protected val delete = s"delete from $tableName where $idColumn = {$idColumn}"

  final def prepareBatchDelete(ids: Seq[ContractId]): Option[BatchSql] = {
    if (ids.nonEmpty) {
      val parameters = ids.map(id => Vector[NamedParameter](idColumn -> id))
      Some(BatchSql(delete, parameters.head, parameters.tail: _*))
    } else {
      None
    }
  }

}
