// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import anorm.{BatchSql, NamedParameter}
import com.daml.platform.store.Conversions._

/**
  * A table storing a flattened representation of a [[WitnessRelation]]
  */
private[events] sealed abstract class WitnessesTable(
    tableName: String,
    idColumn: String,
    witnessColumn: String,
) {

  private val insert =
    s"insert into $tableName($idColumn, $witnessColumn) values ({$idColumn}, {$witnessColumn})"

  final def prepareBatchInsert(witnesses: WitnessRelation[LedgerString]): Option[BatchSql] = {
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

}

private[events] object WitnessesTable {

  private[events] sealed abstract class EventWitnessesTable(tableName: String)
      extends WitnessesTable(
        tableName = tableName,
        idColumn = "event_id",
        witnessColumn = "event_witness",
      )

  /**
    * Concrete [[WitnessesTable]] to store which party can see which
    * event in a flat transaction.
    */
  private[events] object ForFlatTransactions
      extends EventWitnessesTable(
        tableName = "participant_event_flat_transaction_witnesses",
      )

  /**
    * Concrete [[WitnessesTable]] to store which party can see which
    * event in a transaction tree, diffed by the items that are going
    * to be eventually stored in [[ForFlatTransactions]]
    */
  private[events] object Complement
      extends EventWitnessesTable(
        tableName = "participant_event_witnesses_complement",
      )

  /**
    * Concrete [[WitnessesTable]] to store which party can see which
    * contract, relatively to interpretation and validation.
    */
  private[events] object ForContracts
      extends WitnessesTable(
        tableName = "participant_contract_witnesses",
        idColumn = "contract_id",
        witnessColumn = "contract_witness",
      )
}
