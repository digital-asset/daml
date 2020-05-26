// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package db.migration.postgres.v25_backfill_participant_events

import anorm.{BatchSql, NamedParameter}
import com.daml.platform.store.Conversions._

// Copied here to make it safe against future refactoring
// in production code
/**
  * A table storing a flattened representation of a [[DisclosureRelation]],
  * which says which [[NodeId]] is visible to which [[Party]].
  */
private[v25_backfill_participant_events] sealed abstract class V25WitnessesTable(
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

private[v25_backfill_participant_events] object V25WitnessesTable {

  private[v25_backfill_participant_events] sealed abstract class V25EventWitnessesTable(
      tableName: String)
      extends V25WitnessesTable(
        tableName = tableName,
        idColumn = "event_id",
        witnessColumn = "event_witness",
      )

  /**
    * Concrete [[WitnessesTable]] to store which party can see which
    * event in a flat transaction.
    */
  private[v25_backfill_participant_events] object ForFlatTransactions
      extends V25EventWitnessesTable(
        tableName = "participant_event_flat_transaction_witnesses",
      )

  /**
    * Concrete [[WitnessesTable]] to store which party can see which
    * event in a transaction tree, diffed by the items that are going
    * to be eventually stored in [[ForFlatTransactions]]
    */
  private[v25_backfill_participant_events] object Complement
      extends V25EventWitnessesTable(
        tableName = "participant_event_witnesses_complement",
      )
}
