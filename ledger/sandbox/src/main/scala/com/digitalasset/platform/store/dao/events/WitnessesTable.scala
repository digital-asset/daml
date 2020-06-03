// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import anorm.{BatchSql, NamedParameter, ToStatement}
import com.daml.ledger.EventId
import com.daml.platform.store.Conversions._
import com.daml.platform.store.DbType

/**
  * A table storing a flattened representation of a [[WitnessRelation]]
  */
private[events] sealed abstract class WitnessesTable[Id: ToStatement](
    protected val tableName: String,
    protected val idColumn: String,
    protected val witnessColumn: String,
) {

  protected val insert: String

  case class AccumulatingBatches private[WitnessesTable] (
      insertions: WitnessRelation[Id],
      deletions: Set[Id],
  ) {
    def add(newInsertions: WitnessRelation[Id]): AccumulatingBatches = {
      copy(
        insertions = Relation.union(insertions, newInsertions)
      )
    }

    def add(newInsertions: WitnessRelation[Id], newDeletions: Set[Id]): AccumulatingBatches = {
      val totalDeletions = deletions.union(newDeletions)
      val totalInsertions =
        Relation.union(insertions, newInsertions).filterKeys(id => !totalDeletions.contains(id))
      copy(
        insertions = totalInsertions,
        deletions = totalDeletions,
      )
    }

    private def prepareBatchInsert(witnesses: WitnessRelation[Id]): Option[BatchSql] = {
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

    private val delete = s"delete from $tableName where $idColumn = {$idColumn}"

    private def prepareBatchDelete(ids: Seq[Id]): Option[BatchSql] = {
      if (ids.nonEmpty) {
        val parameters = ids.map(id => Vector[NamedParameter](idColumn -> id))
        Some(BatchSql(delete, parameters.head, parameters.tail: _*))
      } else {
        None
      }
    }

    def prepare: PreparedBatches = new PreparedBatches(
      insertions = prepareBatchInsert(insertions),
      deletions = prepareBatchDelete(deletions.toSeq),
    )
  }

  object AccumulatingBatches {
    def empty: AccumulatingBatches = AccumulatingBatches(Map.empty, Set.empty)
  }

  case class PreparedBatches private[WitnessesTable] (
      insertions: Option[BatchSql],
      deletions: Option[BatchSql],
  ) {}
}

private[events] object WitnessesTable {

  private[events] sealed abstract class EventWitnessesTable(tableName: String)
      extends WitnessesTable[EventId](
        tableName = tableName,
        idColumn = "event_id",
        witnessColumn = "event_witness",
      ) {
    protected val insert =
      s"insert into $tableName($idColumn, $witnessColumn) values ({$idColumn}, {$witnessColumn})"
  }

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
    * event in a transaction tree.
    */
  private[events] object ForTransactionTrees
      extends EventWitnessesTable(
        tableName = "participant_event_transaction_tree_witnesses",
      )

  /**
    * Concrete [[WitnessesTable]] to store which party can see which
    * contract, relatively to interpretation and validation.
    */
  private[events] sealed abstract class ForContracts
      extends WitnessesTable[ContractId](
        tableName = "participant_contract_witnesses",
        idColumn = "contract_id",
        witnessColumn = "contract_witness",
      )

  private[events] object ForContracts {

    def apply(dbType: DbType): ForContracts =
      dbType match {
        case DbType.Postgres => Postgresql
        case DbType.H2Database => H2Database
      }

    private object Postgresql extends ForContracts {
      override protected val insert: String =
        s"insert into $tableName($idColumn, $witnessColumn) values ({$idColumn}, {$witnessColumn}) on conflict do nothing"
    }

    private object H2Database extends ForContracts {
      override protected val insert: String =
        s"merge into $tableName using dual on $idColumn = {$idColumn} and $witnessColumn = {$witnessColumn} when not matched then insert ($idColumn, $witnessColumn) values ({$idColumn}, {$witnessColumn})"
    }

  }

}
