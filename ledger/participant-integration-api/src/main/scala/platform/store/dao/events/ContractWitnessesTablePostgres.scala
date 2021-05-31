// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection

import anorm.{Row, SimpleSql, SqlQuery}
import com.daml.platform.store.dao.events.ContractWitnessesTable.Executable

object ContractWitnessesTablePostgres extends ContractWitnessesTable {

  val witnessesContractIdsParam = "witnessesContractIds"
  val partiesParam = "parties"

  private val insertWitnessesQuery: SqlQuery =
    anorm.SQL(s"""insert into $TableName($IdColumn, $WitnessColumn)
       |            select $IdColumn, $WitnessColumn
       |            from unnest({$witnessesContractIdsParam}, {$partiesParam}) as t($IdColumn, $WitnessColumn)
       |            on conflict do nothing;""".stripMargin)

  override def toExecutables(
      info: TransactionIndexing.ContractWitnessesInfo
  ): ContractWitnessesTable.Executables =
    ContractWitnessesTable.Executables(
      deleteWitnesses = prepareBatchDelete(info.netArchives.toList),
      insertWitnesses = buildInsertExecutable(info),
    )

  private def buildInsertExecutable(
      witnesses: TransactionIndexing.ContractWitnessesInfo
  ): Executable = {
    import com.daml.platform.store.JdbcArrayConversions._

    val flattened: Iterator[(ContractId, String)] = Relation.flatten(witnesses.netVisibility)
    val (witnessesContractIds, parties) = flattened
      .map { case (id, party) =>
        id.coid -> party
      }
      .toArray
      .unzip

    val inserts = insertWitnessesQuery.on(
      witnessesContractIdsParam -> witnessesContractIds,
      partiesParam -> parties,
    )

    new InsertExecutable(inserts)
  }

  private class InsertExecutable(insertQuery: SimpleSql[Row]) extends Executable {
    override def execute()(implicit connection: Connection): Unit = {
      insertQuery.executeUpdate()
      ()
    }
  }
}
