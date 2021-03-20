// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection

import anorm.{BatchSql, NamedParameter}
import com.daml.platform.store.Conversions._
import com.daml.platform.store.dao.events.ContractWitnessesTable.Executable

object ContractWitnessesTableOracle extends ContractWitnessesTable {
  private val insert: String =
    s"""merge into $TableName using dual 
       | on ($IdColumn = {$IdColumn} and $WitnessColumn = {$WitnessColumn})
       | when not matched then
       | insert ($IdColumn, $WitnessColumn)
       | values ({$IdColumn}, {$WitnessColumn})""".stripMargin

  override def toExecutables(
      info: TransactionIndexing.ContractWitnessesInfo
  ): ContractWitnessesTable.Executables =
    ContractWitnessesTable.Executables(
      deleteWitnesses = prepareBatchDelete(info.netArchives.toList),
      insertWitnesses = prepareInserts(info.netVisibility),
    )

  private def toNamedParameter(idAndParty: (ContractId, Party)) =
    idAndParty match {
      case (id, party) =>
        List[NamedParameter](
          IdColumn -> id,
          WitnessColumn -> party,
        )
    }

  private def prepareInserts(witnesses: WitnessRelation[ContractId]): Executable = {
    val batchInserts = batch(insert, Relation.flatten(witnesses).map(toNamedParameter).toSeq)
    new InsertsExecutable(batchInserts)
  }

  private class InsertsExecutable(inserts: Option[BatchSql]) extends Executable {
    override def execute()(implicit connection: Connection): Unit = inserts.foreach(_.execute())
  }
}
