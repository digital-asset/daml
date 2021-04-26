// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection

import anorm.{BatchSql, NamedParameter}
import com.daml.platform.store.Conversions._
import com.daml.platform.store.DbType

private[events] abstract class ContractWitnessesTable {

  protected val TableName = "participant_contract_witnesses"
  protected val IdColumn = "contract_id"
  protected val WitnessColumn = "contract_witness"

  private val delete = s"delete from $TableName where $IdColumn = {$IdColumn}"

  protected def prepareBatchDelete(ids: Seq[ContractId]): Option[BatchSql] =
    batch(delete, ids.map(id => List[NamedParameter](IdColumn -> id)))

  def toExecutables(
      info: TransactionIndexing.ContractWitnessesInfo
  ): ContractWitnessesTable.Executables
}

private[events] object ContractWitnessesTable {

  trait Executable {
    def execute()(implicit connection: Connection): Unit
  }

  final case class Executables(deleteWitnesses: Option[BatchSql], insertWitnesses: Executable)

  def apply(dbType: DbType): ContractWitnessesTable =
    dbType match {
      case DbType.Postgres => ContractWitnessesTablePostgres
      case DbType.H2Database => ContractWitnessesTableH2
      case DbType.Oracle => ContractWitnessesTableOracle
    }

}
