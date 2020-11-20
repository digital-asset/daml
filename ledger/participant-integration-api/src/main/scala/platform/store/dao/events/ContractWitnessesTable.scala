// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import anorm.{BatchSql, NamedParameter}
import com.daml.platform.store.Conversions._
import com.daml.platform.store.DbType

private[events] sealed abstract class ContractWitnessesTable {

  protected val insert: String

  protected val TableName = "participant_contract_witnesses"
  protected val IdColumn = "contract_id"
  protected val WitnessColumn = "contract_witness"

  private def toNamedParameter(idAndParty: (ContractId, Party)) =
    idAndParty match {
      case (id, party) =>
        List[NamedParameter](
          IdColumn -> id,
          WitnessColumn -> party,
        )
    }

  private def prepareBatchInsert(witnesses: WitnessRelation[ContractId]): Option[BatchSql] =
    batch(insert, Relation.flatten(witnesses).map(toNamedParameter).toSeq)

  protected val delete = s"delete from $TableName where $IdColumn = {$IdColumn}"

  private def prepareBatchDelete(ids: Seq[ContractId]): Option[BatchSql] =
    batch(delete, ids.map(id => List[NamedParameter](IdColumn -> id)))

  def toExecutables(
      info: TransactionIndexing.ContractWitnessesInfo,
  ): ContractWitnessesTable.Executables = {
    ContractWitnessesTable.Executables(
      deleteWitnesses = prepareBatchDelete(info.netArchives.toList),
      insertWitnesses = prepareBatchInsert(info.netVisibility),
    )
  }

}

private[events] object ContractWitnessesTable {

  final case class Executables(deleteWitnesses: Option[BatchSql], insertWitnesses: Option[BatchSql])

  def apply(dbType: DbType): ContractWitnessesTable =
    dbType match {
      case DbType.Postgres => Postgresql
      case DbType.H2Database => H2Database
    }

  private object Postgresql extends ContractWitnessesTable {
    override protected val insert: String =
      s"insert into $TableName($IdColumn, $WitnessColumn) values ({$IdColumn}, {$WitnessColumn}) on conflict do nothing"
  }

  private object H2Database extends ContractWitnessesTable {
    override protected val insert: String =
      s"merge into $TableName using dual on $IdColumn = {$IdColumn} and $WitnessColumn = {$WitnessColumn} when not matched then insert ($IdColumn, $WitnessColumn) values ({$IdColumn}, {$WitnessColumn})"
  }

}
