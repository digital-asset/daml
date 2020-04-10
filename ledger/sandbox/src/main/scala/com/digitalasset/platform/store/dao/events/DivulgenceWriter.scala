// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import anorm.BatchSql
import com.daml.platform.store.DbType
//import com.daml.platform.store.Conversions._

/**
  * @see [[WitnessesTable.ForContracts]]
  */
private[dao] final class DivulgenceWriter(dbType: DbType) {

//  private val insert =
//    dbType match {
//      case DbType.Postgres => DivulgenceWriter.postgresqlInsert
//      case DbType.H2Database => DivulgenceWriter.h2DatabaseInsert
//    }

  def prepareBatchInsert(witnesses: WitnessRelation[ContractId]): Option[BatchSql] = {
//    val flattenedWitnesses = Relation.flatten(witnesses)
//    if (flattenedWitnesses.nonEmpty) {
//      val ws = flattenedWitnesses.map {
//        case (id, party) =>
//          Vector[NamedParameter]("contract_id" -> id.coid, "contract_witness" -> party)
//      }.toSeq
//      Some(BatchSql(insert, ws.head, ws.tail: _*))
//    } else {
//      None
//    }
    None
  }

}

object DivulgenceWriter {

  private val postgresqlInsert: String =
    "insert into participant_contract_witnesses(contract_id, contract_witness) values({contract_id}, {contract_witness}) on conflict on constraint participant_contract_witnesses_contract_id_fkey do nothing"

  private val h2DatabaseInsert: String =
    "merge into participant_contract_witnesses using dual on contract_id = {contract_id} and contract_witness = {contract_witness} when not matched then insert (contract_id, contract_witness) values ({contract_id}, {contract_witness})"

}
