// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import anorm.{BatchSql, NamedParameter}
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.TransactionId
import com.daml.platform.events.EventIdFormatter.fromTransactionId

/**
  * A table storing a flattened representation of a [[DisclosureRelation]],
  * which says which [[NodeId]] is visible to which [[Party]].
  */
private[events] sealed abstract class WitnessesTable(tableName: String) {

  private def parameters(transactionId: TransactionId)(
      nodeId: NodeId,
      party: Party,
  ): Vector[NamedParameter] =
    Vector[NamedParameter](
      "event_id" -> fromTransactionId(transactionId, nodeId).toString,
      "event_witness" -> party.toString,
    )

  private val insert =
    s"insert into $tableName(event_id, event_witness) values ({event_id}, {event_witness})"

  final def prepareBatchInsert(
      offset: Offset,
      transactionId: TransactionId,
      witnesses: DisclosureRelation,
  ): Option[BatchSql] = {
    val flattenedWitnesses = DisclosureRelation.flatten(witnesses)
    if (flattenedWitnesses.nonEmpty) {
      val ws = flattenedWitnesses.map {
        case (nodeId, party) => parameters(transactionId)(nodeId, party)
      }.toSeq
      Some(BatchSql(insert, ws.head, ws.tail: _*))
    } else {
      None
    }
  }

}

private[events] object WitnessesTable {

  /**
    * Concrete [[WitnessesTable]] to store which party can see which
    * event in a flat transaction.
    */
  private[events] object ForFlatTransactions
      extends WitnessesTable(
        tableName = "participant_event_flat_transaction_witnesses",
      )

  /**
    * Concrete [[WitnessesTable]] to store which party can see which
    * event in a transaction tree, diffed by the items that are going
    * to be eventually stored in [[ForFlatTransactions]]
    */
  private[events] object Complement
      extends WitnessesTable(
        tableName = "participant_event_witnesses_complement",
      )

}
