// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.util

import com.daml.ledger.api.v2.transaction.TreeEvent.Kind.{Created, Empty, Exercised}
import com.daml.ledger.api.v2.transaction.{TransactionTree, TreeEvent}
import com.daml.ledger.api.v2.value.Map as _

object LedgerApiUtil {

  /** @throws java.lang.IllegalArgumentException if the given `transactionTree` contains an empty node
    */
  def contractIdsOfCreateNodesInExecutionOrder(transactionTree: TransactionTree): Seq[String] = {
    def createIdsOfEvent(event: TreeEvent): Seq[String] = event.kind match {
      case Created(createdEvent) => Seq(createdEvent.contractId)
      case Exercised(exercisedEvent) => createIdsOfEventIds(exercisedEvent.childEventIds)
      case Empty =>
        throw new IllegalArgumentException("Found a transaction tree with an empty event.")
    }

    def createIdsOfEventIds(eventIds: Seq[String]): Seq[String] =
      eventIds.foldLeft(Seq.empty[String]) { case (acc, id) =>
        acc ++ createIdsOfEvent(transactionTree.eventsById(id))
      }

    createIdsOfEventIds(transactionTree.rootEventIds)
  }
}
