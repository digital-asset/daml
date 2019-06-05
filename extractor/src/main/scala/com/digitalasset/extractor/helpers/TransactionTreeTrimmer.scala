// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.helpers

import com.digitalasset.ledger.api.v1.transaction.TreeEvent.Kind
import com.digitalasset.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
import com.digitalasset.ledger.api.v1.value.Identifier

object TransactionTreeTrimmer {
  def trim(templateIds: Set[Identifier]): TransactionTree => TransactionTree = {
    val shouldKeep: TreeEvent.Kind => Boolean = containsTemplateId(templateIds.map(asTuple))
    transactionTree: TransactionTree =>
      {
        val eventsById = transactionTree.eventsById.filter(kv => shouldKeep(kv._2.kind))
        val eventIds = eventsById.keySet
        val rootEventIds = transactionTree.rootEventIds.filter(eventIds)
        transactionTree.copy(eventsById = eventsById, rootEventIds = rootEventIds)
      }
  }

  private def containsTemplateId(
      templateIds: Set[(String, String, String)]): TreeEvent.Kind => Boolean = {
    case Kind.Created(event) => contains(templateIds)(event.templateId.map(asTuple))
    case Kind.Exercised(event) => contains(templateIds)(event.templateId.map(asTuple))
    case Kind.Empty => false
  }

  private def contains[A](as: Set[A])(o: Option[A]): Boolean =
    o.exists(as)

  private def asTuple(a: Identifier): (String, String, String) =
    (a.packageId, a.moduleName, a.entityName)
}
