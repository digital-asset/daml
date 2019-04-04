// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import com.digitalasset.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
import scala.collection.breakOut
import com.digitalasset.platform.api.v1.event.EventOps._

trait TransactionTreeDebugging {

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  protected def prettyPrint(tree: TransactionTree): Unit = {
    println(s"-- TX ${tree.transactionId} --")
    println(s"Roots: ${tree.rootEventIds.mkString("[", ",", "]")}")
    printEvents(0, tree.rootEventIds, tree.eventsById).foreach(println)
    println("")
  }

  private def printEvents(
      indentLevel: Int,
      events: Seq[String],
      lookup: Map[String, TreeEvent]): List[String] = {
    val prefix = "\t" * indentLevel
    events
      .map(lookup)
      .flatMap { event =>
        event.kind.fold(
          ex =>
            s"$prefix${ex.eventId}: ${ex.witnessParties.mkString("[", ", ", "]")}" :: printEvents(
              indentLevel + 1,
              ex.childEventIds,
              lookup),
          cr => List(s"$prefix${cr.eventId}: ${cr.witnessParties.mkString("[", ", ", "]")}")
        )
      }(breakOut)
  }
}
