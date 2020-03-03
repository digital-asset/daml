// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.transaction

import com.digitalasset.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.digitalasset.ledger.api.v1.event.Event.Event.{Archived, Created, Empty}
import com.digitalasset.platform.api.v1.event.EventOps.EventOps

import scala.collection.{breakOut, mutable}

object TransientContractRemover {

  /**
    * Cancels out witnesses on creates and archives that are about the same contract.
    * If no witnesses remain on either, the node is removed.
    *
    * @param nodes Must be sorted by event index.
    * @throws IllegalArgumentException if the argument is not sorted properly.
    */
  def removeTransients(nodes: Vector[Event]): Vector[Event] = {

    val resultBuilder = new Array[Option[Event]](nodes.size)
    val creationByContractId = new mutable.HashMap[String, (Int, Event)]()

    nodes.zipWithIndex.foreach {
      case (event, indexInList) =>
        // Each call adds a new (possibly null) element to resultBuilder, and may update items previously added
        updateResultBuilder(resultBuilder, creationByContractId, event, indexInList)
    }

    resultBuilder.collect {
      case Some(v) if v.witnessParties.nonEmpty => v
    }(breakOut)
  }

  /**
    * Update resultBuilder given the next event.
    * This will insert a new element and possibly update a previous one.
    */
  private def updateResultBuilder(
      resultBuilder: Array[Option[Event]],
      creationByContractId: mutable.HashMap[String, (Int, Event)],
      event: Event,
      indexInList: Int
  ): Unit =
    event match {
      case createdEvent @ Event(
            Created(CreatedEvent(_, contractId, _, _, witnessParties, _, _, _, _))) =>
        if (witnessParties.nonEmpty) {
          resultBuilder.update(indexInList, Some(event))
          val _ = creationByContractId.put(contractId, indexInList -> createdEvent)
        }
      case archivedEvent @ Event(Archived(ArchivedEvent(_, contractId, _, witnessParties))) =>
        if (witnessParties.nonEmpty) {
          creationByContractId
            .get(contractId)
            .fold[Unit] {
              // No matching create for this archive. Insert as is.
              resultBuilder.update(indexInList, Some(event))
            } {
              case (createdEventIndex, createdEvent) =>
                // Defensive code to ensure that the set of parties the events are disclosed to are not different.
                if (witnessParties.toSet != createdEvent.witnessParties.toSet)
                  throw new IllegalArgumentException(
                    s"Created and Archived event stakeholders are different in $createdEvent, $archivedEvent")

                resultBuilder.update(createdEventIndex, None)
                resultBuilder.update(indexInList, None)
            }
        }
      case Event(Empty) =>
        throw new IllegalArgumentException("Empty event")
    }

}
