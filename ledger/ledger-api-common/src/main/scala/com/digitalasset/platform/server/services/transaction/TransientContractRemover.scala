// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.transaction

import com.digitalasset.ledger.api.domain.ContractId
import com.digitalasset.ledger.api.v1.event.Event.Event.{Archived, Created, Empty}
import com.digitalasset.ledger.api.v1.event.{CreatedEvent, Event}
import com.digitalasset.platform.api.v1.event.EventOps._

import scala.collection.{breakOut, mutable}

object TransientContractRemover {

  /**
    * Cancels out witnesses on creates and archives that are about the same contract.
    * If no witnesses remain on either, the node is removed.
    * @param nodes Must be sorted by event index.
    * @throws IllegalArgumentException if the argument is not sorted properly.
    */
  def removeTransients(nodes: List[Event]): List[Event] = {

    val resultBuilder = new Array[Option[Event]](nodes.size)
    val creationByContractId = new mutable.HashMap[ContractId, (Int, CreatedEvent)]()

    nodes.iterator.zipWithIndex
      .foldLeft(-1) {
        case (prevEventIndex, (event, indexInList)) =>
          // Each call adds a new (possibly null) element to resultBuilder, and may update items previously added
          updateResultBuilder(resultBuilder, creationByContractId, event, indexInList)

          // This defensive code has substantial overhead, because we extract the integer event index of every element
          // from the eventId String. If we received the nodes with integer IDs, it would make this faster.
          val processedEventIndex = event.eventIndex
          if (processedEventIndex > prevEventIndex) processedEventIndex
          else failOnUnsortedInput(nodes, prevEventIndex, processedEventIndex)
      }

    resultBuilder.collect { case Some(v) if v.witnesses.nonEmpty => v }(breakOut)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def failOnUnsortedInput(
      nodes: List[Event],
      prevEventIndex: Int,
      processedEventIndex: Int): Nothing = {
    throw new IllegalArgumentException(
      s"This method requires all eventIds in its input to be in order. " +
        s"This was not true for received events ${nodes.map(_.eventId)}: $prevEventIndex appeared before $processedEventIndex")
  }

  /**
    * Update resultBuilder given the next event.
    * This will insert a new element and possibly update a previous one.
    */
  private def updateResultBuilder(
      resultBuilder: Array[Option[Event]],
      creationByContractId: mutable.HashMap[ContractId, (Int, CreatedEvent)],
      event: Event,
      indexInList: Int): Unit = {
    event.event match {

      case Created(createdEvent) =>
        if (createdEvent.witnessParties.nonEmpty) {
          resultBuilder.update(indexInList, Some(event))
          val _ = creationByContractId.put(
            ContractId(createdEvent.contractId),
            indexInList -> createdEvent)
        }

      case Archived(archivedEvent) =>
        if (archivedEvent.witnessParties.nonEmpty) {
          creationByContractId
            .get(ContractId(archivedEvent.contractId))
            .fold[Unit] {
              // No matching create for this archive. Insert as is.
              resultBuilder.update(indexInList, Some(event))
            } {
              case (createdEventIndex, createdEvent) =>
                // Defensive code to ensure that the set of parties the events are disclosed to are not different.
                if (archivedEvent.witnessParties.toSet != createdEvent.witnessParties.toSet)
                  throw new IllegalArgumentException(
                    s"Created and Archived event stakeholders are different in $createdEvent, $archivedEvent")

                resultBuilder.update(createdEventIndex, None)
                resultBuilder.update(indexInList, None)
            }
        }

      // Illegal cases
      case Empty =>
        throw new IllegalArgumentException(
          s"Received unexpected Empty event in transient contract removal. Only Create and Archive are allowed")
    }
  }
}
