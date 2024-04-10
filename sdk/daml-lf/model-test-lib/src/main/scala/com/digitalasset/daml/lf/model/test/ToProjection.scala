// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import com.daml.ledger.javaapi
import com.daml.lf.data.Ref
import com.daml.lf.model.test.Projections._
import com.daml.lf.value.{Value => V}

import scala.jdk.CollectionConverters._

object ToProjection {
  type PartyIdReverseMapping = Map[Ref.Party, PartyId]
  type ContractIdReverseMapping = Map[V.ContractId, ContractId]

  private[ToProjection] class FromCanton(
      partyIds: PartyIdReverseMapping,
      contractIds: ContractIdReverseMapping,
  ) {

    def convertFromTransactionTrees(
        trees: List[javaapi.data.TransactionTree]
    ): Projection =
      trees.map(convertFromTransactionTree)

    def convertFromTransactionTree(
        tree: javaapi.data.TransactionTree
    ): Commands = {
      Commands(
        tree.getRootEventIds.asScala.toList
          .map(convertFromEventId(tree.getEventsById.asScala.toMap, _))
      )
    }

    def convertFromEventId(
        eventsById: Map[String, javaapi.data.TreeEvent],
        eventId: String,
    ): Action = {
      eventsById(eventId) match {
        case create: javaapi.data.CreatedEvent =>
          Create(
            contractId = convertFromContractId(create.getContractId),
            signatories = convertFromPartyIds(create.getSignatories),
          )
        case exercise: javaapi.data.ExercisedEvent =>
          Exercise(
            kind = if (exercise.isConsuming) Consuming else NonConsuming,
            contractId = convertFromContractId(exercise.getContractId),
            controllers = convertFromPartyIds(exercise.getActingParties),
            subTransaction = exercise.getChildEventIds.asScala.toList
              .map(convertFromEventId(eventsById, _)),
          )
        case event =>
          throw new IllegalArgumentException(s"Unsupported event type: $event")
      }
    }

    def convertFromContractId(contractId: String): ContractId =
      contractIds(V.ContractId.assertFromString(contractId))

    def convertFromPartyId(partyId: String): PartyId =
      partyIds(Ref.Party.assertFromString(partyId))

    def convertFromPartyIds(partyIds: java.lang.Iterable[String]): PartySet =
      partyIds.asScala.map(convertFromPartyId).toSet
  }

  def convertFromCanton(
      partyIds: PartyIdReverseMapping,
      contractIds: ContractIdReverseMapping,
      trees: List[javaapi.data.TransactionTree],
  ): Projection =
    new FromCanton(partyIds, contractIds).convertFromTransactionTrees(trees)
}
