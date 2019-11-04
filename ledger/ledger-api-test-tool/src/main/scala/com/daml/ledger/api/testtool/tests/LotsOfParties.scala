// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{
  LedgerSession,
  LedgerTest,
  LedgerTestContext,
  LedgerTestSuite
}
import com.digitalasset.ledger.api.v1.event.CreatedEvent
import com.digitalasset.ledger.client.binding.Primitive.{ContractId, Party}
import com.digitalasset.ledger.test_stable.Test.WithObservers

import scala.concurrent.Future

final class LotsOfParties(session: LedgerSession) extends LedgerTestSuite(session) {
  type Parties = Set[Party]
  type PartyMap[T] = Map[Party, T]

  private val participantCount = 2
  private val partyCount = 1024

  private[this] val seeTransactionsInMultipleSinglePartySubscriptions = LedgerTest(
    "LOPseeTransactionsInMultipleSinglePartySubscriptions",
    "Observers should see transactions in multiple single-party subscriptions"
  ) { context =>
    for {
      partiesByLedger <- allocatePartiesAcrossParticipants(context)
      parties = partiesByLedger.toVector.flatMap {
        case (ledger, parties) => parties.map(party => ledger -> party)
      }
      (giverLedger, giver) = parties.head
      observersWithLedgers = parties.drop(1)
      observers = observersWithLedgers.map(_._2)
      contractId <- giverLedger.create(giver, WithObservers(giver, observers))
      transactionsByParty <- Future
        .sequence(observersWithLedgers.map {
          case (ledger, observer) =>
            ledger.flatTransactions(observer).map(observer -> _)
        })
        .map(_.toMap)
    } yield {
      val activeContracts = transactionsByParty.mapValues(_.map(transaction => {
        val event = assertSingleton(
          "LOPseeTransactionsInMultipleSinglePartySubscriptions",
          transaction.events)
        event.event.created.get
      }))

      assertWitnessesOfSinglePartySubscriptions(observers.toSet, contractId, activeContracts)
    }
  }

  private[this] val seeTransactionsInSingleMultiPartySubscription = LedgerTest(
    "LOPseeTransactionsInSingleMultiPartySubscription",
    "Observers should see transactions in a single multi-party subscription"
  ) { context =>
    for {
      partiesByLedger <- allocatePartiesAcrossParticipants(context)
      giverLedger = partiesByLedger.head._1
      giver = partiesByLedger(giverLedger).head
      observers = partiesByLedger.values.flatten.filterNot(_ == giver).toVector
      contractId <- giverLedger.create(giver, WithObservers(giver, observers))
      transactionsByLedger <- Future.sequence(partiesByLedger.map {
        case (ledger, parties) =>
          ledger.flatTransactions(parties: _*).map(parties -> _)
      })
    } yield {
      transactionsByLedger.foreach {
        case (parties, transactions) =>
          val activeContracts =
            transactions.map(transaction => {
              val event =
                assertSingleton(
                  "LOPseeTransactionsInSingleMultiPartySubscription",
                  transaction.events)
              event.event.created.get
            })

          assertWitnessesOfAMultiPartySubscription(parties.toSet, contractId, activeContracts)
      }
    }
  }

  private[this] val seeActiveContractsInMultipleSinglePartySubscriptions = LedgerTest(
    "LOPseeActiveContractsInMultipleSinglePartySubscriptions",
    "Observers should see active contracts in multiple single-party subscriptions"
  ) { context =>
    for {
      partiesByLedger <- allocatePartiesAcrossParticipants(context)
      parties = partiesByLedger.toVector.flatMap {
        case (ledger, parties) => parties.map(party => ledger -> party)
      }
      (giverLedger, giver) = parties.head
      observersWithLedgers = parties.drop(1)
      observers = observersWithLedgers.map(_._2)
      contractId <- giverLedger.create(giver, WithObservers(giver, observers))
      activeContracts <- Future
        .sequence(observersWithLedgers.map {
          case (ledger, observer) =>
            ledger.activeContracts(observer).map(observer -> _)
        })
        .map(_.toMap)
    } yield {
      assertWitnessesOfSinglePartySubscriptions(observers.toSet, contractId, activeContracts)
    }
  }

  private[this] val seeActiveContractsInSingleMultiPartySubscription = LedgerTest(
    "LOPseeActiveContractsInSingleMultiPartySubscription",
    "Observers should see active contracts in a single multi-party subscription"
  ) { context =>
    for {
      partiesByLedger <- allocatePartiesAcrossParticipants(context)
      giverLedger = partiesByLedger.head._1
      giver = partiesByLedger(giverLedger).head
      observers = partiesByLedger.values.flatten.filterNot(_ == giver).toVector
      contractId <- giverLedger.create(giver, WithObservers(giver, observers))
      activeContractsByLedger <- Future.sequence(partiesByLedger.map {
        case (ledger, parties) =>
          ledger.activeContracts(parties: _*).map(parties -> _)
      })
    } yield {
      activeContractsByLedger.foreach {
        case (parties, activeContracts) =>
          assertWitnessesOfAMultiPartySubscription(parties.toSet, contractId, activeContracts)
      }
    }
  }

  override val tests: Vector[LedgerTest] =
    Vector(
      seeTransactionsInMultipleSinglePartySubscriptions,
      seeTransactionsInSingleMultiPartySubscription,
      seeActiveContractsInMultipleSinglePartySubscriptions,
      seeActiveContractsInSingleMultiPartySubscription,
    )

  private def allocatePartiesAcrossParticipants(
      context: LedgerTestContext): Future[Map[ParticipantTestContext, Vector[Party]]] = {
    for {
      ledgers <- context.participants(participantCount)
      partiesWithLedgers <- Future.sequence(ledgers.map(ledger =>
        ledger.allocateParties(partyCount / participantCount).map(parties => ledger -> parties)))
    } yield partiesWithLedgers.toMap
  }

  private def assertWitnessesOfSinglePartySubscriptions(
      observers: Set[Party],
      contractId: ContractId[WithObservers],
      activeContracts: PartyMap[Seq[CreatedEvent]]
  ): Unit = {
    val expectedContractIds: PartyMap[Seq[ContractId[WithObservers]]] =
      observers.map(observer => observer -> Seq(contractId)).toMap
    val actualContractIds: PartyMap[Seq[ContractId[WithObservers]]] =
      activeContracts.mapValues(_.map(e => ContractId(e.contractId)))
    assertEquals("single-party contract IDs", actualContractIds, expectedContractIds)

    val expectedWitnesses: Set[Seq[Parties]] =
      observers.map(observer => Seq(Set(observer)))
    val actualWitnesses: Set[Seq[Parties]] =
      activeContracts.values.map(_.map(_.witnessParties.map(Party(_)).toSet)).toSet
    assertEquals("single-party witnesses", actualWitnesses, expectedWitnesses)
  }

  private def assertWitnessesOfAMultiPartySubscription(
      observers: Set[Party],
      contractId: ContractId[WithObservers],
      activeContracts: Seq[CreatedEvent]
  ): Unit = {
    val expectedContractIds: Seq[ContractId[WithObservers]] =
      Seq(contractId)
    val actualContractIds: Seq[ContractId[WithObservers]] =
      activeContracts.map(e => ContractId(e.contractId))
    assertEquals("multi-party contract IDs", actualContractIds, expectedContractIds)

    val expectedWitnesses: Seq[Parties] =
      Seq(observers)
    val actualWitnesses: Seq[Parties] =
      activeContracts.map(_.witnessParties.map(Party(_)).toSet)
    assertEquals("multi-party witnesses", actualWitnesses, expectedWitnesses)
  }
}
