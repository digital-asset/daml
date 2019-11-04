// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import com.digitalasset.ledger.api.v1.event.CreatedEvent
import com.digitalasset.ledger.client.binding.Primitive.{ContractId, Party}
import com.digitalasset.ledger.test_stable.Test.WithObservers

import scala.concurrent.Future

final class LotsOfParties(session: LedgerSession) extends LedgerTestSuite(session) {
  type Parties = Set[Party]
  type PartyMap[T] = Map[Party, T]

  private val partyCount = 1024

  private[this] val seeTransactionsInMultipleSinglePartySubscriptions = LedgerTest(
    "LOPseeTransactionsInMultipleSinglePartySubscriptions",
    "Observers should see transactions in multiple single-party subscriptions"
  ) { context =>
    for {
      ledger <- context.participant()
      giver <- ledger.allocateParty()
      observers <- ledger.allocateParties(partyCount)
      contractId <- ledger.create(giver, WithObservers(giver, observers))
      transactionsByParty <- Future
        .sequence(observers.map(observer =>
          ledger.flatTransactions(observer).map(transactions => observer -> transactions)))
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
      ledger <- context.participant()
      giver <- ledger.allocateParty()
      observers <- ledger.allocateParties(partyCount)
      contractId <- ledger.create(giver, WithObservers(giver, observers))
      transactions <- ledger.flatTransactions(observers: _*)
    } yield {
      val activeContracts =
        transactions.map(transaction => {
          val event =
            assertSingleton("LOPseeTransactionsInSingleMultiPartySubscription", transaction.events)
          event.event.created.get
        })

      assertWitnessesOfAMultiPartySubscription(observers.toSet, contractId, activeContracts)
    }
  }

  private[this] val seeActiveContractsInMultipleSinglePartySubscriptions = LedgerTest(
    "LOPseeActiveContractsInMultipleSinglePartySubscriptions",
    "Observers should see active contracts in multiple single-party subscriptions"
  ) { context =>
    for {
      ledger <- context.participant()
      giver <- ledger.allocateParty()
      observers <- ledger.allocateParties(partyCount)
      contractId <- ledger.create(giver, WithObservers(giver, observers))
      activeContracts <- Future
        .sequence(observers.map(observer =>
          ledger.activeContracts(observer).map(activeContracts => observer -> activeContracts)))
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
      ledger <- context.participant()
      giver <- ledger.allocateParty()
      observers <- ledger.allocateParties(partyCount)
      contractId <- ledger.create(giver, WithObservers(giver, observers))
      activeContracts <- ledger.activeContracts(observers: _*)
    } yield {
      assertWitnessesOfAMultiPartySubscription(observers.toSet, contractId, activeContracts)
    }
  }

  override val tests: Vector[LedgerTest] =
    Vector(
      seeTransactionsInMultipleSinglePartySubscriptions,
      seeTransactionsInSingleMultiPartySubscription,
      seeActiveContractsInMultipleSinglePartySubscriptions,
      seeActiveContractsInSingleMultiPartySubscription,
    )

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
