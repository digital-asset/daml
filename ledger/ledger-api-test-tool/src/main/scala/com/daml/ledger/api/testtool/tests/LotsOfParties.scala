// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import com.digitalasset.ledger.api.v1.event.CreatedEvent
import com.digitalasset.ledger.api.v1.transaction.Transaction
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
      alpha <- context.participant()
      beta <- context.participant()
      giver <- alpha.allocateParty()
      alphaObservers <- alpha.allocateParties(partyCount / 2 - 1)
      betaObservers <- beta.allocateParties(partyCount / 2)
      observers = alphaObservers ++ betaObservers
      alphaParties = giver +: alphaObservers
      contractId <- alpha.create(giver, WithObservers(giver, observers))
      alphaTransactionsByParty <- transactionsForEachParty(alpha, alphaParties)
      betaTransactionsByParty <- transactionsForEachParty(beta, betaObservers)
    } yield {
      assertWitnessesOfSinglePartySubscriptions(
        alphaParties.toSet,
        contractId,
        activeContractsFrom(alphaTransactionsByParty))
      assertWitnessesOfSinglePartySubscriptions(
        betaObservers.toSet,
        contractId,
        activeContractsFrom(betaTransactionsByParty))
    }
  }

  private[this] val seeTransactionsInSingleMultiPartySubscription = LedgerTest(
    "LOPseeTransactionsInSingleMultiPartySubscription",
    "Observers should see transactions in a single multi-party subscription"
  ) { context =>
    for {
      alpha <- context.participant()
      beta <- context.participant()
      giver <- alpha.allocateParty()
      alphaObservers <- alpha.allocateParties(partyCount / 2 - 1)
      betaObservers <- beta.allocateParties(partyCount / 2)
      observers = alphaObservers ++ betaObservers
      alphaParties = giver +: alphaObservers
      contractId <- alpha.create(giver, WithObservers(giver, observers))
      alphaTransactions <- alpha.flatTransactions(alphaParties: _*)
      betaTransactions <- beta.flatTransactions(betaObservers: _*)
    } yield {
      assertWitnessesOfAMultiPartySubscription(
        alphaParties.toSet,
        contractId,
        activeContractsFrom(alphaTransactions))
      assertWitnessesOfAMultiPartySubscription(
        betaObservers.toSet,
        contractId,
        activeContractsFrom(betaTransactions))
    }
  }

  private[this] val seeActiveContractsInMultipleSinglePartySubscriptions = LedgerTest(
    "LOPseeActiveContractsInMultipleSinglePartySubscriptions",
    "Observers should see active contracts in multiple single-party subscriptions"
  ) { context =>
    for {
      alpha <- context.participant()
      beta <- context.participant()
      giver <- alpha.allocateParty()
      alphaObservers <- alpha.allocateParties(partyCount / 2 - 1)
      betaObservers <- beta.allocateParties(partyCount / 2)
      observers = alphaObservers ++ betaObservers
      alphaParties = giver +: alphaObservers
      contractId <- alpha.create(giver, WithObservers(giver, observers))
      alphaContractsByParty <- activeContractsForEachParty(alpha, alphaParties)
      betaContractsByParty <- activeContractsForEachParty(beta, betaObservers)
    } yield {
      assertWitnessesOfSinglePartySubscriptions(
        alphaParties.toSet,
        contractId,
        alphaContractsByParty)
      assertWitnessesOfSinglePartySubscriptions(
        betaObservers.toSet,
        contractId,
        betaContractsByParty)
    }
  }

  private[this] val seeActiveContractsInSingleMultiPartySubscription = LedgerTest(
    "LOPseeActiveContractsInSingleMultiPartySubscription",
    "Observers should see active contracts in a single multi-party subscription"
  ) { context =>
    for {
      alpha <- context.participant()
      beta <- context.participant()
      giver <- alpha.allocateParty()
      alphaObservers <- alpha.allocateParties(partyCount / 2 - 1)
      betaObservers <- beta.allocateParties(partyCount / 2)
      observers = alphaObservers ++ betaObservers
      alphaParties = giver +: alphaObservers
      contractId <- alpha.create(giver, WithObservers(giver, observers))
      alphaContracts <- alpha.activeContracts(alphaParties: _*)
      betaContracts <- beta.activeContracts(betaObservers: _*)
    } yield {
      assertWitnessesOfAMultiPartySubscription(alphaParties.toSet, contractId, alphaContracts)
      assertWitnessesOfAMultiPartySubscription(betaObservers.toSet, contractId, betaContracts)
    }
  }

  override val tests: Vector[LedgerTest] =
    Vector(
      seeTransactionsInMultipleSinglePartySubscriptions,
      seeTransactionsInSingleMultiPartySubscription,
      seeActiveContractsInMultipleSinglePartySubscriptions,
      seeActiveContractsInSingleMultiPartySubscription,
    )

  private def transactionsForEachParty(
      ledger: ParticipantTestContext,
      observers: Vector[Party]): Future[PartyMap[Vector[Transaction]]] = {
    Future
      .sequence(observers.map(observer => ledger.flatTransactions(observer).map(observer -> _)))
      .map(_.toMap)
  }

  private def activeContractsForEachParty(
      ledger: ParticipantTestContext,
      observers: Vector[Party]): Future[PartyMap[Vector[CreatedEvent]]] = {
    Future
      .sequence(observers.map(observer => ledger.activeContracts(observer).map(observer -> _)))
      .map(_.toMap)
  }

  private def activeContractsFrom(
      transactionsByParty: PartyMap[Vector[Transaction]]): PartyMap[Vector[CreatedEvent]] = {
    transactionsByParty.mapValues(transactions => activeContractsFrom(transactions))
  }

  private def activeContractsFrom(transactions: Vector[Transaction]): Vector[CreatedEvent] = {
    transactions.map(transaction => {
      val event = assertSingleton("single transaction event", transaction.events)
      event.event.created.get
    })
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
