// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}
import com.digitalasset.ledger.api.v1.event.CreatedEvent
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.client.binding.Primitive.{ContractId, Party}
import com.digitalasset.ledger.test_stable.Test.WithObservers

import scala.concurrent.Future

final class LotsOfParties(session: LedgerSession) extends LedgerTestSuite(session) {
  type Parties = Set[Party]
  type PartyMap[T] = Map[Party, T]

  // Can be set to 1024 parties again after fixing
  // https://github.com/digital-asset/daml/issues/3747
  private val partyCount = 125

  private val allocation: ParticipantAllocation =
    allocate(Parties(partyCount / 2), Parties(partyCount / 2))

  // allocating parties seems to be really slow on a real database
  private val timeoutScale = 4.0

  test(
    "LOPseeTransactionsInMultipleSinglePartySubscriptions",
    "Observers should see transactions in multiple single-party subscriptions",
    allocation,
    timeoutScale,
  ) {
    case TestParticipants(t) =>
      for {
        contractId <- t.alpha.create(t.giver, WithObservers(t.giver, t.observers))
        alphaTransactionsByParty <- transactionsForEachParty(t.alpha, t.alphaParties)
        betaTransactionsByParty <- transactionsForEachParty(t.beta, t.betaParties)
      } yield {
        assertWitnessesOfSinglePartySubscriptions(
          t.alphaParties.toSet,
          contractId,
          activeContractsFrom(alphaTransactionsByParty),
        )
        assertWitnessesOfSinglePartySubscriptions(
          t.betaParties.toSet,
          contractId,
          activeContractsFrom(betaTransactionsByParty),
        )
      }
  }

  test(
    "LOPseeTransactionsInSingleMultiPartySubscription",
    "Observers should see transactions in a single multi-party subscription",
    allocation,
    timeoutScale,
  ) {
    case TestParticipants(t) =>
      for {
        contractId <- t.alpha.create(t.giver, WithObservers(t.giver, t.observers))
        alphaTransactions <- t.alpha.flatTransactions(t.alphaParties: _*)
        betaTransactions <- t.beta.flatTransactions(t.betaParties: _*)
      } yield {
        assertWitnessesOfAMultiPartySubscription(
          t.alphaParties.toSet,
          contractId,
          activeContractsFrom(alphaTransactions),
        )
        assertWitnessesOfAMultiPartySubscription(
          t.betaParties.toSet,
          contractId,
          activeContractsFrom(betaTransactions),
        )
      }
  }

  test(
    "LOPseeActiveContractsInMultipleSinglePartySubscriptions",
    "Observers should see active contracts in multiple single-party subscriptions",
    allocation,
    timeoutScale,
  ) {
    case TestParticipants(t) =>
      for {
        contractId <- t.alpha.create(t.giver, WithObservers(t.giver, t.observers))
        alphaContractsByParty <- activeContractsForEachParty(t.alpha, t.alphaParties)
        betaContractsByParty <- activeContractsForEachParty(t.beta, t.betaParties)
      } yield {
        assertWitnessesOfSinglePartySubscriptions(
          t.alphaParties.toSet,
          contractId,
          alphaContractsByParty,
        )
        assertWitnessesOfSinglePartySubscriptions(
          t.betaParties.toSet,
          contractId,
          betaContractsByParty,
        )
      }
  }

  test(
    "LOPseeActiveContractsInSingleMultiPartySubscription",
    "Observers should see active contracts in a single multi-party subscription",
    allocation,
    timeoutScale,
  ) {
    case TestParticipants(t) =>
      for {
        contractId <- t.alpha.create(t.giver, WithObservers(t.giver, t.observers))
        alphaContracts <- t.alpha.activeContracts(t.alphaParties: _*)
        betaContracts <- t.beta.activeContracts(t.betaParties: _*)
      } yield {
        assertWitnessesOfAMultiPartySubscription(t.alphaParties.toSet, contractId, alphaContracts)
        assertWitnessesOfAMultiPartySubscription(t.betaParties.toSet, contractId, betaContracts)
      }
  }

  private def transactionsForEachParty(
      ledger: ParticipantTestContext,
      observers: Vector[Party],
  ): Future[PartyMap[Vector[Transaction]]] = {
    Future
      .sequence(observers.map(observer => ledger.flatTransactions(observer).map(observer -> _)))
      .map(_.toMap)
  }

  private def activeContractsForEachParty(
      ledger: ParticipantTestContext,
      observers: Vector[Party],
  ): Future[PartyMap[Vector[CreatedEvent]]] = {
    Future
      .sequence(observers.map(observer => ledger.activeContracts(observer).map(observer -> _)))
      .map(_.toMap)
  }

  private def activeContractsFrom(
      transactionsByParty: PartyMap[Vector[Transaction]],
  ): PartyMap[Vector[CreatedEvent]] = {
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
      activeContracts: PartyMap[Seq[CreatedEvent]],
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
      activeContracts: Seq[CreatedEvent],
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

  private case class TestParticipants(
      alpha: ParticipantTestContext,
      beta: ParticipantTestContext,
      giver: Party,
      alphaObservers: Vector[Party],
      betaObservers: Vector[Party],
  ) {
    val observers: Vector[Party] = alphaObservers ++ betaObservers
    val alphaParties: Vector[Party] = giver +: alphaObservers
    val betaParties: Vector[Party] = betaObservers
  }

  private object TestParticipants {
    def unapply(participants: Participants): Option[TestParticipants] = participants match {
      case Participants(
          Participant(alpha, giver, alphaObserversSeq @ _*),
          Participant(beta, betaObserversSeq @ _*),
          ) =>
        Some(
          TestParticipants(
            alpha,
            beta,
            giver,
            alphaObserversSeq.toVector,
            betaObserversSeq.toVector,
          ),
        )
      case _ => None
    }
  }
}
