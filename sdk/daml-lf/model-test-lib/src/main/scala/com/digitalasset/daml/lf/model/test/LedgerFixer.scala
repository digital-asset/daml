// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import cats.TraverseFilter
import cats.data.{StateT, WriterT}
import cats.implicits.toTraverseOps
import com.daml.lf.model.test.Ledgers.PartyId
import com.daml.lf.model.test.{Ledgers => L, Skeletons => S}
import org.scalacheck.Gen

object LedgerFixer {
  case class Contract(signatories: L.PartySet, observers: L.PartySet)
  case class GenState(maxContractId: Int, activeContracts: Map[Int, Contract])
}

class LedgerFixer(numParticipants: Int, numParties: Int) {
  import GenInstances._
  import LedgerFixer._

  val globalParties: Set[L.PartyId] = Set.from(1 to numParties)

  // isomorphic to: GenState -> Gen (Maybe (a, GenState))
  type LGen[A] = StateT[Gen, GenState, A]

  def addContract(signatories: L.PartySet, observers: L.PartySet): LGen[Int] = for {
    genState <- StateT.get[Gen, GenState]
    newContractId = genState.maxContractId + 1
    _ <- StateT.set[Gen, GenState](
      genState.copy(
        maxContractId = newContractId,
        activeContracts =
          genState.activeContracts + (newContractId -> Contract(signatories, observers)),
      )
    )
  } yield newContractId

  def archiveContract(contractId: L.ContractId): LGen[Unit] =
    StateT.modify[Gen, GenState](genState =>
      genState.copy(activeContracts = genState.activeContracts - contractId)
    )

  def genSublistOf[A](l: List[A]): Gen[List[A]] =
    TraverseFilter[List].filterA(l)(_ =>
      for {
        n <- Gen.choose(0, 5)
      } yield (n == 0)
    )

  def genSubsetOf[A](s: Set[A]): Gen[Set[A]] =
    genSublistOf(s.toList).map(_.toSet)

  def genNonEmptySubsetOf[A](s: Set[A]): Gen[Set[A]] =
    genSubsetOf(s).retryUntil(_.nonEmpty)

  // The set of parties whose participant hosts the contract
  def canSee(partiesOnParticipant: L.PartySet, contract: Contract): Set[PartyId] =
    (contract.signatories ++ contract.observers)
      .intersect(partiesOnParticipant)

  def fetchable(
      partiesOnParticipant: L.PartySet,
      hidden: L.PartySet,
      authorizers: L.PartySet,
      contractWithId: (L.ContractId, Contract),
  ): Boolean = {
    val (cid, contract) = contractWithId
    !hidden.contains(cid) && authorizers
      .intersect(contract.signatories ++ contract.observers)
      .intersect(canSee(partiesOnParticipant, contract))
      .nonEmpty
  }

  def exerciseable(
      partiesOnParticipant: L.PartySet,
      hidden: L.PartySet,
      authorizers: L.PartySet,
      contractWithId: (L.ContractId, Contract),
  ): Option[(L.ContractId, Contract, Set[PartyId])] = {
    val (cid, contract) = contractWithId
    val possibleControllers = authorizers.intersect(canSee(partiesOnParticipant, contract))
    Option.when(!hidden.contains(cid) && possibleControllers.nonEmpty)(
      (cid, contract, possibleControllers)
    )
  }

  def genKind(kind: S.ExerciseKind): L.ExerciseKind = kind match {
    case S.Consuming => L.Consuming
    case S.NonConsuming => L.NonConsuming
  }

  def genAction(
      partiesOnParticipant: L.PartySet,
      hidden: L.PartySet,
      authorizers: L.PartySet,
      actionSkel: S.Action,
  ): WriterT[LGen, L.PartySet, L.Action] = {

    def liftLGen[A](a: LGen[A]): WriterT[LGen, L.PartySet, A] =
      WriterT.liftF[LGen, L.PartySet, A](a)
    def liftGen[A](a: Gen[A]): WriterT[LGen, L.PartySet, A] =
      WriterT.liftF[LGen, L.PartySet, A](StateT.liftF(a))
    def tell(s: L.PartySet): WriterT[LGen, L.PartySet, Unit] = WriterT.tell[LGen, L.PartySet](s)
    def pure[A](x: A): WriterT[LGen, L.PartySet, A] = WriterT.value[LGen, L.PartySet, A](x)

    actionSkel match {
      case S.Create() =>
        for {
          signatories <- liftGen(genNonEmptySubsetOf(authorizers))
          observers <- liftGen(genSubsetOf(globalParties))
          contractId <- liftLGen(addContract(signatories, observers))
          _ <- tell(Set(contractId))
        } yield L.Create(contractId, signatories, observers)
      case S.Exercise(kind, subTransaction) =>
        for {
          activeContracts <- liftLGen(StateT.inspect(_.activeContracts))
          exerciseableContracts = activeContracts.flatMap(
            exerciseable(partiesOnParticipant, hidden, authorizers, _)
          )
          toConsume <- liftGen(
            Gen
              .const(exerciseableContracts)
              // Give up the entire generation if no contract is exerciseable
              .suchThat(_.nonEmpty)
              .flatMap(Gen.oneOf(_))
          )
          (cid, contract, controllers) = toConsume
          choiceObservers <- liftGen(genSubsetOf(globalParties))
          _ <- if (kind == S.Consuming) liftLGen(archiveContract(cid)) else pure(())
          fixedSubTransaction <- genTransaction(
            partiesOnParticipant,
            hidden,
            controllers ++ contract.signatories,
            subTransaction,
          )
        } yield L.Exercise(genKind(kind), cid, controllers, choiceObservers, fixedSubTransaction)
      case S.Fetch() =>
        for {
          activeContracts <- liftLGen(StateT.inspect(_.activeContracts))
          fetchableContracts <- liftGen(
            Gen
              .const(
                activeContracts.filter(fetchable(partiesOnParticipant, hidden, authorizers, _))
              )
              .suchThat(_.nonEmpty)
          )
          cid <- liftGen(Gen.oneOf(fetchableContracts.keys))
        } yield L.Fetch(cid)
      case S.Rollback(subTransaction) =>
        for {
          activeContracts <- liftLGen(StateT.inspect(_.activeContracts))
          fixedSubTransaction <- genTransaction(
            partiesOnParticipant,
            hidden,
            authorizers,
            subTransaction,
          )
          maxContractId <- liftLGen(StateT.inspect(_.maxContractId))
          _ <- liftLGen(StateT.set(GenState(maxContractId, activeContracts)))
        } yield L.Rollback(fixedSubTransaction)
    }
  }

  def genTransaction(
      partiesOnParticipant: L.PartySet,
      hidden: L.PartySet,
      authorizers: L.PartySet,
      transaction: S.Transaction,
  ): WriterT[LGen, L.PartySet, L.Transaction] = {
    transaction.traverse(genAction(partiesOnParticipant, hidden, authorizers, _))
  }

  def genActions(
      partiesOnParticipant: L.PartySet,
      hidden: L.PartySet,
      authorizers: L.PartySet,
      actions: List[S.Action],
  ): LGen[List[L.Action]] = {
    actions match {
      case Nil => StateT.pure(List.empty)
      case action :: actions =>
        for {
          genActionResult <- genAction(partiesOnParticipant, hidden, authorizers, action).run
          (created, fixedAction) = genActionResult
          fixedActions <- genActions(
            partiesOnParticipant,
            created ++ hidden,
            authorizers,
            actions,
          )
        } yield fixedAction :: fixedActions
    }
  }

  def genCommands(topology: L.Topology, commands: S.Commands): LGen[L.Commands] = for {
    participantId <- StateT.liftF(Gen.choose(0, numParticipants - 1))
    actAs <- StateT.liftF(genNonEmptySubsetOf(topology(participantId).parties))
    fixedActions <- genActions(topology(participantId).parties, Set.empty, actAs, commands.actions)
  } yield L.Commands(participantId, actAs, fixedActions)

  def genLedger(topology: L.Topology, ledger: S.Ledger): LGen[L.Ledger] =
    ledger.traverse(genCommands(topology, _))

  def fixLedger(ledger: S.Ledger): Gen[L.Scenario] = for {
    topology <- new Generators(numParticipants, numParties).topologyGen
    ledger <- genLedger(topology, ledger).run(GenState(-1, Map.empty)).map(_._2)
  } yield L.Scenario(topology, ledger)
}
