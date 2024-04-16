// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import cats.data.{StateT, WriterT}
import cats.implicits.toTraverseOps
import cats.{Monad, TraverseFilter}
import com.daml.lf.model.test.{Ledgers => L}
import com.daml.lf.model.test.{Skeletons => S}
import org.scalacheck.Gen

object LedgerFixer {
  object Instances {
    implicit val genMonad: Monad[Gen] = new Monad[Gen] {

      override def flatMap[A, B](fa: Gen[A])(f: A => Gen[B]): Gen[B] = fa.flatMap(f)

      // not tail recursive ¯\_(ツ)_/¯
      override def tailRecM[A, B](a: A)(f: A => Gen[Either[A, B]]): Gen[B] =
        f(a).flatMap {
          case Left(k) => tailRecM(k)(f)
          case Right(x) => Gen.const(x)
        }

      override def pure[A](x: A): Gen[A] = Gen.const(x)
    }
  }

  case class Contract(signatories: L.PartySet, observers: L.PartySet)
  case class GenState(maxContractId: Int, activeContracts: Map[Int, Contract])
}

class LedgerFixer(numParties: Int) {
  import LedgerFixer.Instances._
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

  def fetchable(
      hidden: L.PartySet,
      authorizers: L.PartySet,
      contract: (L.ContractId, Contract),
  ): Boolean = {
    val (cid, Contract(signatories, observers)) = contract
    authorizers.intersect(signatories.union(observers)).nonEmpty && !hidden.contains(cid)
  }

  def genKind(kind: S.ExerciseKind): L.ExerciseKind = kind match {
    case S.Consuming => L.Consuming
    case S.NonConsuming => L.NonConsuming
  }

  def genAction(
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
          visibleContracts <- liftGen(
            Gen.const(activeContracts.view.filterKeys(!hidden.contains(_))).suchThat(_.nonEmpty)
          )
          toConsume <- liftGen(Gen.oneOf(visibleContracts))
          (cid, Contract(signatories, _)) = toConsume
          controllers <- liftGen(genNonEmptySubsetOf(authorizers))
          choiceObservers <- liftGen(genSubsetOf(globalParties))
          _ <- if (kind == S.Consuming) liftLGen(archiveContract(cid)) else pure(())
          fixedSubTransaction <- genTransaction(hidden, controllers ++ signatories, subTransaction)
        } yield L.Exercise(genKind(kind), cid, controllers, choiceObservers, fixedSubTransaction)
      case S.Fetch() =>
        for {
          activeContracts <- liftLGen(StateT.inspect(_.activeContracts))
          fetchableContracts <- liftGen(
            Gen
              .const(activeContracts.filter(fetchable(hidden, authorizers, _)))
              .suchThat(_.nonEmpty)
          )
          cid <- liftGen(Gen.oneOf(fetchableContracts.keys))
        } yield L.Fetch(cid)
      case S.Rollback(subTransaction) =>
        for {
          activeContracts <- liftLGen(StateT.inspect(_.activeContracts))
          fixedSubTransaction <- genTransaction(hidden, authorizers, subTransaction)
          maxContractId <- liftLGen(StateT.inspect(_.maxContractId))
          _ <- liftLGen(StateT.set(GenState(maxContractId, activeContracts)))
        } yield L.Rollback(fixedSubTransaction)
    }
  }

  def genTransaction(
      hidden: L.PartySet,
      authorizers: L.PartySet,
      transaction: S.Transaction,
  ): WriterT[LGen, L.PartySet, L.Transaction] = {
    transaction.traverse(genAction(hidden, authorizers, _))
  }

  def genActions(
      hidden: L.PartySet,
      authorizers: L.PartySet,
      actions: List[S.Action],
  ): LGen[List[L.Action]] = {
    actions match {
      case Nil => StateT.pure(List.empty)
      case action :: actions =>
        for {
          genActionResult <- genAction(hidden, authorizers, action).run
          (created, fixedAction) = genActionResult
          fixedActions <- genActions(created ++ hidden, authorizers, actions)
        } yield fixedAction :: fixedActions
    }
  }

  def genCommands(commands: S.Commands): LGen[L.Commands] = for {
    actAs <- StateT.liftF(genNonEmptySubsetOf(globalParties))
    fixedActions <- genActions(Set.empty, actAs, commands.actions)
  } yield L.Commands(actAs, fixedActions)

  def genLedger(ledger: S.Ledger): LGen[L.Ledger] = ledger.traverse(genCommands)

  def fixLedger(ledger: S.Ledger): Gen[L.Ledger] =
    genLedger(ledger).run(GenState(-1, Map.empty)).map(_._2)
}
