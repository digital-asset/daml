// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import com.daml.lf.model.test.Symbolic._
import com.microsoft.z3.Status.{SATISFIABLE, UNSATISFIABLE}
import com.microsoft.z3.{BoolExpr, Context, FuncDecl, Sort}

object SymbolicSolver {
  def solve(skeleton: Skeletons.Ledger, numParties: Int): Option[Ledgers.Ledger] = {
    val ctx = new Context()
    val res = new SymbolicSolver(ctx, numParties).solve(skeleton)
    ctx.close()
    res
  }
}

private class SymbolicSolver(ctx: Context, numParties: Int) {

  private val contractIdSort = ctx.mkIntSort()
  private val partySort = ctx.mkIntSort()
  private val partySetSort = ctx.mkSetSort(ctx.mkIntSort())

  private def and(bools: Seq[BoolExpr]): BoolExpr =
    ctx.mkAnd(bools: _*)

  private def or(bools: Seq[BoolExpr]): BoolExpr =
    ctx.mkOr(bools: _*)

  private def isEmptyPartySet(partySet: PartySet): BoolExpr =
    ctx.mkEq(partySet, ctx.mkEmptySet(partySort))

  private def collectCreates(action: Action): Set[ContractId] = action match {
    case Create(contractId, _, _) =>
      Set(contractId)
    case Exercise(_, _, _, _, subTransaction) =>
      subTransaction.view.flatMap(collectCreates).toSet
    case Fetch(_) =>
      Set.empty
    case Rollback(subTransaction) =>
      subTransaction.view.flatMap(collectCreates).toSet
  }

  private def collectReferences(ledger: Ledger): Set[ContractId] =
    ledger.flatMap(_.actions.flatMap(collectReferences)).toSet

  private def collectReferences(action: Action): Set[ContractId] = action match {
    case Create(_, _, _) =>
      Set.empty
    case Exercise(_, contractId, _, _, subTransaction) =>
      subTransaction.view.flatMap(collectReferences).toSet + contractId
    case Fetch(contractId) =>
      Set(contractId)
    case Rollback(subTransaction) =>
      subTransaction.view.flatMap(collectReferences).toSet
  }

  private def collectPartySets(ledger: Ledger): List[PartySet] = {
    def collectCommandsPartySets(commands: Commands): List[PartySet] =
      commands.actAs +: commands.actions.flatMap(collectActionPartySets)

    def collectActionPartySets(action: Action): List[PartySet] = action match {
      case Create(_, signatories, observers) =>
        List(signatories, observers)
      case Exercise(_, _, controllers, choiceObservers, subTransaction) =>
        controllers +: choiceObservers +: subTransaction.flatMap(collectActionPartySets)
      case Fetch(_) =>
        List.empty
      case Rollback(subTransaction) =>
        subTransaction.flatMap(collectActionPartySets)
    }

    ledger.flatMap(collectCommandsPartySets)
  }

  private def collectNonEmptyPartySets(ledger: Ledger): List[PartySet] = {
    def collectCommandsNonEmptyPartySets(commands: Commands): List[PartySet] =
      commands.actAs +: commands.actions.flatMap(collectActionNonEmptyPartySets)

    def collectActionNonEmptyPartySets(action: Action): List[PartySet] = action match {
      case Create(_, signatories, _) =>
        List(signatories)
      case Exercise(_, _, controllers, _, subTransaction) =>
        controllers +: subTransaction.flatMap(collectActionNonEmptyPartySets)
      case Fetch(_) =>
        List.empty
      case Rollback(subTransaction) =>
        subTransaction.flatMap(collectActionNonEmptyPartySets)
    }

    ledger.flatMap(collectCommandsNonEmptyPartySets)
  }

  private def numberLedger(ledger: Ledger): BoolExpr = {
    var lastContractId = -1

    def numberCommands(commands: Commands): BoolExpr =
      and(commands.actions.map(numberAction))

    def numberAction(action: Action): BoolExpr = action match {
      case Create(contractId, _, _) =>
        lastContractId += 1
        ctx.mkEq(contractId, ctx.mkInt(lastContractId))
      case Exercise(_, _, _, _, subTransaction) =>
        and(subTransaction.map(numberAction))
      case Fetch(_) =>
        ctx.mkBool(true)
      case Rollback(subTransaction) =>
        and(subTransaction.map(numberAction))
    }

    and(ledger.map(numberCommands))
  }

  private def consistentLedger(ledger: Ledger): BoolExpr = {
    case class State(created: Set[ContractId], consumed: Set[ContractId])
    var state = State(Set.empty, Set.empty)

    def elem(contractId: ContractId, set: Set[ContractId]): BoolExpr =
      or(set.map(ctx.mkEq(contractId, _)).toList)

    def consistentCommands(commands: Commands): BoolExpr =
      and(commands.actions.map(consistentAction))

    def consistentAction(action: Action): BoolExpr = {
      action match {
        case Create(contractId, _, _) =>
          state = state.copy(created = state.created + contractId)
          ctx.mkBool(true)
        case Exercise(kind, contractId, _, _, subTransaction) =>
          val oldState = state
          if (kind == Consuming) {
            state = state.copy(consumed = state.consumed + contractId)
          }
          ctx.mkAnd(
            elem(contractId, oldState.created),
            ctx.mkNot(elem(contractId, oldState.consumed)),
            and(subTransaction.map(consistentAction)),
          )
        case Fetch(contractId) =>
          ctx.mkAnd(
            elem(contractId, state.created),
            ctx.mkNot(elem(contractId, state.consumed)),
          )
        case Rollback(subTransaction) =>
          val oldState = state
          val subConstraint = and(subTransaction.map(consistentAction))
          state = oldState
          subConstraint
      }
    }

    and(ledger.map(consistentCommands))
  }

  private def hideCreatedContractsInSiblings(ledger: Ledger): BoolExpr = {
    def hideInCommands(commands: Commands): BoolExpr =
      hideInActions(commands.actions)

    def hideInActions(actions: List[Action]): BoolExpr = actions match {
      case Nil => ctx.mkBool(true)
      case action :: actions =>
        val constraints = for {
          contractId <- collectCreates(action)
          reference <- actions.flatMap(collectReferences)
        } yield ctx.mkNot(ctx.mkEq(contractId, reference))
        ctx.mkAnd(and(constraints.toList), hideInActions(actions))
    }

    and(ledger.map(hideInCommands))
  }

  private def authorized(
      signatoriesOf: FuncDecl[PartySetSort],
      observersOf: FuncDecl[PartySetSort],
      ledger: Ledger,
  ): BoolExpr = {

    def authorizedCommands(commands: Commands): BoolExpr =
      and(commands.actions.map(authorizedAction(commands.actAs, _)))

    def authorizedAction(actAs: PartySet, action: Action): BoolExpr = action match {
      case Create(contractId, signatories, observers) =>
        ctx.mkAnd(
          ctx.mkEq(ctx.mkApp(signatoriesOf, contractId), signatories),
          ctx.mkEq(ctx.mkApp(observersOf, contractId), observers),
          ctx.mkSetSubset(signatories, actAs),
        )
      case Exercise(_, contractId, controllers, _, subTransaction) =>
        ctx.mkAnd(
          ctx.mkSetSubset(controllers, actAs),
          and(
            subTransaction.map(
              authorizedAction(
                ctx.mkSetUnion(
                  controllers,
                  ctx.mkApp(signatoriesOf, contractId).asInstanceOf[PartySet],
                ),
                _,
              )
            )
          ),
        )
      case Fetch(contractId) =>
        ctx.mkNot(
          isEmptyPartySet(
            ctx.mkSetIntersection(
              actAs,
              ctx.mkSetUnion(
                ctx.mkApp(signatoriesOf, contractId).asInstanceOf[PartySet],
                ctx.mkApp(observersOf, contractId).asInstanceOf[PartySet],
              ),
            )
          )
        )
      case Rollback(subTransaction) =>
        and(subTransaction.map(authorizedAction(actAs, _)))
    }

    and(ledger.map(authorizedCommands))
  }

  private val allPartiesSetLiteral =
    (1 to numParties).foldLeft(ctx.mkEmptySet(ctx.mkIntSort()))((acc, i) =>
      ctx.mkSetAdd(acc, ctx.mkInt(i))
    )

  private def partySetsWellFormed(ledger: Ledger, allParties: PartySet): BoolExpr =
    and(collectPartySets(ledger).map(s => ctx.mkSetSubset(s, allParties)))

  private def nonEmptyPartySetsWellFormed(ledger: Ledger): BoolExpr =
    and(
      collectNonEmptyPartySets(ledger)
        .map(s => ctx.mkNot(isEmptyPartySet(s)))
    )

  // HERE

  private def hashConstraint(chunkSize: Int, ledger: Ledger): BoolExpr = {
    def contractIdToBits(contractId: ContractId): Seq[BoolExpr] = {
      val bitVector = ctx.mkInt2BV(10, contractId)
      (0 until 10).map(i => ctx.mkEq(ctx.mkExtract(i, i, bitVector), ctx.mkBV(1, 1)))
    }

    def partySetToBits(partySet: PartySet): Seq[BoolExpr] = {
      (1 to numParties).map(i => ctx.mkSelect(partySet, ctx.mkInt(i)).asInstanceOf[BoolExpr])
    }

    def ledgerToBits(ledger: Ledger): Seq[BoolExpr] =
      Seq.concat(
        collectReferences(ledger).toSeq.flatMap(contractIdToBits),
        collectPartySets(ledger).flatMap(partySetToBits),
      )

    def xor(bits: Iterable[BoolExpr]): BoolExpr = bits.reduce((b1, b2) => ctx.mkXor(b1, b2))

    def mkRandomHash(chunkSize: Int, ledger: Ledger): Seq[BoolExpr] =
      ledgerToBits(ledger).grouped(chunkSize).map(xor).toSeq

    and(mkRandomHash(chunkSize, ledger))
  }

  private def solve(ledger: Skeletons.Ledger): Option[Ledgers.Ledger] = {
    val solver = ctx.mkSolver()

    val sLedger = new ToSymbolic(ctx).toSymbolic(ledger)
    val allParties = ctx.mkConst(ctx.mkSymbol("all_parties"), partySetSort).asInstanceOf[PartySet]
    val signatoriesOf = ctx.mkFuncDecl("signatories", Array[Sort](contractIdSort), partySetSort)
    val observersOf = ctx.mkFuncDecl("observers", Array[Sort](contractIdSort), partySetSort)

    // Declare allParties = {1 .. numParties}
    solver.add(ctx.mkEq(allParties, allPartiesSetLiteral))
    // Assign distinct contract IDs to create events
    solver.add(numberLedger(sLedger))
    // Every party set in ledger is a subset of allParties
    solver.add(partySetsWellFormed(sLedger, allParties))
    // Every non-empty party set in ledger is non-empty
    solver.add(nonEmptyPartySetsWellFormed(sLedger))
    // Only active contracts can be exercised
    solver.add(consistentLedger(sLedger))
    // Contracts created in a command cannot be referenced by other actions in the same command
    solver.add(hideCreatedContractsInSiblings(sLedger))
    // Transactions adhere to the authorization rules
    solver.add(authorized(signatoriesOf, observersOf, sLedger))
    // Equate a random hash of all the symbols to resolve to 0
    solver.add(hashConstraint(30, sLedger))

    solver.check() match {
      case SATISFIABLE =>
        Some(new FromSymbolic(numParties, ctx, solver.getModel).toConcrete(sLedger))
      case UNSATISFIABLE =>
        print(".")
        None
      case other =>
        throw new IllegalStateException(s"Unexpected solver result: $other")
    }
  }
}
