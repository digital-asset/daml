// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import com.daml.lf.model.test.Symbolic._
import com.microsoft.z3.Status.{SATISFIABLE, UNSATISFIABLE}
import com.microsoft.z3.{BoolExpr, Context, FuncDecl, Sort}

object SymbolicSolver {
  def solve(skeleton: Skeletons.Scenario, numParties: Int): Option[Ledgers.Scenario] = {
    val ctx = new Context()
    val res = new SymbolicSolver(ctx, numParties).solve(skeleton)
    ctx.close()
    res
  }
}

private class SymbolicSolver(ctx: Context, numParties: Int) {

  private val participantIdSort = ctx.mkIntSort()
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

  private def numberParticipants(topology: Symbolic.Topology): BoolExpr = {
    def numberParticipant(participant: Participant, i: Int): BoolExpr = {
      ctx.mkEq(participant.participantId, ctx.mkInt(i))
    }

    and(topology.zipWithIndex.map((numberParticipant _).tupled))
  }

  private def tiePartiesToParticipants(
      topology: Topology,
      partiesOf: FuncDecl[PartySetSort],
  ): BoolExpr = {
    def tie(participant: Participant): BoolExpr = {
      ctx.mkEq(ctx.mkApp(partiesOf, participant.participantId), participant.parties)
    }

    and(topology.map(tie))
  }

  private def partition(
      allParties: Symbolic.PartySet,
      partySets: Seq[Symbolic.PartySet],
  ): BoolExpr = {
    def pairwiseNonIntersecting(sets: List[Symbolic.PartySet]): BoolExpr = sets match {
      case Nil => ctx.mkBool(true)
      case set1 :: tail =>
        ctx.mkAnd(
          and(tail.map(set2 => isEmptyPartySet(ctx.mkSetIntersection(set1, set2)))),
          pairwiseNonIntersecting(tail),
        )
    }
    def union(sets: Seq[Symbolic.PartySet]): Symbolic.PartySet =
      sets.foldLeft(ctx.mkEmptySet(partySort))(ctx.mkSetUnion(_, _))

    ctx.mkAnd(ctx.mkEq(union(partySets), allParties), pairwiseNonIntersecting(partySets.toList))
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

  private def visibleContracts(
      partiesOf: FuncDecl[PartySetSort],
      signatoriesOf: FuncDecl[PartySetSort],
      observersOf: FuncDecl[PartySetSort],
      ledger: Symbolic.Ledger,
  ): BoolExpr = {

    def visibleContractId(participantId: ParticipantId, contractId: ContractId): BoolExpr =
      ctx.mkNot(
        isEmptyPartySet(
          ctx.mkSetIntersection(
            ctx.mkApp(partiesOf, participantId).asInstanceOf[PartySet],
            ctx.mkSetUnion(
              ctx.mkApp(signatoriesOf, contractId).asInstanceOf[PartySet],
              ctx.mkApp(observersOf, contractId).asInstanceOf[PartySet],
            ),
          )
        )
      )

    def visibleAction(participantId: ParticipantId, action: Action): BoolExpr = action match {
      case Create(_, _, _) =>
        ctx.mkTrue()
      case Exercise(_, contractId, _, _, subTransaction) =>
        ctx.mkAnd(
          visibleContractId(participantId, contractId),
          and(subTransaction.map(visibleAction(participantId, _))),
        )
      case Fetch(contractId) =>
        visibleContractId(participantId, contractId)
      case Rollback(subTransaction) =>
        and(subTransaction.map(visibleAction(participantId, _)))
    }

    def visibleCommands(commands: Commands): BoolExpr =
      and(commands.actions.map(visibleAction(commands.participantId, _)))

    and(ledger.map(visibleCommands))
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

  private def validParticipantIdsInCommands(scenario: Scenario): BoolExpr = {
    def allParticipantIds = scenario.topology.map(_.participantId)

    def isValidParticipantId(participantId: ParticipantId): BoolExpr =
      or(allParticipantIds.map(ctx.mkEq(participantId, _)))

    and(scenario.ledger.map(commands => isValidParticipantId(commands.participantId)))
  }

  private def actorsAreHostedOnParticipant(
      partiesOf: FuncDecl[PartySetSort],
      ledger: Symbolic.Ledger,
  ): BoolExpr = {
    def actorsAreHostedOnParticipant(commands: Commands): BoolExpr =
      ctx.mkSetSubset(
        commands.actAs,
        ctx.mkApp(partiesOf, commands.participantId).asInstanceOf[PartySet],
      )

    and(ledger.map(actorsAreHostedOnParticipant))
  }

  private def controllersAreHostedOnParticipant(
      partiesOf: FuncDecl[PartySetSort],
      ledger: Symbolic.Ledger,
  ): BoolExpr = {
    def validActionControllers(participantId: ParticipantId, action: Action): BoolExpr =
      action match {
        case Create(_, _, _) =>
          ctx.mkBool(true)
        case Exercise(_, _, controllers, _, subTransaction) =>
          ctx.mkAnd(
            ctx
              .mkSetSubset(controllers, ctx.mkApp(partiesOf, participantId).asInstanceOf[PartySet]),
            and(subTransaction.map(validActionControllers(participantId, _))),
          )
        case Fetch(_) =>
          ctx.mkBool(true)
        case Rollback(subTransaction) =>
          and(subTransaction.map(validActionControllers(participantId, _)))
      }

    def validCommandsControllers(commands: Commands): BoolExpr =
      and(commands.actions.map(validActionControllers(commands.participantId, _)))

    and(ledger.map(validCommandsControllers))
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

  private def hashConstraint(chunkSize: Int, scenario: Scenario): BoolExpr = {
    def contractIdToBits(contractId: ContractId): Seq[BoolExpr] = {
      val bitVector = ctx.mkInt2BV(5, contractId)
      (0 until 5).map(i => ctx.mkEq(ctx.mkExtract(i, i, bitVector), ctx.mkBV(1, 1)))
    }

    def partySetToBits(partySet: PartySet): Seq[BoolExpr] = {
      (1 to numParties).map(i => ctx.mkSelect(partySet, ctx.mkInt(i)).asInstanceOf[BoolExpr])
    }

    def ledgerToBits(ledger: Ledger): Seq[BoolExpr] =
      Seq.concat(
        collectReferences(ledger).toSeq.flatMap(contractIdToBits),
        collectPartySets(ledger).flatMap(partySetToBits),
      )

    def participantToBits(participant: Participant): Seq[BoolExpr] =
      partySetToBits(participant.parties)

    def topologyToBits(topology: Topology): Seq[BoolExpr] =
      topology.flatMap(participantToBits)

    def scenarioToBits(scenario: Scenario): Seq[BoolExpr] = {
      Seq.concat(topologyToBits(scenario.topology), ledgerToBits(scenario.ledger))
    }

    def xor(bits: Iterable[BoolExpr]): BoolExpr = bits.reduce((b1, b2) => ctx.mkXor(b1, b2))

    def mkRandomHash(chunkSize: Int, scenario: Scenario): Seq[BoolExpr] =
      scenarioToBits(scenario).grouped(chunkSize).map(xor).toSeq

    and(mkRandomHash(chunkSize, scenario))
  }

  private def solve(scenario: Skeletons.Scenario): Option[Ledgers.Scenario] = {
    val solver = ctx.mkSolver()

    val symScenario = new ToSymbolic(ctx).toSymbolic(scenario)
    val Scenario(symTopology, symLedger) = symScenario

    val allParties = ctx.mkConst(ctx.mkSymbol("all_parties"), partySetSort).asInstanceOf[PartySet]
    val signatoriesOf = ctx.mkFuncDecl("signatories", Array[Sort](contractIdSort), partySetSort)
    val observersOf = ctx.mkFuncDecl("observers", Array[Sort](contractIdSort), partySetSort)
    val partiesOf = ctx.mkFuncDecl("parties", Array[Sort](participantIdSort), partySetSort)

    // Declare allParties = {1 .. numParties}
    solver.add(ctx.mkEq(allParties, allPartiesSetLiteral))
    // Assign distinct contract IDs to create events
    solver.add(numberLedger(symLedger))
    // Assign distinct participant IDs to participants in the topology
    solver.add(numberParticipants(symTopology))
    // Tie parties to participants via partiesOf
    solver.add(tiePartiesToParticipants(symTopology, partiesOf))
    // Participants form a partition of allParties
    solver.add(partition(allParties, symTopology.map(_.parties)))
    // Participants have at least one party
    solver.add(and(symTopology.map(p => ctx.mkNot(isEmptyPartySet(p.parties)))))
    // Every party set in ledger is a subset of allParties
    solver.add(partySetsWellFormed(symLedger, allParties))
    // Every non-empty party set in ledger is non-empty
    solver.add(nonEmptyPartySetsWellFormed(symLedger))
    // Only active contracts can be exercised or fetched
    solver.add(consistentLedger(symLedger))
    // Only contracts visible to the participant can be exercised of fetched on a participant
    solver.add(visibleContracts(partiesOf, signatoriesOf, observersOf, symLedger))
    // Contracts created in a command cannot be referenced by other actions in the same command
    solver.add(hideCreatedContractsInSiblings(symLedger))
    // Transactions adhere to the authorization rules
    solver.add(authorized(signatoriesOf, observersOf, symLedger))
    // Participants IDs in a command refer to existing participants
    solver.add(validParticipantIdsInCommands(symScenario))
    // Actors of a command should be hosted by the participant executing the command
    solver.add(actorsAreHostedOnParticipant(partiesOf, symLedger))
    // Controllers of an exercise should be hosted by the participant executing the command
    solver.add(controllersAreHostedOnParticipant(partiesOf, symLedger))
    // Equate a random hash of all the symbols to resolve to 0
    solver.add(hashConstraint(20, symScenario))

    solver.check() match {
      case SATISFIABLE =>
        Some(new FromSymbolic(numParties, ctx, solver.getModel).toConcrete(symScenario))
      case UNSATISFIABLE =>
        print(".")
        None
      case other =>
        throw new IllegalStateException(s"Unexpected solver result: $other")
    }
  }
}
