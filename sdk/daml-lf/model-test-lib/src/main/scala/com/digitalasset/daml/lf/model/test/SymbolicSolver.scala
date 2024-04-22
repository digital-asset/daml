// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import com.daml.lf.model.test.Symbolic._
import com.microsoft.z3.Status.{SATISFIABLE, UNSATISFIABLE}
import com.microsoft.z3._

object SymbolicSolver {
  def solve(skeleton: Skeletons.Scenario, numParties: Int): Option[Ledgers.Scenario] = {
    // Global.setParameter("verbose", "1")
    val ctx = new Context()
    val res = new SymbolicSolver(ctx, numParties).solve(skeleton)
    ctx.close()
    res
  }

  def valid(scenario: Ledgers.Scenario, numParties: Int): Boolean = {
    val ctx = new Context()
    val res = new SymbolicSolver(ctx, numParties).validate(scenario)
    ctx.close()
    res
  }
}

private class SymbolicSolver(ctx: Context, numParties: Int) {

  private val participantIdSort = ctx.mkIntSort()
  private val contractIdSort = ctx.mkIntSort()
  private val keyIdSort = ctx.mkIntSort()
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
    case CreateWithKey(contractId, _, _, _, _) =>
      Set(contractId)
    case Exercise(_, _, _, _, subTransaction) =>
      subTransaction.view.flatMap(collectCreates).toSet
    case ExerciseByKey(_, _, _, _, _, _, subTransaction) =>
      subTransaction.view.flatMap(collectCreates).toSet
    case Fetch(_) =>
      Set.empty
    case Rollback(subTransaction) =>
      subTransaction.view.flatMap(collectCreates).toSet
  }

  private def collectkeyIds(ledger: Ledger): Set[keyId] =
    ledger.flatMap(_.actions.flatMap(collectkeyIds)).toSet

  private def collectkeyIds(action: Action): Set[keyId] = action match {
    case Create(_, _, _) =>
      Set.empty
    case CreateWithKey(_, keyId, _, _, _) =>
      Set(keyId)
    case Exercise(_, _, _, _, subTransaction) =>
      subTransaction.view.flatMap(collectkeyIds).toSet
    case ExerciseByKey(_, _, keyId, _, _, _, subTransaction) =>
      subTransaction.view.flatMap(collectkeyIds).toSet + keyId
    case Fetch(_) =>
      Set.empty
    case Rollback(subTransaction) =>
      subTransaction.view.flatMap(collectkeyIds).toSet
  }

  private def collectReferences(ledger: Ledger): Set[ContractId] =
    ledger.flatMap(_.actions.flatMap(collectReferences)).toSet

  private def collectReferences(action: Action): Set[ContractId] = action match {
    case Create(_, _, _) =>
      Set.empty
    case CreateWithKey(_, _, _, _, _) =>
      Set.empty
    case Exercise(_, contractId, _, _, subTransaction) =>
      subTransaction.view.flatMap(collectReferences).toSet + contractId
    case ExerciseByKey(_, contractId, _, _, _, _, subTransaction) =>
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
      case CreateWithKey(_, _, maintainers, signatories, observers) =>
        List(maintainers, signatories, observers)
      case Exercise(_, _, controllers, choiceObservers, subTransaction) =>
        controllers +: choiceObservers +: subTransaction.flatMap(collectActionPartySets)
      case ExerciseByKey(_, _, _, maintainers, controllers, choiceObservers, subTransaction) =>
        maintainers +: controllers +: choiceObservers +: subTransaction.flatMap(
          collectActionPartySets
        )
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
      case CreateWithKey(_, _, maintainers, signatories, _) =>
        List(maintainers, signatories)
      case Exercise(_, _, controllers, _, subTransaction) =>
        controllers +: subTransaction.flatMap(collectActionNonEmptyPartySets)
      case ExerciseByKey(_, _, _, maintainers, controllers, _, subTransaction) =>
        maintainers +: controllers +: subTransaction.flatMap(collectActionNonEmptyPartySets)
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
      case CreateWithKey(contractId, _, _, _, _) =>
        lastContractId += 1
        ctx.mkEq(contractId, ctx.mkInt(lastContractId))
      case Exercise(_, _, _, _, subTransaction) =>
        and(subTransaction.map(numberAction))
      case ExerciseByKey(_, _, _, _, _, _, subTransaction) =>
        and(subTransaction.map(numberAction))
      case Fetch(_) =>
        ctx.mkBool(true)
      case Rollback(subTransaction) =>
        and(subTransaction.map(numberAction))
    }

    and(ledger.map(numberCommands))
  }

  private def numberkeyIds(ledger: Ledger): BoolExpr = {
    val keyIds = collectkeyIds(ledger)
    and(
      keyIds
        .map(keyId =>
          ctx.mkAnd(
            ctx.mkGe(keyId, ctx.mkInt(1)),
            ctx.mkLe(keyId, ctx.mkInt(keyIds.size)),
          )
        )
        .toSeq
    )
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

  private def allPartiesCoveredBy(
      allParties: Symbolic.PartySet,
      partySets: Seq[Symbolic.PartySet],
  ): BoolExpr = {
    def union(sets: Seq[Symbolic.PartySet]): Symbolic.PartySet =
      sets.foldLeft(ctx.mkEmptySet(partySort))(ctx.mkSetUnion(_, _))
    ctx.mkAnd(ctx.mkEq(union(partySets), allParties))
  }

  private def consistentLedger(
      hasKey: FuncDecl[BoolSort],
      keyIdsOf: FuncDecl[keyIdSort],
      maintainersOf: FuncDecl[PartySetSort],
      ledger: Ledger,
  ): BoolExpr = {
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
          ctx.mkEq(ctx.mkApp(hasKey, contractId), ctx.mkFalse())
        case CreateWithKey(contractId, keyId, maintainers, _, _) =>
          val oldState = state
          state = state.copy(created = state.created + contractId)
          ctx.mkAnd(
            ctx.mkEq(ctx.mkApp(keyIdsOf, contractId), keyId),
            ctx.mkEq(ctx.mkApp(maintainersOf, contractId), maintainers),
            ctx.mkEq(ctx.mkApp(hasKey, contractId), ctx.mkTrue()),
            and(
              for {
                cid <- oldState.created.toSeq
              } yield ctx.mkImplies(
                ctx.mkAnd(
                  ctx.mkEq(ctx.mkApp(keyIdsOf, cid), keyId),
                  ctx.mkEq(ctx.mkApp(maintainersOf, cid), maintainers),
                ),
                elem(cid, oldState.consumed),
              )
            ),
          )
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
        case ExerciseByKey(kind, contractId, keyId, maintainers, _, _, subTransaction) =>
          val oldState = state
          if (kind == Consuming) {
            state = state.copy(consumed = state.consumed + contractId)
          }
          ctx.mkAnd(
            elem(contractId, oldState.created),
            ctx.mkNot(elem(contractId, oldState.consumed)),
            ctx.mkEq(ctx.mkApp(hasKey, contractId), ctx.mkTrue()),
            ctx.mkEq(ctx.mkApp(keyIdsOf, contractId), keyId),
            ctx.mkEq(ctx.mkApp(maintainersOf, contractId), maintainers),
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
      case CreateWithKey(_, _, _, _, _) =>
        ctx.mkTrue()
      case Exercise(_, contractId, _, _, subTransaction) =>
        ctx.mkAnd(
          visibleContractId(participantId, contractId),
          and(subTransaction.map(visibleAction(participantId, _))),
        )
      case ExerciseByKey(_, contractId, _, _, _, _, subTransaction) =>
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
      case CreateWithKey(contractId, _, _, signatories, observers) =>
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
      case ExerciseByKey(_, contractId, _, _, controllers, _, subTransaction) =>
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

  private def maintainersAreSubsetsOfSignatories(ledger: Ledger): BoolExpr = {
    def validCommands(commands: Commands): BoolExpr = {
      and(commands.actions.map(validAction))
    }

    def validAction(action: Action): BoolExpr = action match {
      case Create(_, _, _) =>
        ctx.mkTrue()
      case CreateWithKey(_, _, maintainers, signatories, _) =>
        ctx.mkSetSubset(maintainers, signatories)
      case Exercise(_, _, _, _, subTransaction) =>
        and(subTransaction.map(validAction))
      case ExerciseByKey(_, _, _, _, _, _, subTransaction) =>
        and(subTransaction.map(validAction))
      case Fetch(_) =>
        ctx.mkTrue()
      case Rollback(subTransaction) =>
        and(subTransaction.map(validAction))
    }

    and(ledger.map(validCommands))
  }

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

  private def validScenario(
      symScenario: Scenario,
      allParties: PartySet,
      signatoriesOf: FuncDecl[PartySetSort],
      observersOf: FuncDecl[PartySetSort],
      partiesOf: FuncDecl[PartySetSort],
      keyIdsOf: FuncDecl[keyIdSort],
      maintainersOf: FuncDecl[PartySetSort],
      hasKey: FuncDecl[BoolSort],
  ): BoolExpr = {
    val Scenario(symTopology, symLedger) = symScenario
    ctx.mkAnd(
      // Declare allParties = {1 .. numParties}
      ctx.mkEq(allParties, allPartiesSetLiteral),
      // Assign distinct contract IDs to create events
      numberLedger(symLedger),
      // Make sure that key nums are between 1 and number of create with keys
      // in order to limit the leeway given to the hash function here.
      numberkeyIds(symLedger),
      // Assign distinct participant IDs to participants in the topology
      numberParticipants(symTopology),
      // Tie parties to participants via partiesOf
      tiePartiesToParticipants(symTopology, partiesOf),
      // Participants cover all parties
      allPartiesCoveredBy(allParties, symTopology.map(_.parties)),
      // Participants have at least one party - not required but empty participants are kind of useless
      and(symTopology.map(p => ctx.mkNot(isEmptyPartySet(p.parties)))),
      // Every party set in ledger is a subset of allParties
      partySetsWellFormed(symLedger, allParties),
      // Signatories and controllers in ledger are non-empty
      nonEmptyPartySetsWellFormed(symLedger),
      // Only active contracts can be exercised or fetched
      consistentLedger(hasKey, keyIdsOf, maintainersOf, symLedger),
      // Only contracts visible to the participant can be exercised of fetched on that participant
      visibleContracts(partiesOf, signatoriesOf, observersOf, symLedger),
      // Contracts created in a command cannot be referenced by other actions in the same command
      hideCreatedContractsInSiblings(symLedger),
      // Transactions adhere to the authorization rules
      authorized(signatoriesOf, observersOf, symLedger),
      // Participants IDs in a command refer to existing participants
      validParticipantIdsInCommands(symScenario),
      // Actors of a command should be hosted by the participant executing the command
      actorsAreHostedOnParticipant(partiesOf, symLedger),
      // Maintainers are subsets of signatories
      maintainersAreSubsetsOfSignatories(symLedger),
    )
  }

  private case class Constants(
      allParties: PartySet,
      signatoriesOf: FuncDecl[PartySetSort],
      observersOf: FuncDecl[PartySetSort],
      partiesOf: FuncDecl[PartySetSort],
      keyIdsOf: FuncDecl[keyIdSort],
      maintainersOf: FuncDecl[PartySetSort],
      hasKey: FuncDecl[BoolSort],
  )

  private def mkFreshConstants(): Constants =
    Constants(
      allParties = ctx.mkFreshConst("all_parties", partySetSort).asInstanceOf[PartySet],
      signatoriesOf = ctx.mkFreshFuncDecl("signatories", Array[Sort](contractIdSort), partySetSort),
      observersOf = ctx.mkFreshFuncDecl("observers", Array[Sort](contractIdSort), partySetSort),
      partiesOf = ctx.mkFreshFuncDecl("parties", Array[Sort](participantIdSort), partySetSort),
      keyIdsOf = ctx.mkFreshFuncDecl("key_nums", Array[Sort](contractIdSort), keyIdSort),
      maintainersOf = ctx.mkFreshFuncDecl("maintainers", Array[Sort](contractIdSort), partySetSort),
      hasKey = ctx.mkFreshFuncDecl("has_key", Array[Sort](contractIdSort), ctx.mkBoolSort()),
    )

  private def solve(scenario: Skeletons.Scenario): Option[Ledgers.Scenario] = {
    val symScenario = SkeletonToSymbolic.toSymbolic(ctx, scenario)
    val Constants(
      allParties,
      signatoriesOf,
      observersOf,
      partiesOf,
      keyIdsOf,
      maintainersOf,
      hasKey,
    ) =
      mkFreshConstants()

    val solver = ctx.mkSolver()
    // val params = ctx.mkParams()
    // params.add("threads", 20)
    // solver.setParameters(params)

    // The scenario is valid
    solver.add(
      validScenario(
        symScenario,
        allParties,
        signatoriesOf,
        observersOf,
        partiesOf,
        keyIdsOf,
        maintainersOf,
        hasKey,
      )
    )
    // Equate a random hash of all the symbols to 0
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

  private def validate(scenario: Ledgers.Scenario): Boolean = {
    val (symScenario, constraints) = ConcreteToSymbolic.toSymbolic(ctx, scenario)
    val Constants(
      allParties,
      signatoriesOf,
      observersOf,
      partiesOf,
      keyIdsOf,
      maintainersOf,
      hasKey,
    ) =
      mkFreshConstants()

    val solver = ctx.mkSolver()
    // Translation constraints
    solver.add(constraints)
    // The scenario is valid
    solver.add(
      validScenario(
        symScenario,
        allParties,
        signatoriesOf,
        observersOf,
        partiesOf,
        keyIdsOf,
        maintainersOf,
        hasKey,
      )
    )

    solver.check() match {
      case SATISFIABLE => true
      case UNSATISFIABLE => false
      case other =>
        throw new IllegalStateException(s"Unexpected solver result: $other")
    }
  }
}
