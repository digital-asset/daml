// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import com.daml.lf.model.test.LedgerImplicits._
import com.daml.lf.model.test.Symbolic._
import com.microsoft.z3.Status.{SATISFIABLE, UNKNOWN, UNSATISFIABLE}
import com.microsoft.z3._

import scala.util.Random

object SymbolicSolver {
  def solve(
      skeleton: Skeletons.Scenario,
      numPackages: Int,
      numParties: Int,
  ): Option[Ledgers.Scenario] = {
    Global.setParameter("model_validate", "true")
    val ctx = new Context()
    val res = new SymbolicSolver(ctx, numPackages, numParties).solve(skeleton)
    ctx.close()
    res
  }

  def valid(scenario: Ledgers.Scenario, numPackages: Int, numParties: Int): Boolean = {
    val ctx = new Context()
    val res = new SymbolicSolver(ctx, numPackages, numParties).validate(scenario)
    ctx.close()
    res
  }
}

//@nowarn("cat=unused")
private class SymbolicSolver(ctx: Context, numPackages: Int, numParties: Int) {

  private val participantIdSort = ctx.mkIntSort()
  private val contractIdSort = ctx.mkIntSort()
  private val keyIdSort = ctx.mkIntSort()
  private val partySort = ctx.mkIntSort()
  private val partySetSort = ctx.mkSetSort(ctx.mkIntSort())

  private def and(bools: Seq[BoolExpr]): BoolExpr =
    ctx.mkAnd(bools: _*)

  private def or(bools: Seq[BoolExpr]): BoolExpr =
    ctx.mkOr(bools: _*)

  def union[E <: Sort](elemSort: E, sets: Seq[ArrayExpr[E, BoolSort]]): ArrayExpr[E, BoolSort] =
    sets.foldLeft(ctx.mkEmptySet(elemSort))(ctx.mkSetUnion(_, _))

  def toSet[E <: Sort](elemSort: E, elems: Iterable[Expr[E]]): ArrayExpr[E, BoolSort] =
    elems.foldLeft(ctx.mkEmptySet(elemSort))((acc, cid) => ctx.mkSetAdd(acc, cid))

  private def isEmptyPartySet(partySet: PartySet): BoolExpr =
    ctx.mkEq(partySet, ctx.mkEmptySet(partySort))

  private def collectCreatedContractIds(ledger: Ledger): Set[ContractId] =
    ledger.flatMap(_.commands.flatMap(collectCreatedContractIds)).toSet

  private def collectCreatedContractIds(command: Command): Set[ContractId] = command match {
    case Command(_, action) =>
      collectCreatedContractIds(action)
  }

  private def collectCreatedContractIds(action: Action): Set[ContractId] = action match {
    case Create(contractId, _, _) =>
      Set(contractId)
    case CreateWithKey(contractId, _, _, _, _) =>
      Set(contractId)
    case Exercise(_, _, _, _, subTransaction) =>
      subTransaction.view.flatMap(collectCreatedContractIds).toSet
    case ExerciseByKey(_, _, _, _, _, _, subTransaction) =>
      subTransaction.view.flatMap(collectCreatedContractIds).toSet
    case Fetch(_) =>
      Set.empty
    case FetchByKey(_, _, _) =>
      Set.empty
    case LookupByKey(_, _, _) =>
      Set.empty
    case Rollback(subTransaction) =>
      subTransaction.view.flatMap(collectCreatedContractIds).toSet
  }

  private def collectCreatedKeyIds(ledger: Ledger): Set[KeyId] =
    ledger.flatMap(_.commands.flatMap(collectCreatedKeyIds)).toSet

  private def collectCreatedKeyIds(command: Command): Set[KeyId] = command match {
    case Command(_, action) => collectCreatedKeyIds(action)
  }

  private def collectCreatedKeyIds(action: Action): Set[KeyId] = action match {
    case Create(_, _, _) =>
      Set.empty
    case CreateWithKey(_, keyId, _, _, _) =>
      Set(keyId)
    case Exercise(_, _, _, _, subTransaction) =>
      subTransaction.view.flatMap(collectCreatedKeyIds).toSet
    case ExerciseByKey(_, _, keyId, _, _, _, subTransaction) =>
      subTransaction.view.flatMap(collectCreatedKeyIds).toSet + keyId
    case Fetch(_) =>
      Set.empty
    case FetchByKey(_, _, _) =>
      Set.empty
    case LookupByKey(_, _, _) =>
      Set.empty
    case Rollback(subTransaction) =>
      subTransaction.view.flatMap(collectCreatedKeyIds).toSet
  }

  private def collectReferences(ledger: Ledger): Set[ContractId] =
    ledger.flatMap(_.commands.flatMap(collectReferences)).toSet

  private def collectReferences(command: Command): Set[ContractId] = command match {
    case Command(_, action) => collectReferences(action)
  }

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
    case FetchByKey(contractId, _, _) =>
      Set(contractId)
    case LookupByKey(contractId, _, _) =>
      contractId.fold(Set.empty[ContractId])(Set(_))
    case Rollback(subTransaction) =>
      subTransaction.view.flatMap(collectReferences).toSet
  }

  private def collectPartySets(ledger: Ledger): List[PartySet] = {
    def collectCommandsPartySets(commands: Commands): List[PartySet] =
      commands.actAs +: commands.commands.flatMap(collectCommandPartySets)

    def collectCommandPartySets(command: Command): List[PartySet] = command match {
      case Command(_, action) => collectActionPartySets(action)
    }

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
      case FetchByKey(_, _, maintainers) =>
        List(maintainers)
      case LookupByKey(_, _, maintainers) =>
        List(maintainers)
      case Rollback(subTransaction) =>
        subTransaction.flatMap(collectActionPartySets)
    }

    ledger.flatMap(collectCommandsPartySets)
  }

  private def collectDisclosures(ledger: Ledger): List[ContractIdSet] = {
    ledger.map(_.disclosures)
  }

  private def collectNonEmptyPartySets(ledger: Ledger): List[PartySet] = {
    def collectCommandsNonEmptyPartySets(commands: Commands): List[PartySet] =
      commands.actAs +: commands.commands.flatMap(collectCommandNonEmptyPartySets)

    def collectCommandNonEmptyPartySets(command: Command): List[PartySet] = command match {
      case Command(_, action) => collectActionNonEmptyPartySets(action)
    }

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
      case FetchByKey(_, _, maintainers) =>
        List(maintainers)
      case LookupByKey(_, _, maintainers) =>
        List(maintainers)
      case Rollback(subTransaction) =>
        subTransaction.flatMap(collectActionNonEmptyPartySets)
    }

    ledger.flatMap(collectCommandsNonEmptyPartySets)
  }

  private def collectPackageIds(ledger: Ledger): Set[PackageId] =
    ledger.flatMap(_.commands.flatMap(_.packageId)).toSet

  private def numberLedger(ledger: Ledger): BoolExpr = {
    var lastContractId = -1

    def numberCommands(commands: Commands): BoolExpr =
      and(commands.commands.map(numberCommand))

    def numberCommand(command: Command): BoolExpr = command match {
      case Command(_, action) => numberAction(action)
    }

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
        ctx.mkTrue()
      case FetchByKey(_, _, _) =>
        ctx.mkTrue()
      case LookupByKey(_, _, _) =>
        ctx.mkTrue()
      case Rollback(subTransaction) =>
        and(subTransaction.map(numberAction))
    }

    and(ledger.map(numberCommands))
  }

  private def numberkeyIds(ledger: Ledger): BoolExpr = {
    val keyIds = collectCreatedKeyIds(ledger)
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
  ): BoolExpr =
    ctx.mkAnd(ctx.mkEq(union(partySort, partySets), allParties))

  private def consistentLedger(
      hasKey: FuncDecl[BoolSort],
      keyIdsOf: FuncDecl[KeyIdSort],
      maintainersOf: FuncDecl[PartySetSort],
      ledger: Ledger,
  ): BoolExpr = {
    case class State(created: Set[ContractId], consumed: Set[ContractId])
    var state = State(Set.empty, Set.empty)

    def elem(contractId: ContractId, set: Set[ContractId]): BoolExpr =
      or(set.map(ctx.mkEq(contractId, _)).toList)

    def isActive(contractId: ContractId): BoolExpr =
      ctx.mkAnd(
        elem(contractId, state.created),
        ctx.mkNot(elem(contractId, state.consumed)),
      )

    def contractHasKey(contractId: ContractId, keyId: KeyId, maintainers: PartySet): BoolExpr =
      ctx.mkAnd(
        ctx.mkApp(hasKey, contractId),
        ctx.mkEq(ctx.mkApp(keyIdsOf, contractId), keyId),
        ctx.mkEq(ctx.mkApp(maintainersOf, contractId), maintainers),
      )

    def noActiveContractWithKey(keyId: KeyId, maintainers: PartySet): BoolExpr =
      and(
        for {
          cid <- state.created.toSeq
        } yield ctx.mkImplies(
          contractHasKey(cid, keyId, maintainers),
          elem(cid, state.consumed),
        )
      )

    def consistentCommands(commands: Commands): BoolExpr = {
      ctx.mkAnd(
        ctx.mkSetSubset(
          commands.disclosures,
          toSet(contractIdSort, state.created),
        ),
        // in theory you can disclose consumed contracts, but we don't allow it
        // yet because their disclosure is tricky to fetch.
        isEmptyPartySet(
          ctx.mkSetIntersection(
            commands.disclosures,
            toSet(contractIdSort, state.consumed),
          )
        ),
        and(commands.commands.map(consistentCommand)),
      )
    }

    def consistentCommand(command: Command): BoolExpr = command match {
      case Command(_, action) => consistentAction(action)
    }

    def consistentAction(action: Action): BoolExpr = {
      action match {
        case Create(contractId, _, _) =>
          state = state.copy(created = state.created + contractId)
          ctx.mkEq(ctx.mkApp(hasKey, contractId), ctx.mkFalse())
        case CreateWithKey(contractId, keyId, maintainers, _, _) =>
          val res = ctx.mkAnd(
            contractHasKey(contractId, keyId, maintainers),
            noActiveContractWithKey(keyId, maintainers),
          )
          state = state.copy(created = state.created + contractId)
          res
        case Exercise(kind, contractId, _, _, subTransaction) =>
          val pre = isActive(contractId)
          if (kind == Consuming) {
            state = state.copy(consumed = state.consumed + contractId)
          }
          val post = and(subTransaction.map(consistentAction))
          ctx.mkAnd(pre, post)
        case ExerciseByKey(kind, contractId, keyId, maintainers, _, _, subTransaction) =>
          val pre = ctx.mkAnd(
            isActive(contractId),
            contractHasKey(contractId, keyId, maintainers),
          )
          if (kind == Consuming) {
            state = state.copy(consumed = state.consumed + contractId)
          }
          val post = and(subTransaction.map(consistentAction))
          ctx.mkAnd(pre, post)
        case Fetch(contractId) =>
          isActive(contractId)
        case FetchByKey(contractId, keyId, maintainers) =>
          ctx.mkAnd(
            isActive(contractId),
            contractHasKey(contractId, keyId, maintainers),
          )
        case LookupByKey(contractId, keyId, maintainers) =>
          contractId match {
            case Some(cid) =>
              ctx.mkAnd(
                isActive(cid),
                contractHasKey(cid, keyId, maintainers),
              )
            case None =>
              noActiveContractWithKey(keyId, maintainers)
          }
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

    def visibleContractId(
        disclosures: ContractIdSet,
        participantId: ParticipantId,
        contractId: ContractId,
    ): BoolExpr = {
      ctx.mkOr(
        ctx.mkSetMembership(contractId, disclosures),
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
        ),
      )
    }

    def visibleAction(
        disclosures: ContractIdSet,
        participantId: ParticipantId,
        action: Action,
    ): BoolExpr = action match {
      case Create(_, _, _) =>
        ctx.mkTrue()
      case CreateWithKey(_, _, _, _, _) =>
        ctx.mkTrue()
      case Exercise(_, contractId, _, _, subTransaction) =>
        ctx.mkAnd(
          visibleContractId(disclosures, participantId, contractId),
          and(subTransaction.map(visibleAction(disclosures, participantId, _))),
        )
      case ExerciseByKey(_, contractId, _, _, _, _, subTransaction) =>
        ctx.mkAnd(
          visibleContractId(disclosures, participantId, contractId),
          and(subTransaction.map(visibleAction(disclosures, participantId, _))),
        )
      case Fetch(contractId) =>
        visibleContractId(disclosures, participantId, contractId)
      case FetchByKey(contractId, _, _) =>
        visibleContractId(disclosures, participantId, contractId)
      case LookupByKey(contractId, _, _) =>
        contractId match {
          case Some(cid) =>
            visibleContractId(disclosures, participantId, cid)
          case None =>
            ctx.mkTrue()
        }
      case Rollback(subTransaction) =>
        and(subTransaction.map(visibleAction(disclosures, participantId, _)))
    }

    def visibleCommand(
        disclosures: ContractIdSet,
        participantId: ParticipantId,
        command: Command,
    ): BoolExpr = command match {
      case Command(_, action) => visibleAction(disclosures, participantId, action)
    }

    def visibleCommands(commands: Commands): BoolExpr =
      and(commands.commands.map(visibleCommand(commands.disclosures, commands.participantId, _)))

    and(ledger.map(visibleCommands))
  }

  private def hideCreatedContractsInSiblings(ledger: Ledger): BoolExpr = {
    def hideInCommands(commands: Commands): BoolExpr =
      hideInCommandList(commands.commands)

    def hideInCommandList(commands: List[Command]): BoolExpr = commands match {
      case Nil => ctx.mkBool(true)
      case command :: commands =>
        val constraints = for {
          contractId <- collectCreatedContractIds(command)
          reference <- commands.flatMap(collectReferences)
        } yield ctx.mkNot(ctx.mkEq(contractId, reference))
        ctx.mkAnd(and(constraints.toList), hideInCommandList(commands))
    }

    and(ledger.map(hideInCommands))
  }

  private def authorized(
      signatoriesOf: FuncDecl[PartySetSort],
      observersOf: FuncDecl[PartySetSort],
      ledger: Ledger,
  ): BoolExpr = {

    def authorizedCommands(commands: Commands): BoolExpr =
      and(commands.commands.map(authorizedCommand(commands.actAs, _)))

    def authorizedCommand(actAs: PartySet, command: Command): BoolExpr = command match {
      case Command(_, action) => authorizedAction(actAs, action)
    }

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
      case FetchByKey(contractId, _, _) =>
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
      case LookupByKey(_, _, maintainers) =>
        ctx.mkSetSubset(maintainers, actAs)
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

  private val allPartiesSetLiteral = toSet(partySort, (1 to numParties).map(ctx.mkInt))

  private def partySetsWellFormed(ledger: Ledger, allParties: PartySet): BoolExpr =
    and(collectPartySets(ledger).map(s => ctx.mkSetSubset(s, allParties)))

  private def nonEmptyPartySetsWellFormed(ledger: Ledger): BoolExpr =
    and(
      collectNonEmptyPartySets(ledger)
        .map(s => ctx.mkNot(isEmptyPartySet(s)))
    )

  private def packageIdsWellFormed(ledger: Ledger): BoolExpr = {
    val packageIds = collectPackageIds(ledger)
    and(
      packageIds
        .map(packageId =>
          ctx.mkAnd(
            ctx.mkGe(packageId, ctx.mkInt(0)),
            ctx.mkLt(packageId, ctx.mkInt(numPackages)),
          )
        )
        .toSeq
    )
  }

  private def maintainersAreSubsetsOfSignatories(ledger: Ledger): BoolExpr = {
    def validCommands(commands: Commands): BoolExpr = {
      and(commands.commands.map(validCommand))
    }

    def validCommand(command: Command): BoolExpr = command match {
      case Command(_, action) => validAction(action)
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
      case FetchByKey(_, _, _) =>
        ctx.mkTrue()
      case LookupByKey(_, _, _) =>
        ctx.mkTrue()
      case Rollback(subTransaction) =>
        and(subTransaction.map(validAction))
    }

    and(ledger.map(validCommands))
  }

  private def hashConstraint(chunkSize: Int, scenario: Scenario): BoolExpr = {

    val numBitsContractId = {
      1.max((scala.math.log(scenario.ledger.numContracts.toDouble) / scala.math.log(2)).ceil.toInt)
    }

    val numBitsPackageId = (scala.math.log(numPackages.toDouble) / scala.math.log(2)).ceil.toInt

    def contractIdToBits(contractId: ContractId): Seq[BoolExpr] = {
      val bitVector = ctx.mkInt2BV(numBitsContractId, contractId)
      (0 until numBitsContractId).map(i => ctx.mkEq(ctx.mkExtract(i, i, bitVector), ctx.mkBV(1, 1)))
    }

    def partySetToBits(partySet: PartySet): Seq[BoolExpr] = {
      (1 to numParties).map(i => ctx.mkSelect(partySet, ctx.mkInt(i)).asInstanceOf[BoolExpr])
    }

    def contractIdSetToBits(contractIdSet: ContractIdSet): Seq[BoolExpr] = {
      (0 until scenario.ledger.numContracts).map(i =>
        ctx.mkSelect(contractIdSet, ctx.mkInt(i)).asInstanceOf[BoolExpr]
      )
    }

    def packageIdToBits(packageId: PackageId): Seq[BoolExpr] = {
      val bitVector = ctx.mkInt2BV(numBitsPackageId, packageId)
      (0 until numBitsPackageId).map(i => ctx.mkEq(ctx.mkExtract(i, i, bitVector), ctx.mkBV(1, 1)))
    }

    def ledgerToBits(ledger: Ledger): Seq[BoolExpr] = {
      Seq.concat(
        collectReferences(ledger).toSeq.flatMap(contractIdToBits),
        collectPartySets(ledger).flatMap(partySetToBits),
        collectDisclosures(ledger).flatMap(contractIdSetToBits),
        collectPackageIds(ledger).flatMap(packageIdToBits),
      )
    }

    def participantToBits(participant: Participant): Seq[BoolExpr] =
      partySetToBits(participant.parties)

    def topologyToBits(topology: Topology): Seq[BoolExpr] =
      topology.flatMap(participantToBits)

    def scenarioToBits(scenario: Scenario): Seq[BoolExpr] = {
      Seq.concat(
        topologyToBits(scenario.topology),
        ledgerToBits(scenario.ledger),
      )
    }

    def xor(bits: Iterable[BoolExpr]): BoolExpr = bits.reduce((b1, b2) => ctx.mkXor(b1, b2))

    def mkRandomHash(chunkSize: Int, scenario: Scenario): Seq[BoolExpr] =
      Random.shuffle(scenarioToBits(scenario)).grouped(chunkSize).map(xor).toSeq

    and(mkRandomHash(chunkSize, scenario))
  }

  private def niceNumbers(symScenario: Scenario): BoolExpr = {
    val Scenario(symTopology, symLedger) = symScenario
    ctx.mkAnd(
      // Assign IDs 0..n to create events
      numberLedger(symLedger),
      // Make sure that key nums are between 1 and number of creates with keys
      numberkeyIds(symLedger),
      // Assign distinct participant IDs to participants in the topology
      numberParticipants(symTopology),
    )
  }

  private def validScenario(
      constants: Constants,
      allParties: PartySet,
      symScenario: Scenario,
  ): BoolExpr = {
    import constants._
    val Scenario(symTopology, symLedger) = symScenario
    ctx.mkAnd(
      // Assign distinct contract IDs to create actions
      ctx.mkDistinct(collectCreatedContractIds(symLedger).toSeq: _*),
      // Assign distinct participant IDs to participants in the topology
      ctx.mkDistinct(symTopology.map(_.participantId): _*),
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
      // Package IDs are in {0 .. numPackages-1}
      packageIdsWellFormed(symLedger),
    )
  }

  private case class Constants(
      signatoriesOf: FuncDecl[PartySetSort],
      observersOf: FuncDecl[PartySetSort],
      partiesOf: FuncDecl[PartySetSort],
      keyIdsOf: FuncDecl[KeyIdSort],
      maintainersOf: FuncDecl[PartySetSort],
      hasKey: FuncDecl[BoolSort],
  )

  private def mkFreshConstants(): Constants =
    Constants(
      signatoriesOf = ctx.mkFreshFuncDecl("signatories", Array[Sort](contractIdSort), partySetSort),
      observersOf = ctx.mkFreshFuncDecl("observers", Array[Sort](contractIdSort), partySetSort),
      partiesOf = ctx.mkFreshFuncDecl("parties", Array[Sort](participantIdSort), partySetSort),
      keyIdsOf = ctx.mkFreshFuncDecl("key_nums", Array[Sort](contractIdSort), keyIdSort),
      maintainersOf = ctx.mkFreshFuncDecl("maintainers", Array[Sort](contractIdSort), partySetSort),
      hasKey = ctx.mkFreshFuncDecl("has_key", Array[Sort](contractIdSort), ctx.mkBoolSort()),
    )

  private def solve(scenario: Skeletons.Scenario): Option[Ledgers.Scenario] = {
    val symScenario = SkeletonToSymbolic.toSymbolic(ctx, scenario)
    val constants = mkFreshConstants()

    val solver = ctx.mkSolver()
    // val params = ctx.mkParams()
    // params.add("threads", 20)
    // solver.setParameters(params)

    // Constrain the various IDs without loss of generality in order to
    // prevent the generation of different but alpha-equivalent random
    // scenarios.
    solver.add(niceNumbers(symScenario))
    // The scenario is valid
    val allParties = ctx.mkFreshConst("all_parties", partySetSort).asInstanceOf[PartySet]
    solver.add(
      ctx.mkAnd(
        ctx.mkEq(allParties, allPartiesSetLiteral),
        validScenario(constants, allParties, symScenario),
      )
    )
    // Equate a random hash of all the symbols to a constant
    solver.add(hashConstraint(20, symScenario))

    solver.check() match {
      case SATISFIABLE =>
        Some(
          new FromSymbolic(numPackages, numParties, ctx, solver.getModel).toConcrete(symScenario)
        )
      case UNSATISFIABLE =>
        print(".")
        None
      case other =>
        throw new IllegalStateException(
          s"Unexpected solver result: $other, ${solver.getReasonUnknown}"
        )
    }
  }

  private def validate(scenario: Ledgers.Scenario): Boolean = {
    // Global.setParameter("verbose", "1000")
    val solver = ctx.mkSolver()
    val params = ctx.mkParams()
    params.add("proof", false)
    params.add("model", false)
    params.add("unsat_core", false)
    params.add("timeout", 5000)
    solver.setParameters(params)

    val symScenario = ConcreteToSymbolic.toSymbolic(ctx, scenario)
    val allParties = union(partySort, symScenario.topology.map(_.parties))
    val constants = mkFreshConstants()

    solver.check(validScenario(constants, allParties, symScenario)) match {
      case SATISFIABLE => true
      case UNSATISFIABLE =>
        false
      case UNKNOWN =>
        throw new IllegalStateException(s"Unexpected solver result: ${solver.getReasonUnknown}")
    }
  }
}
