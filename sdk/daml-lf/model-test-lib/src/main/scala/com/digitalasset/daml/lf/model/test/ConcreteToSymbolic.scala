// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.model.test.{Ledgers => Conc, Symbolic => Sym}
import com.microsoft.z3.{BoolExpr, Context}

import scala.collection.mutable

object ConcreteToSymbolic {
  def toSymbolic(ctx: Context, scenario: Conc.Scenario): (Sym.Scenario, BoolExpr) = {
    val converter = new Converter(ctx)
    val symScenario = converter.toSymbolic(scenario)
    (symScenario, converter.constraints.foldLeft(ctx.mkTrue())(ctx.mkAnd(_, _)))
  }

  private class Converter(ctx: Context) {

    private val partySetSort: Sym.PartySetSort = ctx.mkSetSort(ctx.mkIntSort())
    private val contractIdSort: Sym.ContractIdSort = ctx.mkIntSort()
    private val keyIdSort: Sym.keyIdSort = ctx.mkIntSort()
    private val participantIdSort: Sym.ParticipantIdSort = ctx.mkIntSort()

    // constraints accumulated during the translation
    val constraints: mutable.ArrayBuffer[BoolExpr] = mutable.ArrayBuffer.empty

    private def mkFreshParticipantId(): Sym.ParticipantId =
      ctx.mkFreshConst("p", participantIdSort).asInstanceOf[Sym.ContractId]

    private def mkFreshContractId(): Sym.ContractId =
      ctx.mkFreshConst("c", contractIdSort).asInstanceOf[Sym.ContractId]

    private def mkFreshKeyId(): Sym.ContractId =
      ctx.mkFreshConst("ki", keyIdSort).asInstanceOf[Sym.keyId]

    private def mkFreshPartySet(name: String): Sym.PartySet =
      ctx.mkFreshConst(name, partySetSort).asInstanceOf[Sym.PartySet]

    private def toSymbolic(parties: Conc.PartySet): Sym.PartySet =
      parties
        .foldLeft(ctx.mkEmptySet(ctx.mkIntSort())) { (acc, party) =>
          ctx.mkSetAdd(acc, ctx.mkInt(party))
        }
        .asInstanceOf[Sym.PartySet]

    private def toSymbolic(commands: Conc.Commands): Sym.Commands = {
      val participantId = mkFreshParticipantId()
      val actAs = mkFreshPartySet("a")
      val _ = constraints += ctx.mkEq(participantId, ctx.mkInt(commands.participantId))
      val _ = constraints += ctx.mkEq(actAs, toSymbolic(commands.actAs))
      Sym.Commands(participantId, actAs, commands.actions.map(toSymbolic))
    }

    private def toSymbolic(kind: Conc.ExerciseKind): Sym.ExerciseKind = {
      kind match {
        case Conc.Consuming => Sym.Consuming
        case Conc.NonConsuming => Sym.NonConsuming
      }
    }

    private def toSymbolic(action: Conc.Action): Sym.Action = action match {
      case create: Conc.Create =>
        val contractId = mkFreshContractId()
        val signatories = mkFreshPartySet("s")
        val observers = mkFreshPartySet("o")
        val _ = constraints += ctx.mkEq(contractId, ctx.mkInt(create.contractId))
        val _ = constraints += ctx.mkEq(signatories, toSymbolic(create.signatories))
        val _ = constraints += ctx.mkEq(observers, toSymbolic(create.observers))
        Sym.Create(
          mkFreshContractId(),
          mkFreshPartySet("s"),
          mkFreshPartySet("o"),
        )
      case create: Conc.CreateWithKey =>
        val contractId = mkFreshContractId()
        val keyId = mkFreshKeyId()
        val maintainers = mkFreshPartySet("m")
        val signatories = mkFreshPartySet("s")
        val observers = mkFreshPartySet("o")
        val _ = constraints += ctx.mkEq(contractId, ctx.mkInt(create.contractId))
        val _ = constraints += ctx.mkEq(keyId, ctx.mkInt(create.keyId))
        val _ = constraints += ctx.mkEq(signatories, toSymbolic(create.signatories))
        val _ = constraints += ctx.mkEq(observers, toSymbolic(create.observers))
        val _ = constraints += ctx.mkEq(maintainers, toSymbolic(create.maintainers))
        Sym.CreateWithKey(
          contractId,
          keyId,
          maintainers,
          signatories,
          observers,
        )
      case exe: Conc.Exercise =>
        val contractId = mkFreshContractId()
        val controllers = mkFreshPartySet("k")
        val choiceObservers = mkFreshPartySet("q")
        val _ = constraints += ctx.mkEq(contractId, ctx.mkInt(exe.contractId))
        val _ = constraints += ctx.mkEq(controllers, toSymbolic(exe.controllers))
        val _ = constraints += ctx.mkEq(choiceObservers, toSymbolic(exe.choiceObservers))
        Sym.Exercise(
          toSymbolic(exe.kind),
          contractId,
          controllers,
          choiceObservers,
          exe.subTransaction.map(toSymbolic),
        )
      case exe: Conc.ExerciseByKey =>
        val contractId = mkFreshContractId()
        val keyId = mkFreshKeyId()
        val maintainers = mkFreshPartySet("m")
        val controllers = mkFreshPartySet("k")
        val choiceObservers = mkFreshPartySet("q")
        val _ = constraints += ctx.mkEq(contractId, ctx.mkInt(exe.contractId))
        val _ = constraints += ctx.mkEq(keyId, ctx.mkInt(exe.keyId))
        val _ = constraints += ctx.mkEq(controllers, toSymbolic(exe.controllers))
        val _ = constraints += ctx.mkEq(choiceObservers, toSymbolic(exe.choiceObservers))
        val _ = constraints += ctx.mkEq(maintainers, toSymbolic(exe.maintainers))
        Sym.ExerciseByKey(
          toSymbolic(exe.kind),
          contractId,
          keyId,
          maintainers,
          controllers,
          choiceObservers,
          exe.subTransaction.map(toSymbolic),
        )
      case fetch: Conc.Fetch =>
        val contractId = mkFreshContractId()
        val _ = constraints += ctx.mkEq(contractId, ctx.mkInt(fetch.contractId))
        Sym.Fetch(contractId)
      case lookup: Conc.LookupByKey =>
        val keyId = mkFreshKeyId()
        val maintainers = mkFreshPartySet("m")
        val _ = constraints += ctx.mkEq(keyId, ctx.mkInt(lookup.keyId))
        val _ = constraints += ctx.mkEq(maintainers, toSymbolic(lookup.maintainers))
        lookup.contractId match {
          case Some(cid) =>
            val conctractId = mkFreshContractId()
            val _ = constraints += ctx.mkEq(conctractId, ctx.mkInt(cid))
            Sym.LookupByKey(Some(ctx.mkInt(cid)), keyId, maintainers)
          case None =>
            Sym.LookupByKey(None, keyId, maintainers)
        }
      case rollback: Conc.Rollback =>
        Sym.Rollback(rollback.subTransaction.map(toSymbolic))
    }

    def toSymbolic(ledger: Conc.Ledger): Sym.Ledger =
      ledger.map(toSymbolic)

    def toSymbolic(participant: Conc.Participant): Sym.Participant = {
      val participantId = mkFreshParticipantId()
      val parties = mkFreshPartySet("ps")
      val _ = constraints += ctx.mkEq(participantId, ctx.mkInt(participant.participantId))
      val _ = constraints += ctx.mkEq(parties, toSymbolic(participant.parties))
      Sym.Participant(participantId, parties)
    }

    def toSymbolic(topology: Conc.Topology): Sym.Topology =
      topology.map(toSymbolic)

    def toSymbolic(scenario: Conc.Scenario): Sym.Scenario =
      Sym.Scenario(toSymbolic(scenario.topology), toSymbolic(scenario.ledger))
  }
}
