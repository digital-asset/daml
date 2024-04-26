// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.model.test.{Ledgers => Conc, Symbolic => Sym}
import com.microsoft.z3.{ArrayExpr, BoolSort, Context, IntSort}

object ConcreteToSymbolic {
  def toSymbolic(ctx: Context, scenario: Conc.Scenario): Sym.Scenario =
    new Converter(ctx).toSymbolic(scenario)

  private class Converter(ctx: Context) {

    private def toSymbolic(set: Set[Int]): ArrayExpr[IntSort, BoolSort] =
      set
        .foldLeft(ctx.mkEmptySet(ctx.mkIntSort())) { (acc, elem) =>
          ctx.mkSetAdd(acc, ctx.mkInt(elem))
        }
        .asInstanceOf[ArrayExpr[IntSort, BoolSort]]

    private def toSymbolic(commands: Conc.Commands): Sym.Commands =
      Sym.Commands(
        ctx.mkInt(commands.participantId),
        toSymbolic(commands.actAs),
        toSymbolic(commands.disclosures),
        commands.actions.map(toSymbolic),
      )

    private def toSymbolic(kind: Conc.ExerciseKind): Sym.ExerciseKind = {
      kind match {
        case Conc.Consuming => Sym.Consuming
        case Conc.NonConsuming => Sym.NonConsuming
      }
    }

    private def toSymbolic(action: Conc.Action): Sym.Action = action match {
      case create: Conc.Create =>
        Sym.Create(
          ctx.mkInt(create.contractId),
          toSymbolic(create.signatories),
          toSymbolic(create.observers),
        )
      case create: Conc.CreateWithKey =>
        Sym.CreateWithKey(
          ctx.mkInt(create.contractId),
          ctx.mkInt(create.keyId),
          toSymbolic(create.maintainers),
          toSymbolic(create.signatories),
          toSymbolic(create.observers),
        )
      case exe: Conc.Exercise =>
        Sym.Exercise(
          toSymbolic(exe.kind),
          ctx.mkInt(exe.contractId),
          toSymbolic(exe.controllers),
          toSymbolic(exe.choiceObservers),
          exe.subTransaction.map(toSymbolic),
        )
      case exe: Conc.ExerciseByKey =>
        Sym.ExerciseByKey(
          toSymbolic(exe.kind),
          ctx.mkInt(exe.contractId),
          ctx.mkInt(exe.keyId),
          toSymbolic(exe.maintainers),
          toSymbolic(exe.controllers),
          toSymbolic(exe.choiceObservers),
          exe.subTransaction.map(toSymbolic),
        )
      case fetch: Conc.Fetch =>
        Sym.Fetch(ctx.mkInt(fetch.contractId))
      case fetch: Conc.FetchByKey =>
        Sym.FetchByKey(
          ctx.mkInt(fetch.contractId),
          ctx.mkInt(fetch.keyId),
          toSymbolic(fetch.maintainers),
        )
      case lookup: Conc.LookupByKey =>
        Sym.LookupByKey(
          lookup.contractId.map(ctx.mkInt),
          ctx.mkInt(lookup.keyId),
          toSymbolic(lookup.maintainers),
        )
      case rollback: Conc.Rollback =>
        Sym.Rollback(rollback.subTransaction.map(toSymbolic))
    }

    def toSymbolic(ledger: Conc.Ledger): Sym.Ledger =
      ledger.map(toSymbolic)

    def toSymbolic(participant: Conc.Participant): Sym.Participant =
      Sym.Participant(ctx.mkInt(participant.participantId), toSymbolic(participant.parties))

    def toSymbolic(topology: Conc.Topology): Sym.Topology =
      topology.map(toSymbolic)

    def toSymbolic(scenario: Conc.Scenario): Sym.Scenario =
      Sym.Scenario(toSymbolic(scenario.topology), toSymbolic(scenario.ledger))
  }
}
