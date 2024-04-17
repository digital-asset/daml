// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import com.daml.lf.model.test.{Ledgers => L, Symbolic => S}
import com.microsoft.z3.enumerations.Z3_lbool
import com.microsoft.z3.{Context, IntNum, Model}

class FromSymbolic(numParties: Int, ctx: Context, model: Model) {

  private def evalContractId(cid: S.ContractId): L.ContractId =
    model.evaluate(cid, false).asInstanceOf[IntNum].getInt

  private def evalPartySet(set: S.PartySet): L.PartySet =
    (1 to numParties).toSet.filter { i =>
      model
        .evaluate(ctx.mkSelect(set, ctx.mkInt(i)), false)
        .getBoolValue == Z3_lbool.Z3_L_TRUE
    }

  private def toConcrete(commands: S.Commands): L.Commands =
    L.Commands(0, evalPartySet(commands.actAs), commands.actions.map(toConcrete))

  private def toConcrete(kind: S.ExerciseKind): L.ExerciseKind = {
    kind match {
      case S.Consuming => L.Consuming
      case S.NonConsuming => L.NonConsuming
    }
  }

  private def toConcrete(action: S.Action): L.Action = action match {
    case Symbolic.Create(contractId, signatories, observers) =>
      L.Create(
        evalContractId(contractId),
        evalPartySet(signatories),
        evalPartySet(observers),
      )
    case Symbolic.Exercise(kind, contractId, controllers, choiceObservers, subTransaction) =>
      L.Exercise(
        toConcrete(kind),
        evalContractId(contractId),
        evalPartySet(controllers),
        evalPartySet(choiceObservers),
        subTransaction.map(toConcrete),
      )
    case Symbolic.Fetch(contractId) =>
      L.Fetch(evalContractId(contractId))
    case Symbolic.Rollback(subTransaction) =>
      L.Rollback(subTransaction.map(toConcrete))
  }

  def toConcrete(ledger: S.Ledger): L.Ledger =
    ledger.map(toConcrete)
}
