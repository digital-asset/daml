// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import com.microsoft.z3.{Context, IntSort, StringSymbol}
import com.daml.lf.model.test.{Symbolic => Sym}
import com.daml.lf.model.test.{Skeletons => Skel}

import scala.collection.mutable

class ToSymbolic(ctx: Context) {

  private val partySetSort = ctx.mkSetSort(ctx.mkIntSort())
  private val contractIdSort: IntSort = ctx.mkIntSort()

  private val counters: mutable.Map[String, Int] = mutable.HashMap.empty

  private def mkFreshSymbol(name: String): StringSymbol =
    ctx.mkSymbol({
      counters.get(name) match {
        case Some(i) =>
          counters.update(name, i + 1)
          s"${name}$i"
        case None =>
          counters.update(name, 1)
          s"${name}0"
      }
    })

  private def mkFreshContractId(): Sym.ContractId =
    ctx.mkConst(mkFreshSymbol("c"), contractIdSort).asInstanceOf[Sym.ContractId]

  private def mkFreshPartySet(name: String): Sym.PartySet =
    ctx.mkConst(mkFreshSymbol(name), partySetSort).asInstanceOf[Sym.PartySet]

  private def toSymbolic(commands: Skel.Commands): Sym.Commands =
    Sym.Commands(mkFreshPartySet("a"), commands.actions.map(toSymbolic))

  private def toSymbolic(kind: Skel.ExerciseKind): Sym.ExerciseKind = {
    kind match {
      case Skel.Consuming => Sym.Consuming
      case Skel.NonConsuming => Sym.NonConsuming
    }
  }

  private def toSymbolic(action: Skel.Action): Sym.Action = action match {
    case Skel.Create() =>
      Sym.Create(
        mkFreshContractId(),
        mkFreshPartySet("s"),
        mkFreshPartySet("o"),
      )
    case Skel.Exercise(kind, subTransaction) =>
      Sym.Exercise(
        toSymbolic(kind),
        mkFreshContractId(),
        mkFreshPartySet("k"),
        mkFreshPartySet("q"),
        subTransaction.map(toSymbolic),
      )
    case Skel.Fetch() =>
      Sym.Fetch(mkFreshContractId())
    case Skel.Rollback(subTransaction) =>
      Sym.Rollback(subTransaction.map(toSymbolic))
  }

  def toSymbolic(ledger: Skel.Ledger): Sym.Ledger =
    ledger.map(toSymbolic)
}
