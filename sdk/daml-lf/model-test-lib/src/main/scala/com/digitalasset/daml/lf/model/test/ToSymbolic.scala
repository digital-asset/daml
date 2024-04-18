// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import com.daml.lf.model.test.{Skeletons => Skel, Symbolic => Sym}
import com.microsoft.z3.{Context, StringSymbol}

import scala.annotation.nowarn
import scala.collection.mutable

class ToSymbolic(ctx: Context) {

  private val partySetSort: Sym.PartySetSort = ctx.mkSetSort(ctx.mkIntSort())
  private val contractIdSort: Sym.ContractIdSort = ctx.mkIntSort()
  private val participantIdSort: Sym.ParticipantIdSort = ctx.mkIntSort()

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

  private def mkFreshParticipantId(): Sym.ParticipantId =
    ctx.mkConst(mkFreshSymbol("p"), participantIdSort).asInstanceOf[Sym.ContractId]

  private def mkFreshContractId(): Sym.ContractId =
    ctx.mkConst(mkFreshSymbol("c"), contractIdSort).asInstanceOf[Sym.ContractId]

  private def mkFreshPartySet(name: String): Sym.PartySet =
    ctx.mkConst(mkFreshSymbol(name), partySetSort).asInstanceOf[Sym.PartySet]

  private def toSymbolic(commands: Skel.Commands): Sym.Commands =
    Sym.Commands(mkFreshParticipantId(), mkFreshPartySet("a"), commands.actions.map(toSymbolic))

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

  @nowarn("cat=unused")
  def toSymbolic(participant: Skel.Participant): Sym.Participant =
    Sym.Participant(mkFreshParticipantId(), mkFreshPartySet("ps"))

  def toSymbolic(topology: Skel.Topology): Sym.Topology =
    topology.map(toSymbolic)

  def toSymbolic(scenario: Skel.Scenario): Sym.Scenario =
    Sym.Scenario(toSymbolic(scenario.topology), toSymbolic(scenario.ledger))
}
