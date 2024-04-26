// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import com.daml.lf.model.test.{Skeletons => Skel, Symbolic => Sym}
import com.microsoft.z3.Context

import scala.annotation.nowarn

object SkeletonToSymbolic {
  def toSymbolic(ctx: Context, skeleton: Skel.Scenario): Sym.Scenario =
    new Converter(ctx).toSymbolic(skeleton)

  private class Converter(ctx: Context) {

    private val partySetSort: Sym.PartySetSort = ctx.mkSetSort(ctx.mkIntSort())
    private val contractIdSetSort: Sym.ContractIdSetSort = ctx.mkSetSort(ctx.mkIntSort())
    private val contractIdSort: Sym.ContractIdSort = ctx.mkIntSort()
    private val participantIdSort: Sym.ParticipantIdSort = ctx.mkIntSort()

    private def mkFreshParticipantId(): Sym.ParticipantId =
      ctx.mkFreshConst("p", participantIdSort).asInstanceOf[Sym.ContractId]

    private def mkFreshContractId(): Sym.ContractId =
      ctx.mkFreshConst("c", contractIdSort).asInstanceOf[Sym.ContractId]

    private def mkFreshkeyId(): Sym.KeyId =
      ctx.mkFreshConst("ki", contractIdSort).asInstanceOf[Sym.KeyId]

    private def mkFreshPartySet(name: String): Sym.PartySet =
      ctx.mkFreshConst(name, partySetSort).asInstanceOf[Sym.PartySet]

    private def mkFreshContractIdSet(name: String): Sym.ContractIdSet =
      ctx.mkFreshConst(name, contractIdSetSort).asInstanceOf[Sym.ContractIdSet]

    private def toSymbolic(commands: Skel.Commands): Sym.Commands =
      Sym.Commands(
        mkFreshParticipantId(),
        mkFreshPartySet("a"),
        mkFreshContractIdSet("d"),
        commands.actions.map(toSymbolic),
      )

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
      case Skel.CreateWithKey() =>
        Sym.CreateWithKey(
          mkFreshContractId(),
          mkFreshkeyId(),
          mkFreshPartySet("m"),
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
      case Skel.ExerciseByKey(kind, subTransaction) =>
        Sym.ExerciseByKey(
          toSymbolic(kind),
          mkFreshContractId(),
          mkFreshkeyId(),
          mkFreshPartySet("m"),
          mkFreshPartySet("k"),
          mkFreshPartySet("q"),
          subTransaction.map(toSymbolic),
        )
      case Skel.Fetch() =>
        Sym.Fetch(mkFreshContractId())
      case Skel.FetchByKey() =>
        Sym.FetchByKey(
          mkFreshContractId(),
          mkFreshkeyId(),
          mkFreshPartySet("m"),
        )
      case Skel.LookupByKey(successful) =>
        Sym.LookupByKey(
          if (successful) Some(mkFreshContractId()) else None,
          mkFreshkeyId(),
          mkFreshPartySet("m"),
        )
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

}
