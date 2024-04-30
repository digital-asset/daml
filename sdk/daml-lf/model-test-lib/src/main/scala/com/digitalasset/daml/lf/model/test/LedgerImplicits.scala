// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import com.daml.lf.model.test.Ledgers.{Participant, ParticipantId, PartyId}

object LedgerImplicits {

  implicit class RichTopology(topology: Ledgers.Topology) {
    def groupedByPartyId: Map[PartyId, Set[Participant]] = {
      topology
        .flatMap(participant => participant.parties.map(party => party -> Set(participant)))
        .groupMapReduce(_._1)(_._2)(_ ++ _)
    }

    def simplify: Map[ParticipantId, Set[PartyId]] = {
      topology
        .map(participant => participant.participantId -> participant.parties)
        .toMap
    }
  }

  implicit class RichSymbolicLedger(ledger: Symbolic.Ledger) {
    def numContracts: Int = {
      def numActionContracts(action: Symbolic.Action): Int = action match {
        case _: Symbolic.Create =>
          1
        case _: Symbolic.CreateWithKey =>
          1
        case exe: Symbolic.Exercise =>
          exe.subTransaction.map(numActionContracts).sum
        case exe: Symbolic.ExerciseByKey =>
          exe.subTransaction.map(numActionContracts).sum
        case _: Symbolic.Fetch =>
          0
        case _: Symbolic.FetchByKey =>
          0
        case _: Symbolic.LookupByKey =>
          0
        case rb: Symbolic.Rollback =>
          rb.subTransaction.map(numActionContracts).sum
      }

      def numCommandContracts(command: Symbolic.Command): Int =
        numActionContracts(command.action)

      def numCommandsContracts(commands: Symbolic.Commands): Int =
        commands.commands.map(numCommandContracts).sum

      ledger.map(numCommandsContracts).sum
    }
  }

  implicit class RichCommands(commands: Ledgers.Commands) {
    def actions: List[Ledgers.Action] = commands.commands.map(_.action)
  }
}
