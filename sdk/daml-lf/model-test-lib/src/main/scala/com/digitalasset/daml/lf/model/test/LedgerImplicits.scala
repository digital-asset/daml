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
}
