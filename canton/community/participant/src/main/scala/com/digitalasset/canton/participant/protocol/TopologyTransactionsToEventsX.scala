// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.sync.LedgerSyncEvent
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.SignedTopologyTransactionsX.PositiveSignedTopologyTransactionsX
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{ParticipantId, PartyId}

// TODO(#15087) Design proper events and change implementation
/*
  This is for the PoC only. It does not take into account the domainId nor
  changes in the following attributes: threshold, permissions, ...
 */
private[protocol] final class TopologyTransactionsToEventsX(
    override protected val loggerFactory: NamedLoggerFactory
) extends NamedLogging {

  def events(
      sequenceTime: SequencedTime,
      effectiveTime: EffectiveTime,
      old: PositiveSignedTopologyTransactionsX,
      current: PositiveSignedTopologyTransactionsX,
  ): Seq[LedgerSyncEvent] = {
    val (added, removed) = computePartiesAddedRemoved(old, current)

    def regroup(d: Map[PartyId, Set[ParticipantId]]) = d.toSeq
      .flatMap { case (party, participants) =>
        participants.map((_, party))
      }
      .groupMap { case (participantId, _) => participantId } { case (_, partyId) => partyId }

    val addedEvents = regroup(added).flatMap { case (participant, parties) =>
      NonEmpty.from(parties).map { parties =>
        LedgerSyncEvent.PartiesAddedToParticipant(
          parties.toSet.map(_.toLf),
          participant.toLf,
          sequenceTime.value.toLf,
          effectiveTime.value.toLf,
        )
      }
    }

    val removedEvents = regroup(removed).flatMap { case (participant, parties) =>
      NonEmpty.from(parties).map { parties =>
        LedgerSyncEvent.PartiesRemovedFromParticipant(
          parties.toSet.map(_.toLf),
          participant.toLf,
          sequenceTime.value.toLf,
          effectiveTime.value.toLf,
        )
      }
    }

    (addedEvents.toSeq ++ removedEvents)
  }

  def computePartiesAddedRemoved(
      old: PositiveSignedTopologyTransactionsX,
      current: PositiveSignedTopologyTransactionsX,
  ): (Map[PartyId, Set[ParticipantId]], Map[PartyId, Set[ParticipantId]]) = {
    val oldPartyToParticipants: Map[PartyId, Set[ParticipantId]] =
      getPartyToParticipants(old)

    val currentPartyToParticipants: Map[PartyId, Set[ParticipantId]] =
      getPartyToParticipants(current)

    val newKeys =
      currentPartyToParticipants.keySet.diff(oldPartyToParticipants.keySet)
    val removedKeys =
      oldPartyToParticipants.keySet.diff(currentPartyToParticipants.keySet)
    val sameKeys =
      oldPartyToParticipants.keySet.intersect(currentPartyToParticipants.keySet)

    val newParties = currentPartyToParticipants.filter { case (participant, _) =>
      newKeys.contains(participant)
    }
    val removedParties = oldPartyToParticipants.filter { case (participant, _) =>
      removedKeys.contains(participant)
    }

    val empty = Map.empty[PartyId, Set[ParticipantId]]
    val (added, removed) = sameKeys.foldLeft((empty, empty)) { case ((added, removed), key) =>
      val initialParticipants = oldPartyToParticipants.getOrElse(key, Nil).toSet
      val finalParticipants = currentPartyToParticipants.getOrElse(key, Nil).toSet

      (
        added.+((key, finalParticipants.diff(initialParticipants))),
        removed.+((key, initialParticipants.diff(finalParticipants))),
      )
    }

    def filterOutEmpty(data: Map[PartyId, Set[ParticipantId]]) = data.filter { case (_, values) =>
      values.nonEmpty
    }

    (filterOutEmpty(added ++ newParties), filterOutEmpty(removed ++ removedParties))
  }

  private def getPartyToParticipants(
      state: PositiveSignedTopologyTransactionsX
  ): Map[PartyId, Set[ParticipantId]] =
    state
      .collectOfMapping[PartyToParticipantX]
      .result
      .map(_.mapping)
      .flatMap { mapping =>
        mapping.participants.map(p => (mapping.partyId, p.participantId))
      }
      .groupMap { case (participantId, _) => participantId } { case (_, partyId) => partyId }
      .view
      .mapValues(_.toSet)
      .toMap
}
