// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import com.digitalasset.canton.crypto.{SyncCryptoApi, SyncCryptoClient}
import com.digitalasset.canton.sequencing.protocol.{
  AllMembersOfDomain,
  GroupRecipient,
  MediatorGroupRecipient,
  ParticipantsOfParty,
  SequencersOfDomain,
}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{MediatorGroup, Member, PartyId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

final class GroupAddressResolver(cryptoApi: SyncCryptoClient[SyncCryptoApi]) {
  def resolveGroupsToMembers(
      groupRecipients: Set[GroupRecipient],
      topologySnapshot: TopologySnapshot,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Map[GroupRecipient, Set[Member]]] = {
    if (groupRecipients.isEmpty) Future.successful(Map.empty)
    else
      for {
        participantsOfParty <- {
          val parties = groupRecipients.collect { case ParticipantsOfParty(party) =>
            party.toLf
          }
          if (parties.isEmpty)
            Future.successful(Map.empty[GroupRecipient, Set[Member]])
          else
            for {
              mapping <-
                topologySnapshot
                  .activeParticipantsOfParties(parties.toSeq)
            } yield mapping.map[GroupRecipient, Set[Member]] { case (party, participants) =>
              ParticipantsOfParty(
                PartyId.tryFromLfParty(party)
              ) -> participants.toSet[Member]
            }
        }
        mediatorGroupByMember <- {
          val mediatorGroups = groupRecipients.collect { case MediatorGroupRecipient(group) =>
            group
          }.toSeq
          if (mediatorGroups.isEmpty)
            Future.successful(Map.empty[GroupRecipient, Set[Member]])
          else
            for {
              groups <- topologySnapshot
                .mediatorGroupsOfAll(mediatorGroups)
                .leftMap(_ => Seq.empty[MediatorGroup])
                .merge
            } yield groups
              .map(group =>
                MediatorGroupRecipient(group.index) -> (group.active.forgetNE ++ group.passive)
                  .toSet[Member]
              )
              .toMap[GroupRecipient, Set[Member]]
        }
        allRecipients <- {
          if (!groupRecipients.contains(AllMembersOfDomain)) {
            Future.successful(Map.empty[GroupRecipient, Set[Member]])
          } else {
            topologySnapshot
              .allMembers()
              .map(members => Map((AllMembersOfDomain: GroupRecipient, members)))
          }
        }

        sequencersOfDomain <- {
          val useSequencersOfDomain = groupRecipients.contains(SequencersOfDomain)
          if (useSequencersOfDomain) {
            for {
              sequencers <-
                topologySnapshot
                  .sequencerGroup()
                  .map(
                    _.map(group => (group.active.forgetNE ++ group.passive).toSet[Member])
                      .getOrElse(Set.empty[Member])
                  )
            } yield Map((SequencersOfDomain: GroupRecipient) -> sequencers)
          } else
            Future.successful(Map.empty[GroupRecipient, Set[Member]])
        }
      } yield participantsOfParty ++ mediatorGroupByMember ++ sequencersOfDomain ++ allRecipients
  }
}
