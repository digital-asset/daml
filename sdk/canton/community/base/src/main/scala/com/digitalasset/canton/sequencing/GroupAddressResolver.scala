// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{MediatorGroup, Member, ParticipantId, PartyId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

object GroupAddressResolver {

  def resolveGroupsToMembers(
      groupRecipients: Set[GroupRecipient],
      topologyOrSequencingSnapshot: TopologySnapshot,
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
                topologyOrSequencingSnapshot
                  .activeParticipantsOfParties(parties.toSeq)
            } yield asGroupRecipientsToMembers(mapping)
        }
        mediatorGroupByMember <- {
          val mediatorGroups = groupRecipients.collect { case MediatorGroupRecipient(group) =>
            group
          }.toSeq
          if (mediatorGroups.isEmpty)
            Future.successful(Map.empty[GroupRecipient, Set[Member]])
          else
            for {
              groups <- topologyOrSequencingSnapshot
                .mediatorGroupsOfAll(mediatorGroups)
                .leftMap(_ => Seq.empty[MediatorGroup])
                .merge
            } yield asGroupRecipientsToMembers(groups)
        }
        allRecipients <- {
          if (!groupRecipients.contains(AllMembersOfDomain)) {
            Future.successful(Map.empty[GroupRecipient, Set[Member]])
          } else {
            topologyOrSequencingSnapshot
              .allMembers()
              .map(members => Map((AllMembersOfDomain: GroupRecipient, members)))
          }
        }

        sequencersOfDomain <- {
          val useSequencersOfDomain = groupRecipients.contains(SequencersOfDomain)
          if (useSequencersOfDomain) {
            for {
              sequencers <-
                topologyOrSequencingSnapshot
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

  def asGroupRecipientsToMembers(
      groups: Seq[MediatorGroup]
  ): Map[GroupRecipient, Set[Member]] =
    groups
      .map(group =>
        MediatorGroupRecipient(group.index) -> (group.active.forgetNE ++ group.passive)
          .toSet[Member]
      )
      .toMap[GroupRecipient, Set[Member]]

  def asGroupRecipientsToMembers(
      mapping: Map[LfPartyId, Set[ParticipantId]]
  ): Map[GroupRecipient, Set[Member]] = {
    mapping.map[GroupRecipient, Set[Member]] { case (party, participants) =>
      ParticipantsOfParty(
        PartyId.tryFromLfParty(party)
      ) -> participants.toSet[Member]
    }
  }
}
