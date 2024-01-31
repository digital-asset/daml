// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.alternative.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.sequencing.protocol.{
  MemberRecipient,
  ParticipantsOfParty,
  Recipient,
  Recipients,
  RecipientsTree,
}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{MediatorRef, ParticipantId, PartyId}
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*

import scala.concurrent.{ExecutionContext, Future}

object RootHashMessageRecipients extends HasLoggerName {

  def rootHashRecipientsForInformees(
      informees: Set[LfPartyId],
      ipsSnapshot: TopologySnapshot,
  )(implicit
      loggingContext: NamedLoggingContext,
      executionContext: ExecutionContext,
  ): Future[Seq[Recipient]] = {
    val informeesList = informees.toList
    for {
      participantsAddressedByInformees <- ipsSnapshot
        .activeParticipantsOfAll(informeesList)
        .valueOr(informeesWithoutParticipants =>
          ErrorUtil.internalError(
            new IllegalArgumentException(
              show"Informees without participants: $informeesWithoutParticipants"
            )
          )
        )
      groupAddressedInformees <- ipsSnapshot.partiesWithGroupAddressing(informeesList)
      participantsOfGroupAddressedInformees <- ipsSnapshot.activeParticipantsOfParties(
        groupAddressedInformees.toList
      )
    } yield {
      // If there are several group-addressed informees with overlapping participants,
      // we actually look for a set cover. It doesn't matter which one we pick.
      //
      // It might be tempting to optimize even further by considering group addresses
      // that resolve to a subset of the participants to be addressed---even if the party behind
      // the group address is not an informee.  This would work in terms of reducing the bandwidth required for spelling out the recipients.
      // But it would violate privacy, as the following example shows:
      // Let the parties A and B be informees of a request. Party A is hosted on participants P1 and P2 with group addressing,
      // and party B is hosted on participants P2 and P3 with group addressing.
      // Participants P1, P2 and P3 also host party C with group addressing, which is not an informee.
      // Then, it is bandwidth-optimal to address the root hash message to C.
      // However, since the recipients learn about the used address, P1 can infer that P3 is involved in the transaction,
      // but if the views for A are disjoint from the views for B, then P1 should not be able to learn that P3 is involved.
      // By sticking to the informees of the original request,
      // we avoid such a leak.
      //
      // Note: The SetCover below merely optimizes the size of the submission request. If we wanted to take the
      // size of the delivered messages into account as well, we would compute a weighted set cover.
      // For example, let the only informees A and B of a request be hosted on
      // P1, P2, P3, P4 and P2, P3, P4, P5, P6, respectively, with group addressing.
      // Then the root hash message will be addressed to A and B. This gives the following recipients
      // on the root hash message envelope:
      // - Submission request: {med, A}, {med, B}
      // - Envelope delivered to the mediator, P2, P3, P4: {med, A}, {med, B}
      // - Envelope delivered to P1: {med, A}
      // - Envelope delivered to P5, P6: {med, B}
      // This makes 4 + 4 * 4 + 2 + 2 * 2 = 26 recipients in total.
      //
      // In contrast, if the recipients for the root hash message were A, P5, P6, we would get the following situation:
      // - Submission request: {med, A}, {med, P5}, {med, P6}
      // - Envelope delivered to the mediator: {med, A}, {med, P5}, {med, P6}
      // - Envelope delivered to P1, P2, P3, P4: {med, A}
      // - Envelope delivered to P5: {med, P5}
      // - Envelope delivered to P6: {med, P6}
      // This makes 6 + 6 + 4 * 2 + 2 + 2 = 24 recipients in total.

      val groupAddressedParticipants = participantsOfGroupAddressedInformees.values.flatten.toSet
      val directlyAddressedParticipants =
        participantsAddressedByInformees -- groupAddressedParticipants
      val sets = participantsOfGroupAddressedInformees.map { case (party, participants) =>
        ParticipantsOfParty(PartyId.tryFromLfParty(party)) -> participants
      } ++ directlyAddressedParticipants.map { participant =>
        MemberRecipient(participant) -> Set(participant)
      }
      // TODO(#13883) Use a set cover for the recipients instead of all of them
      //  SetCover.greedy(sets.toMap)
      sets.map { case (recipient, _) => recipient }.toSeq
    }
  }

  def recipientsAreValid(
      recipients: Recipients,
      participantId: ParticipantId,
      mediator: MediatorRef,
      participantIsAddressByPartyGroupAddress: (
          Seq[LfPartyId],
          ParticipantId,
      ) => FutureUnlessShutdown[Boolean],
  ): FutureUnlessShutdown[Boolean] =
    recipients.asSingleGroup match {
      case Some(group) =>
        if (group == NonEmpty.mk(Set, MemberRecipient(participantId), mediator.toRecipient))
          FutureUnlessShutdown.pure(true)
        else if (group.contains(mediator.toRecipient) && group.size >= 2) {
          val informeeParty = group.collect { case ParticipantsOfParty(party) =>
            party.toLf
          }
          if (informeeParty.isEmpty) FutureUnlessShutdown.pure(false)
          else
            participantIsAddressByPartyGroupAddress(
              informeeParty.toSeq,
              participantId,
            )
        } else FutureUnlessShutdown.pure(false)
      case _ => FutureUnlessShutdown.pure(false)
    }

  def wrongAndCorrectRecipients(
      recipientsList: Seq[Recipients],
      mediator: MediatorRef,
  ): (Seq[RecipientsTree], Seq[NonEmpty[Set[Recipient]]]) = {
    val (wrongRecipients, correctRecipients) = recipientsList.flatMap { recipients =>
      recipients.trees.toList.map {
        case tree @ RecipientsTree(group, Seq()) =>
          val participantCount = group.count {
            case MemberRecipient(_: ParticipantId) => true
            case _ => false
          }
          val groupAddressCount = group.count {
            case ParticipantsOfParty(_) => true
            case _ => false
          }
          val groupAddressingBeingUsed = groupAddressCount > 0
          Either.cond(
            ((group.size == 2) || (groupAddressingBeingUsed && group.size >= 2)) && group.contains(
              mediator.toRecipient
            ) && (participantCount + groupAddressCount > 0),
            group,
            tree,
          )
        case badTree => Left(badTree)
      }
    }.separate
    (wrongRecipients, correctRecipients)
  }

  def wrongMembers(
      rootHashMessagesRecipients: Seq[Recipient],
      request: MediatorRequest,
      topologySnapshot: TopologySnapshot,
  )(implicit executionContext: ExecutionContext): Future[WrongMembers] = {
    val informeesAddressedAsGroup = rootHashMessagesRecipients.collect {
      case ParticipantsOfParty(informee) =>
        informee.toLf
    }
    val participants = rootHashMessagesRecipients.collect {
      case MemberRecipient(p: ParticipantId) => p
    }
    val informeesNotAddressedAsGroups = request.allInformees -- informeesAddressedAsGroup.toSet
    val superfluousInformees = informeesAddressedAsGroup.toSet -- request.allInformees
    for {
      allNonGroupAddressedInformeeParticipants <-
        informeesNotAddressedAsGroups.toList
          .parTraverse(topologySnapshot.activeParticipantsOf)
          .map(_.flatMap(_.keySet).toSet)
      participantsAddressedAsGroup <- informeesAddressedAsGroup.toList
        .parTraverse(topologySnapshot.activeParticipantsOf)
        .map(_.flatMap(_.keySet).toSet)
    } yield {
      val participantsSet = participants.toSet
      val missingInformeeParticipants =
        allNonGroupAddressedInformeeParticipants diff participantsSet diff participantsAddressedAsGroup
      val superfluousMembers = participantsSet diff allNonGroupAddressedInformeeParticipants
      WrongMembers(missingInformeeParticipants, superfluousMembers, superfluousInformees)
    }
  }

  final case class WrongMembers(
      missingInformeeParticipants: Set[ParticipantId],
      superfluousMembers: Set[ParticipantId],
      superfluousInformees: Set[LfPartyId],
  )
}
