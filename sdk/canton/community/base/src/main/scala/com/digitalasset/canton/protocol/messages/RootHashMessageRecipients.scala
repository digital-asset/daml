// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.alternative.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{Checked, ErrorUtil, SetCover}

import scala.concurrent.{ExecutionContext, Future}

object RootHashMessageRecipients extends HasLoggerName {

  /** Computes the list of recipients for the root hash messages of a confirmation request.
    * Each recipient returned is either a participant or a group address
    * [[com.digitalasset.canton.sequencing.protocol.ParticipantsOfParty]].
    * The group addresses can be overlapping, but a participant member recipient will only be present if it is
    * not included in any of the group addresses.
    *
    * @param informees informees of the confirmation request
    * @param ipsSnapshot topology snapshot used at submission time
    * @return list of root hash message recipients
    */
  def rootHashRecipientsForInformees(
      informees: Set[LfPartyId],
      ipsSnapshot: TopologySnapshot,
  )(implicit
      loggingContext: NamedLoggingContext,
      executionContext: ExecutionContext,
  ): Future[Seq[Recipient]] = {
    implicit val tc = loggingContext.traceContext
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
      participantsOfGroupAddressedInformees <- ipsSnapshot
        .activeParticipantsOfPartiesWithGroupAddressing(
          informeesList
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
      SetCover.greedy(sets)
    }
  }

  /** Validate the recipients of root hash messages received by a participant in Phase 3.
    */
  def validateRecipientsOnParticipant(recipients: Recipients): Checked[Nothing, String, Unit] = {
    // group members must be of size 2, which must be participant and mediator, due to previous checks
    val validGroups = recipients.trees.collect {
      case RecipientsTree(group, Seq()) if group.sizeCompare(2) == 0 => group
    }

    if (validGroups.size == recipients.trees.size) {
      val allUseGroupAddressing = validGroups.forall {
        _.exists {
          case ParticipantsOfParty(_) => true
          case _ => false
        }
      }

      // Due to how rootHashRecipientsForInformees() computes recipients, if there is more than one group,
      // they must all address the participant using group addressing.
      if (allUseGroupAddressing || validGroups.sizeCompare(1) == 0) Checked.unit
      else
        Checked.continue(
          s"The root hash message has more than one recipient group, not all using group addressing.\n$recipients"
        )
    } else Checked.continue(s"The root hash message has invalid recipient groups.\n$recipients")
  }

  /** Validate the recipients of root hash messages received by a mediator in Phase 2.
    *
    * A recipient is valid if each recipient tree:
    *   - contains only a single recipient group (no children)
    *   - the recipient group is if size 2
    *   - the recipient group contains:
    *     - the mediator group recipient
    *     - either a participant member recipient or a PartyOfParticipant group recipient
    */
  def wrongAndCorrectRecipients(
      recipientsList: Seq[Recipients],
      mediator: MediatorGroupRecipient,
  ): (Seq[RecipientsTree], Seq[NonEmpty[Set[Recipient]]]) = {
    val (wrongRecipients, correctRecipients) = recipientsList.flatMap { recipients =>
      recipients.trees.toList.map {
        case tree @ RecipientsTree(group, Seq()) =>
          val hasMediator = group.contains(mediator)
          val hasParticipantOrPop = group.exists {
            case MemberRecipient(_: ParticipantId) | ParticipantsOfParty(_) => true
            case _ => false
          }

          Either.cond(
            group.sizeCompare(2) == 0 && hasMediator && hasParticipantOrPop,
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
      request: MediatorConfirmationRequest,
      topologySnapshot: TopologySnapshot,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[WrongMembers] = {
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
      allNonGroupAddressedInformeeParticipants <- topologySnapshot
        .activeParticipantsOfPartiesWithAttributes(informeesNotAddressedAsGroups.toList)
        .map(_.values.flatMap(_.keySet).toSet)
      participantsAddressedAsGroup <-
        topologySnapshot
          .activeParticipantsOfPartiesWithAttributes(informeesAddressedAsGroup.toList)
          .map(_.values.flatMap(_.keySet).toSet)
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
