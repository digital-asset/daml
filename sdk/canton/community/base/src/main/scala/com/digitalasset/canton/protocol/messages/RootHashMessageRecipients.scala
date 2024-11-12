// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.alternative.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{Checked, ErrorUtil}

import scala.concurrent.{ExecutionContext, Future}

object RootHashMessageRecipients extends HasLoggerName {

  /** Computes the list of recipients for the root hash messages of a confirmation request.
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
    } yield participantsAddressedByInformees.toSeq.map(MemberRecipient(_))
  }

  /** Validate the recipients of root hash messages received by a participant in Phase 3.
    */
  def validateRecipientsOnParticipant(recipients: Recipients): Checked[Nothing, String, Unit] = {
    // group members must be of size 2, which must be participant and mediator, due to previous checks
    val validGroups = recipients.trees.collect {
      case RecipientsTree(group, Seq()) if group.sizeCompare(2) == 0 => group
    }

    if (validGroups.sizeIs == recipients.trees.size) {
      // Due to how rootHashRecipientsForInformees() computes recipients, there should be only one group
      if (validGroups.sizeCompare(1) == 0) Checked.unit
      else
        Checked.continue(
          s"The root hash message has more than one recipient group.\n$recipients"
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
          val hasParticipant = group.exists {
            case MemberRecipient(_: ParticipantId) => true
            case _ => false
          }

          Either.cond(
            group.sizeCompare(2) == 0 && hasMediator && hasParticipant,
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
  ): FutureUnlessShutdown[WrongMembers] = FutureUnlessShutdown.outcomeF {
    val participants = rootHashMessagesRecipients.collect {
      case MemberRecipient(p: ParticipantId) => p
    }
    for {
      allInformeeParticipants <- topologySnapshot
        .activeParticipantsOfParties(request.allInformees.toList)
        .map(_.values.toSet.flatten)
    } yield {
      val participantsSet = participants.toSet
      val missingInformeeParticipants = allInformeeParticipants diff participantsSet
      val superfluousMembers = participantsSet diff allInformeeParticipants
      WrongMembers(missingInformeeParticipants, superfluousMembers)
    }
  }

  final case class WrongMembers(
      missingInformeeParticipants: Set[ParticipantId],
      superfluousMembers: Set[ParticipantId],
  )
}
