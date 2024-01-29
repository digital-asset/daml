// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.data.EitherT
import cats.syntax.alternative.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.messages.EncryptedViewMessageV1.RecipientsInfo
import com.digitalasset.canton.sequencing.protocol.{Recipient, Recipients, RecipientsTree}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{MediatorRef, ParticipantId}
import com.digitalasset.canton.util.FutureInstances.*

import scala.concurrent.{ExecutionContext, Future}

object RootHashMessageRecipients {

  /** Returns a Left if some of the informeeParties don't have active
    * participants, in which case the parties with missing active participants are returned.
    */
  def encryptedViewMessageRecipientsInfo(
      ipsSnapshot: TopologySnapshot,
      informeeParties: List[LfPartyId],
  )(implicit
      ec: ExecutionContext
  ): EitherT[Future, Set[LfPartyId], RecipientsInfo] = for {
    participantsOfInformess <- ipsSnapshot
      .activeParticipantsOfAll(informeeParties)
  } yield RecipientsInfo(
    informeeParticipants = participantsOfInformess
  )

  def confirmationRequestRootHashMessagesRecipients(
      recipientInfos: Seq[RecipientsInfo],
      mediator: MediatorRef,
  ): List[Recipients] = {

    val participantRecipients = recipientInfos.toSet
      .flatMap[ParticipantId](
        _.informeeParticipants
      )
      .map(Recipient(_))

    val recipients = participantRecipients

    NonEmpty
      .from(recipients.toList)
      .map { recipientsNE =>
        Recipients.recipientGroups(
          recipientsNE.map(NonEmpty.mk(Set, _, mediator.toRecipient))
        )
      }
      .toList
  }

  def recipientsAreValid(
      recipients: Recipients,
      participantId: ParticipantId,
      mediator: MediatorRef,
  ): FutureUnlessShutdown[Boolean] =
    recipients.asSingleGroup match {
      case Some(group) =>
        if (group == NonEmpty.mk(Set, Recipient(participantId), mediator.toRecipient))
          FutureUnlessShutdown.pure(true)
        else FutureUnlessShutdown.pure(false)
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
            case Recipient(_: ParticipantId) => true
            case _ => false
          }
          Either.cond(
            (group.size == 2) && group.contains(mediator.toRecipient) && (participantCount > 0),
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
    val participants = rootHashMessagesRecipients.collect { case Recipient(p: ParticipantId) =>
      p
    }
    val informees = request.allInformees
    for {
      allParticipants <-
        informees.toList
          .parTraverse(topologySnapshot.activeParticipantsOf)
          .map(_.flatMap(_.keySet).toSet)
    } yield {
      val participantsSet = participants.toSet
      val missingInformeeParticipants = allParticipants diff participantsSet
      val superfluousMembers = participantsSet diff allParticipants
      WrongMembers(missingInformeeParticipants, superfluousMembers)
    }
  }

  final case class WrongMembers(
      missingInformeeParticipants: Set[ParticipantId],
      superfluousMembers: Set[ParticipantId],
  )
}
