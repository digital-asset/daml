// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.data.EitherT
import cats.syntax.alternative.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.messages.EncryptedViewMessage.RecipientsInfo
import com.digitalasset.canton.sequencing.protocol.{
  MemberRecipient,
  ParticipantsOfParty,
  Recipient,
  Recipients,
  RecipientsTree,
}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{MediatorRef, ParticipantId, PartyId}
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
    partiesWithGroupAddressing <- EitherT.right(
      ipsSnapshot.partiesWithGroupAddressing(informeeParties)
    )
    participantsOfInformessWithoutGroupAddressing <- ipsSnapshot
      .activeParticipantsOfAll((informeeParties.toSet -- partiesWithGroupAddressing).toList)
    participantsCoveredByGroupAddressing <- ipsSnapshot
      .activeParticipantsOfAll(partiesWithGroupAddressing.toList)
  } yield RecipientsInfo(
    informeeParticipants =
      participantsOfInformessWithoutGroupAddressing -- participantsCoveredByGroupAddressing,
    partiesWithGroupAddressing.map(PartyId.tryFromLfParty),
    participantsCoveredByGroupAddressing,
  )

  def confirmationRequestRootHashMessagesRecipients(
      recipientInfos: Seq[RecipientsInfo],
      mediator: MediatorRef,
  ): List[Recipients] = {

    val participantRecipients = {
      val participantsAddressedByGroupAddress =
        recipientInfos.toSet.flatMap[ParticipantId](
          _.participantsAddressedByGroupAddress
        )
      val allInformeeParticipants = recipientInfos.toSet.flatMap[ParticipantId](
        _.informeeParticipants
      )
      allInformeeParticipants -- participantsAddressedByGroupAddress
    }.map(MemberRecipient)

    val groupRecipients = recipientInfos.toSet
      .flatMap[PartyId](_.partiesWithGroupAddressing)
      .map(p => ParticipantsOfParty(p))

    val recipients = participantRecipients ++ groupRecipients
    val groupAddressingBeingUsed = groupRecipients.nonEmpty

    NonEmpty
      .from(recipients.toList)
      .map { recipientsNE =>
        if (groupAddressingBeingUsed) {
          // if using group addressing, we just place all recipients in one group instead of separately as before (it was separate for legacy reasons)
          val mediatorSet: NonEmpty[Set[Recipient]] = NonEmpty.mk(Set, mediator.toRecipient)
          Recipients.recipientGroups(
            NonEmpty
              .mk(Seq, recipientsNE.toSet ++ mediatorSet)
          )
        } else
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
