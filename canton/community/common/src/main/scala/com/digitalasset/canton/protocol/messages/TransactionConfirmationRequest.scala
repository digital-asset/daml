// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.ViewType
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  MediatorsOfDomain,
  OpenEnvelope,
  ParticipantsOfParty,
  Recipients,
}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

/** Represents the confirmation request for a transaction as sent from a participant node to the sequencer.
  */
final case class TransactionConfirmationRequest(
    informeeMessage: InformeeMessage,
    viewEnvelopes: Seq[OpenEnvelope[TransactionViewMessage]],
    protocolVersion: ProtocolVersion,
) extends PrettyPrinting
    with HasLoggerName {

  def mediator: MediatorsOfDomain = informeeMessage.mediator

  lazy val rootHashMessage: RootHashMessage[EmptyRootHashMessagePayload.type] = RootHashMessage(
    rootHash = informeeMessage.fullInformeeTree.transactionId.toRootHash,
    domainId = informeeMessage.domainId,
    viewType = ViewType.TransactionViewType,
    payload = EmptyRootHashMessagePayload,
    protocolVersion = protocolVersion,
  )

  def asBatch(ipsSnapshot: TopologySnapshot)(implicit
      loggingContext: NamedLoggingContext,
      executionContext: ExecutionContext,
  ): Future[Batch[DefaultOpenEnvelope]] = {
    val mediatorEnvelope: DefaultOpenEnvelope =
      OpenEnvelope(informeeMessage, Recipients.cc(mediator))(protocolVersion)

    val informees = informeeMessage.allInformees
    RootHashMessageRecipients.rootHashRecipientsForInformees(informees, ipsSnapshot).map {
      recipientsOfRootHashMessage =>
        val rootHashMessageEnvelopes =
          NonEmpty.from(recipientsOfRootHashMessage) match {
            case Some(recipientsNE) =>
              // TODO(#13883) Use BCC also for group addresses
              // val groupsWithMediator =
              //   recipientsOfRootHashMessage.map(recipient => NonEmpty(Set, recipient, mediatorRecipient))
              // val rootHashMessageEnvelope = OpenEnvelope(
              //   rootHashMessage,
              //   Recipients.recipientGroups(NonEmptyUtil.fromUnsafe(groupsWithMediator)),
              // )(protocolVersion)
              val groupAddressing = recipientsOfRootHashMessage.exists {
                case ParticipantsOfParty(_) => true
                case _ => false
              }
              // if using group addressing, we just place all recipients in one group instead of separately as before (it was separate for legacy reasons)
              val rootHashMessageRecipients =
                if (groupAddressing)
                  Recipients.recipientGroups(
                    NonEmpty.mk(Seq, recipientsNE.toSet ++ Seq(mediator))
                  )
                else
                  Recipients.recipientGroups(
                    recipientsNE.map(NonEmpty.mk(Set, _, mediator))
                  )
              List(OpenEnvelope(rootHashMessage, rootHashMessageRecipients)(protocolVersion))
            case None =>
              loggingContext.warn("Confirmation request without root hash message recipients")
              List.empty
          }
        val envelopes =
          rootHashMessageEnvelopes ++ (viewEnvelopes.toList: List[DefaultOpenEnvelope])
        Batch(mediatorEnvelope +: envelopes, protocolVersion)
    }
  }

  override def pretty: Pretty[TransactionConfirmationRequest] = prettyOfClass(
    param("informee message", _.informeeMessage),
    param("view envelopes", _.viewEnvelopes),
  )
}
