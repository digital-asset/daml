// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.protocol.RootHash
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  MediatorGroupRecipient,
  OpenEnvelope,
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

  def mediator: MediatorGroupRecipient = informeeMessage.mediator

  lazy val rootHash: RootHash = informeeMessage.fullInformeeTree.transactionId.toRootHash

  private def rootHashMessage(
      submissionTopologyTime: CantonTimestamp
  ): RootHashMessage[EmptyRootHashMessagePayload.type] = RootHashMessage(
    rootHash = rootHash,
    domainId = informeeMessage.domainId,
    viewType = ViewType.TransactionViewType,
    submissionTopologyTime = submissionTopologyTime,
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
              val groupsWithMediator = recipientsNE.map(NonEmpty(Set, _, mediator))
              val rootHashMessageEnvelope = OpenEnvelope(
                rootHashMessage(ipsSnapshot.timestamp),
                Recipients.recipientGroups(groupsWithMediator),
              )(protocolVersion)

              List(rootHashMessageEnvelope)
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
