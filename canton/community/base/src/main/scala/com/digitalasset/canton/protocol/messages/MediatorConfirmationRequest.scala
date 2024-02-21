// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.{Informee, ViewPosition, ViewType}
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.{RequestId, RootHash}
import com.digitalasset.canton.sequencing.protocol.MediatorsOfDomain
import com.digitalasset.canton.topology.ParticipantId

import java.util.UUID

trait MediatorConfirmationRequest extends UnsignedProtocolMessage {
  def requestUuid: UUID

  def mediator: MediatorsOfDomain

  def informeesAndThresholdByViewPosition: Map[ViewPosition, (Set[Informee], NonNegativeInt)]

  def allInformees: Set[LfPartyId] =
    informeesAndThresholdByViewPosition
      .flatMap { case (_, (informees, _)) =>
        informees
      }
      .map(_.party)
      .toSet

  def createConfirmationResult(
      requestId: RequestId,
      verdict: Verdict,
      recipientParties: Set[LfPartyId],
  ): ConfirmationResult with SignedProtocolMessageContent

  def minimumThreshold(informees: Set[Informee]): NonNegativeInt

  /** Returns the hash that all [[com.digitalasset.canton.protocol.messages.RootHashMessage]]s of the request batch should contain.
    */
  def rootHash: RootHash

  def submittingParticipant: ParticipantId

  def submittingParticipantSignature: Signature

  def viewType: ViewType
}

object MediatorConfirmationRequest {
  implicit val mediatorRequestProtocolMessageContentCast
      : ProtocolMessageContentCast[MediatorConfirmationRequest] =
    ProtocolMessageContentCast.create[MediatorConfirmationRequest]("MediatorConfirmationRequest") {
      case m: MediatorConfirmationRequest => Some(m)
      case _ => None
    }
}
