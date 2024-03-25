// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.{
  ConfirmingParty,
  ViewConfirmationParameters,
  ViewPosition,
  ViewType,
}
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.{RequestId, RootHash, ViewHash}
import com.digitalasset.canton.topology.MediatorRef

import java.util.UUID

trait MediatorRequest extends ProtocolMessage with UnsignedProtocolMessage {
  def requestUuid: UUID

  def mediator: MediatorRef

  def informeesAndConfirmationParamsByViewHash: Map[ViewHash, ViewConfirmationParameters]

  def informeesAndConfirmationParamsByViewPosition: Map[ViewPosition, ViewConfirmationParameters]

  def allInformees: Set[LfPartyId] =
    informeesAndConfirmationParamsByViewPosition.flatMap {
      case (_, ViewConfirmationParameters(informees, _)) =>
        informees
    }.toSet

  def createMediatorResult(
      requestId: RequestId,
      verdict: Verdict,
      recipientParties: Set[LfPartyId],
  ): MediatorResult with SignedProtocolMessageContent

  def minimumThreshold(confirmingParties: Set[ConfirmingParty]): NonNegativeInt

  /** Returns the hash that all [[com.digitalasset.canton.protocol.messages.RootHashMessage]]s of the request batch should contain.
    * [[scala.None$]] indicates that no [[com.digitalasset.canton.protocol.messages.RootHashMessage]] should be in the batch.
    */
  def rootHash: Option[RootHash]

  def viewType: ViewType
}

object MediatorRequest {
  implicit val mediatorRequestProtocolMessageContentCast
      : ProtocolMessageContentCast[MediatorRequest] =
    ProtocolMessageContentCast.create[MediatorRequest]("MediatorRequest") {
      case m: MediatorRequest => Some(m)
      case _ => None
    }
}
