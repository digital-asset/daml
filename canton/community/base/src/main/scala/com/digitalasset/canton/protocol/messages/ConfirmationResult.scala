// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence

trait ConfirmationResult
    extends ProtocolVersionedMemoizedEvidence
    with HasDomainId
    with HasRequestId
    with SignedProtocolMessageContent {

  def verdict: Verdict

  override def signingTimestamp: Option[CantonTimestamp] = Some(requestId.unwrap)

  def viewType: ViewType
}

/** The mediator issues a regular confirmation result for well-formed mediator confirmation requests.
  * Malformed confirmation requests lead to a [[MalformedConfirmationRequestResult]].
  */
trait RegularConfirmationResult extends ConfirmationResult

object RegularConfirmationResult {
  implicit val regularMediatorResultMessageCast
      : SignedMessageContentCast[RegularConfirmationResult] =
    SignedMessageContentCast.create[RegularConfirmationResult]("RegularConfirmationResult") {
      case m: RegularConfirmationResult => Some(m)
      case _ => None
    }
}
