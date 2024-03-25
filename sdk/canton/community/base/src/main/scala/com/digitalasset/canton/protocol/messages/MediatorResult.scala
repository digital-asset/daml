// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence

trait MediatorResult
    extends ProtocolVersionedMemoizedEvidence
    with HasDomainId
    with HasRequestId
    with SignedProtocolMessageContent {

  def verdict: Verdict

  override def signingTimestamp: CantonTimestamp = requestId.unwrap

  def viewType: ViewType
}

/** The mediator issues a regular mediator result for well-formed mediator requests.
  * Malformed mediator requests lead to a [[MalformedMediatorRequestResult]].
  */
trait RegularMediatorResult extends MediatorResult

object RegularMediatorResult {
  implicit val regularMediatorResultMessageCast: SignedMessageContentCast[RegularMediatorResult] =
    SignedMessageContentCast.create[RegularMediatorResult]("RegularMediatorResult") {
      case m: RegularMediatorResult => Some(m)
      case _ => None
    }
}
