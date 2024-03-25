// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.event

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.lf.ledger.EventId
import com.daml.lf.value.Value

sealed trait KeyContinuationToken extends Product with Serializable
object KeyContinuationToken {

  final case object NoToken extends KeyContinuationToken
  final case class EndExclusiveSeqIdToken(seqId: Long) extends KeyContinuationToken
  final case class EndExclusiveEventIdToken(eventId: EventId) extends KeyContinuationToken

  private val sequenceTokenPrefix = "s:"
  private val eventTokenPrefix = "e:"
  private val prefixLength = 2

  def toTokenString(token: KeyContinuationToken): String = token match {
    case NoToken => ""
    case EndExclusiveSeqIdToken(seqId) => s"$sequenceTokenPrefix$seqId"
    case EndExclusiveEventIdToken(eventId) => s"$eventTokenPrefix${eventId.toLedgerString}"
  }

  def fromTokenString(s: String): Either[String, KeyContinuationToken] =
    s.splitAt(prefixLength) match {
      case ("", "") => Right(NoToken)
      case (`sequenceTokenPrefix`, oth) =>
        oth.toLongOption
          .toRight(s"Unable to parse $s into a sequence token")
          .map(EndExclusiveSeqIdToken)
      case (`eventTokenPrefix`, eventId) =>
        EventId
          .fromString(eventId)
          .left
          .map(errStr => s"Failed to parse $eventId into an event-id: $errStr")
          .map(EndExclusiveEventIdToken)
      case _ => Left(s"Unable to parse '$s' into token string")
    }
}

final case class GetEventsByContractKeyRequest(
    contractKey: Value,
    templateId: Ref.Identifier,
    requestingParties: Set[Party],
    keyContinuationToken: KeyContinuationToken,
)
