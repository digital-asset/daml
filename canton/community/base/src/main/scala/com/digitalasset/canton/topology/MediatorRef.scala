// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.protocol.Recipient
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

final case class MediatorRef(mediatorId: MediatorId) extends PrettyPrinting {
  def toProtoPrimitive: String = mediatorId.toProtoPrimitive
  def toRecipient: Recipient = Recipient(mediatorId)

  def pretty: Pretty[MediatorRef] = prettyOfParam(_.mediatorId)
}

object MediatorRef {
  def apply(mediatorGroup: MediatorGroup): MediatorRef = {
    val MediatorGroup(_, active, threshold) = mediatorGroup
    if (threshold.value == 1) {
      MediatorRef(active)
    } else {
      throw new IllegalStateException("Unable to handle mediator group")
    }
  }

  def fromProtoPrimitive(
      recipient: String,
      fieldName: String,
  ): ParsingResult[MediatorRef] = {
    Recipient.fromProtoPrimitive(recipient, fieldName).flatMap {
      case Recipient(mediatorId: MediatorId) => Right(MediatorRef(mediatorId))
      case _ =>
        Left(
          InvariantViolation(
            s"MediatorRecipient only allows MED or MOD recipients for the field $fieldName, found: $recipient"
          )
        )
    }
  }
}
