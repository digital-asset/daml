// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.protocol.{MediatorsOfDomain, MemberRecipient, Recipient}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

/** This class represents a union of [[MediatorId]] and [[com.digitalasset.canton.sequencing.protocol.MediatorsOfDomain]].
  * It is used throughout the protocol to represent target/source mediator or mediator group in case of a BFT domain
  */
sealed trait MediatorRef extends PrettyPrinting with Product with Serializable {
  def toProtoPrimitive: String

  def toRecipient: Recipient

  def isGroup: Boolean = false

  def isSingle: Boolean = false
}

object MediatorRef {
  final case class Single(mediatorId: MediatorId) extends MediatorRef {
    override def toProtoPrimitive: String = mediatorId.toProtoPrimitive

    override def toRecipient: Recipient = MemberRecipient(mediatorId)

    override def isSingle: Boolean = true

    override def pretty: Pretty[Single] = prettyOfParam(_.mediatorId)
  }

  final case class Group(mediatorsOfDomain: MediatorsOfDomain) extends MediatorRef {
    override def toProtoPrimitive: String = mediatorsOfDomain.toProtoPrimitive

    override def toRecipient: Recipient = mediatorsOfDomain

    override def isGroup: Boolean = true

    override def pretty: Pretty[Group] = prettyOfParam(_.mediatorsOfDomain)
  }

  def apply(mediatorId: MediatorId): MediatorRef = Single(mediatorId)

  def apply(mediatorsOfDomain: MediatorsOfDomain): MediatorRef = Group(mediatorsOfDomain)

  def apply(mediatorGroup: MediatorGroup): MediatorRef = {
    val MediatorGroup(_, active, _, threshold) = mediatorGroup
    if (active.sizeIs == 1 && threshold.value == 1) {
      MediatorRef(active(0))
    } else {
      MediatorRef(MediatorsOfDomain(mediatorGroup.index))
    }
  }

  def fromProtoPrimitive(
      recipient: String,
      fieldName: String,
  ): ParsingResult[MediatorRef] = {
    Recipient.fromProtoPrimitive(recipient, fieldName).flatMap {
      case MemberRecipient(mediatorId: MediatorId) => Right(MediatorRef(mediatorId))
      case mod @ MediatorsOfDomain(_) => Right(MediatorRef(mod))
      case _ =>
        Left(
          InvariantViolation(
            s"MediatorRecipient only allows MED or MOD recipients for the field $fieldName, found: $recipient"
          )
        )
    }
  }
}
