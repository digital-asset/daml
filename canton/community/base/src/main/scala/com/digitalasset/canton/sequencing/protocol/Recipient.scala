// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member

final case class Recipient(member: Member) extends PrettyPrinting {
  override def pretty: Pretty[Recipient] =
    prettyOfClass(
      unnamedParam(_.member)
    )

  def toProtoPrimitive: String = toLengthLimitedString.unwrap

  def toLengthLimitedString: String300 = member.toLengthLimitedString
}

object Recipient {
  def fromProtoPrimitive(
      recipient: String,
      fieldName: String,
  ): ParsingResult[Recipient] =
    Member.fromProtoPrimitive(recipient, fieldName).map(Recipient(_))
}
