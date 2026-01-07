// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.google.protobuf.timestamp.Timestamp

sealed trait PartyOnboardingFlagStatus extends PrettyPrinting {
  val status: (Boolean, Option[CantonTimestamp])
}

case object FlagNotSet extends PartyOnboardingFlagStatus {
  override val status: (Boolean, Option[CantonTimestamp]) = (true, None)

  override protected def pretty: Pretty[FlagNotSet.type] = prettyOfObject[FlagNotSet.type]
}

final case class FlagSet(safeToClear: CantonTimestamp) extends PartyOnboardingFlagStatus {
  override val status: (Boolean, Option[CantonTimestamp]) = (false, Some(safeToClear))

  override protected def pretty: Pretty[FlagSet] = prettyOfClass(
    param("earliest safe time to clear the flag", _.safeToClear)
  )
}

object PartyOnboardingFlagStatus {

  def toProtoV30(
      status: PartyOnboardingFlagStatus
  ): (Boolean, Option[Timestamp]) = {
    val (onboarded, timestamp) = status.status
    (onboarded, timestamp.map(_.toProtoTimestamp))
  }

}
