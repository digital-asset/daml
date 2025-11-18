// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.implicits.toTraverseOps
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

sealed trait PartyOnboardingFlagStatus extends PrettyPrinting {
  protected val status: (Boolean, Option[CantonTimestamp])
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

  def fromProtoV30(
      proto: v30.ClearPartyOnboardingFlagResponse
  ): ParsingResult[PartyOnboardingFlagStatus] =
    proto.earliestRetryTimestamp
      .traverse(CantonTimestamp.fromProtoTimestamp)
      .flatMap { tsOption =>
        (proto.onboarded, tsOption) match {
          case FlagNotSet.status => Right(FlagNotSet)
          case (false, Some(ts)) => Right(FlagSet(ts))
          case (onboarded, ts) =>
            Left(
              OtherError(
                s"Invalid PartyOnboardingFlagStatus: onboarded=$onboarded, earliest_retry_timestamp=$ts"
              )
            )
        }
      }

}
