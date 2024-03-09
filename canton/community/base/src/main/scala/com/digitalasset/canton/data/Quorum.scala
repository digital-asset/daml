// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.transaction.TrustLevel
import com.digitalasset.canton.{LfPartyId, ProtoDeserializationError}

final case class Quorum(
    confirmers: Map[LfPartyId, WeightAndTrustLevel],
    threshold: NonNegativeInt,
) extends PrettyPrinting {

  override def pretty: Pretty[Quorum] = prettyOfClass(
    param("confirmers", _.confirmers),
    param("threshold", _.threshold),
  )

  private[data] def tryToProtoV0(informees: Seq[LfPartyId]): v0.Quorum =
    v0.Quorum(
      partyIndexAndWeight = confirmers.map {
        case (confirmingParty, WeightAndTrustLevel(weight, requiredTrustLevel)) =>
          v0.PartyIndexAndWeight(
            index = {
              val index = informees.indexOf(confirmingParty)
              if (index < 0) {
                /* this is only called by ViewCommonData.toProto, which itself ensures that, when it's created
                 * or deserialized, the informees' list contains the confirming party
                 */
                throw new IndexOutOfBoundsException(
                  s"$confirmingParty is not part of the informees list $informees"
                )
              }
              index
            },
            weight = weight.unwrap,
            requiredTrustLevel = requiredTrustLevel.toProtoEnum,
          )
      }.toSeq,
      threshold = threshold.unwrap,
    )

  def getConfirmingParties: Set[ConfirmingParty] =
    confirmers.map { party =>
      val (pId, w) = party
      ConfirmingParty(pId, PositiveInt.tryCreate(w.weight.unwrap), w.requiredTrustLevel)
    }.toSet

}

object Quorum {

  def create(confirmers: Set[ConfirmingParty], threshold: NonNegativeInt): Quorum =
    Quorum(
      confirmers.map { case ConfirmingParty(id, weight, trustLevel) =>
        id -> WeightAndTrustLevel(weight, trustLevel)
      }.toMap,
      threshold,
    )

  def fromProtoV0(
      quorumP: v0.Quorum,
      informees: Seq[LfPartyId],
  ): ParsingResult[Quorum] = {
    val v0.Quorum(partyIndexAndWeightsP, thresholdP) = quorumP
    for {
      confirmers <- partyIndexAndWeightsP
        .traverse { partyIndexAndWeight =>
          val v0.PartyIndexAndWeight(indexP, weightP, requiredTrustLevelP) = partyIndexAndWeight
          for {
            weight <- PositiveInt
              .create(weightP)
              .leftMap(err => ProtoDeserializationError.InvariantViolation(err.message))
            requiredTrustLevel <- TrustLevel.fromProtoEnum(requiredTrustLevelP)
            confirmingParty <-
              Either.cond(
                0 <= indexP && indexP < informees.size, {
                  val partyId = informees(indexP)
                  partyId -> WeightAndTrustLevel(weight, requiredTrustLevel)
                },
                ProtoDeserializationError.OtherError(
                  s"Invalid index $indexP for informees list size ${informees.size}"
                ),
              )
          } yield confirmingParty
        }
      threshold <- NonNegativeInt
        .create(thresholdP)
        .leftMap(err => ProtoDeserializationError.InvariantViolation(err.message))

    } yield new Quorum(confirmers.toMap, threshold)
  }

}

final case class WeightAndTrustLevel(
    weight: PositiveInt,
    requiredTrustLevel: TrustLevel,
) extends PrettyPrinting {
  override def pretty: Pretty[WeightAndTrustLevel.this.type] = prettyOfClass(
    param("weight", _.weight),
    param("requiredTrustLevel", _.requiredTrustLevel),
  )
}
