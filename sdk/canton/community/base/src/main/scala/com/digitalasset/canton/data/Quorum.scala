// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.traverse.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter.{
  ParsingResult,
  parseNonNegativeInt,
  parsePositiveInt,
}
import com.digitalasset.canton.{LfPartyId, ProtoDeserializationError}

/** A set of confirming parties and their weights plus a threshold constitutes a quorum.
  *
  * @param confirmers maps a party id to a weight. The weight is a positive int because
  *                   only PlainInformees have a weight of 0.
  */
final case class Quorum(
    confirmers: Map[LfPartyId, PositiveInt],
    threshold: NonNegativeInt,
) extends PrettyPrinting {

  override def pretty: Pretty[Quorum] = prettyOfClass(
    param("confirmers", _.confirmers),
    param("threshold", _.threshold),
  )

  private[data] def tryToProtoV30(informees: Seq[LfPartyId]): v30.Quorum =
    v30.Quorum(
      partyIndexAndWeight = confirmers.map { case (confirmingParty, weight) =>
        v30.PartyIndexAndWeight(
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
        )
      }.toSeq,
      threshold = threshold.unwrap,
    )
}

object Quorum {

  lazy val empty: Quorum = Quorum(Map.empty, NonNegativeInt.zero)

  def fromProtoV30(
      quorumP: v30.Quorum,
      informees: Seq[LfPartyId],
  ): ParsingResult[Quorum] = {
    val v30.Quorum(partyIndexAndWeightsP, thresholdP) = quorumP
    for {
      confirmers <- partyIndexAndWeightsP
        .traverse { partyIndexAndWeight =>
          val v30.PartyIndexAndWeight(indexP, weightP) = partyIndexAndWeight
          for {
            weight <- parsePositiveInt("weight", weightP)
            confirmingParty <-
              Either.cond(
                0 <= indexP && indexP < informees.size, {
                  val partyId = informees(indexP)
                  partyId -> weight
                },
                ProtoDeserializationError.OtherError(
                  s"Invalid index $indexP for informees list size ${informees.size}"
                ),
              )
          } yield confirmingParty
        }
      threshold <- parseNonNegativeInt("threshold", thresholdP)
    } yield new Quorum(confirmers.toMap, threshold)
  }

}
