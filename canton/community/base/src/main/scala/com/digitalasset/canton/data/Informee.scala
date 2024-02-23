// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.{LfPartyId, ProtoDeserializationError}

/** A party that must be informed about the view.
  */
// This class is a reference example of serialization best practices.
// In particular, it demonstrates serializing a trait with different subclasses.
// The design is quite simple. It should be applied whenever possible, but it will not cover all cases.
//
// Please consult the team if you intend to change the design of serialization.
sealed trait Informee extends Product with Serializable with PrettyPrinting {
  def party: LfPartyId

  /** Determines how much "impact" the informee has on approving / rejecting the underlying view.
    *
    * Positive value: confirming party
    * Zero: plain informee, who sees the underlying view, but has no impact on approving / rejecting it
    */
  def weight: NonNegativeInt

  /** Yields an informee resulting from adding `delta` to `weight`.
    *
    * If the new weight is zero, the resulting informee will be a plain informee;
    * in thise case, the resulting informee will have trust level ORDINARY irrespective of the trust level of this.
    */
  def withAdditionalWeight(delta: NonNegativeInt): Informee

  /** Plain informees get weight 0.
    * Confirming parties get their assigned (positive) weight.
    */
  private[data] def toProtoV30: v30.Informee =
    v30.Informee(
      party = party,
      weight = weight.unwrap,
    )

  override def pretty: Pretty[Informee] =
    prettyOfString(inst => show"${inst.party}*${inst.weight}")
}

object Informee {

  def create(
      party: LfPartyId,
      weight: NonNegativeInt,
  ): Informee =
    if (weight == NonNegativeInt.zero) PlainInformee(party)
    else ConfirmingParty(party, PositiveInt.tryCreate(weight.unwrap))

  private[data] def fromProtoV30(informeeP: v30.Informee): ParsingResult[Informee] = {
    val v30.Informee(partyP, weightP) = informeeP
    for {
      party <- LfPartyId
        .fromString(partyP)
        .leftMap(ProtoDeserializationError.ValueDeserializationError("party", _))

      weight <- NonNegativeInt
        .create(weightP)
        .leftMap(err => ProtoDeserializationError.InvariantViolation(err.message))
    } yield Informee.create(party, weight)
  }
}

/** A party that must confirm the underlying view.
  *
  * @param weight determines the impact of the party on whether the view is approved.
  */
final case class ConfirmingParty(
    party: LfPartyId,
    partyWeight: PositiveInt,
) extends Informee {

  val weight: NonNegativeInt = partyWeight.toNonNegative

  def withAdditionalWeight(delta: NonNegativeInt): Informee = {
    copy(partyWeight = partyWeight + delta)
  }
}

/** An informee that is not a confirming party
  */
final case class PlainInformee(party: LfPartyId) extends Informee {
  override val weight: NonNegativeInt = NonNegativeInt.zero

  def withAdditionalWeight(delta: NonNegativeInt): Informee =
    if (delta == NonNegativeInt.zero) this
    else ConfirmingParty(party, PositiveInt.tryCreate(delta.unwrap))
}
