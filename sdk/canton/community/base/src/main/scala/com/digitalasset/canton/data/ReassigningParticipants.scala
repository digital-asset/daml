// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.topology.ParticipantId

/** Contains the list of reassigning participants: confirming as well of observing.
  * @param confirming Reassigning participants who need to confirm (hosting at least one signatory)
  * @param observing All the reassigning participants, including the ones who don't need to confirm.
  *                  An observing reassigning participant can submit the unassignment|assignment request.
  *
  * Invariant: confirming is a subset of observing
  */
final case class ReassigningParticipants private (
    confirming: Set[ParticipantId],
    observing: Set[ParticipantId],
) extends PrettyPrinting {
  {
    val confirmingNonObserving = confirming -- observing
    if (confirmingNonObserving.nonEmpty)
      throw new IllegalArgumentException(
        show"Some confirming reassigning participants are not observing: $confirmingNonObserving"
      )
  }

  override protected def pretty: Pretty[this.type] = prettyOfClass(
    param("confirming", _.confirming),
    param("observing", _.observing),
  )
}

object ReassigningParticipants {
  def tryCreate(
      confirming: Set[ParticipantId],
      observing: Set[ParticipantId],
  ): ReassigningParticipants =
    ReassigningParticipants(confirming = confirming, observing = observing)

  def create(
      confirming: Set[ParticipantId],
      observing: Set[ParticipantId],
  ): Either[String, ReassigningParticipants] = Either
    .catchOnly[IllegalArgumentException](tryCreate(confirming, observing))
    .leftMap(_.getMessage)

  def withConfirmers(confirmers: Set[ParticipantId]): ReassigningParticipants =
    ReassigningParticipants(confirming = confirmers, observing = confirmers)

  def empty: ReassigningParticipants = ReassigningParticipants(Set(), Set())
}
