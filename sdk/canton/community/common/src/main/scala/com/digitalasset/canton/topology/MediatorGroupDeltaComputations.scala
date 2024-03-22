// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.topology.transaction.MediatorDomainStateX

object MediatorGroupDeltaComputations {

  def verifyProposalConsistency(
      adds: Seq[MediatorId],
      removes: Seq[MediatorId],
      observerAdds: Seq[MediatorId],
      observerRemoves: Seq[MediatorId],
      updateThreshold: Option[PositiveInt],
  ): Either[String, Unit] = for {
    _ <-
      Either.cond(
        !(adds.isEmpty && removes.isEmpty && observerAdds.isEmpty && observerRemoves.isEmpty && updateThreshold.isEmpty),
        (),
        "no mediator group changes proposed",
      )

    error = Seq[(Seq[MediatorId], Seq[MediatorId], String)](
      (adds, removes, "added and removed as active"),
      (adds, observerAdds, "added as active and observer"),
      (observerAdds, observerRemoves, "added and removed as observer"),
      (removes, observerRemoves, "removed as active and observer"),
    ).flatMap { case (first, second, msg) =>
      val intersection = first.intersect(second)
      Option.when(intersection.nonEmpty)(
        s"the same mediators ${intersection.mkString(",")} cannot be $msg in the same proposal"
      )
    }.mkString(", ")
    _ <- Either.cond(error.isEmpty, (), error)
  } yield ()

  def verifyProposalAgainstCurrentState(
      mdsO: Option[MediatorDomainStateX],
      adds: Seq[MediatorId],
      removes: Seq[MediatorId],
      observerAdds: Seq[MediatorId],
      observerRemoves: Seq[MediatorId],
      updateThreshold: Option[PositiveInt],
  ): Either[String, Unit] = {
    val (currentActive, currentObservers, currentThreshold) =
      mdsO.fold[(Seq[MediatorId], Seq[MediatorId], PositiveInt)]((Nil, Nil, PositiveInt.one))(mds =>
        (mds.active, mds.observers, mds.threshold)
      )

    val error = Seq[(Seq[MediatorId], Seq[MediatorId], Boolean, String)](
      (currentActive, adds, false, "to be added already active"),
      (currentObservers, observerAdds, false, "to be added as observer already observer"),
      (currentActive, removes, true, "to be removed not active"),
      (currentObservers, observerRemoves, true, "to be removed as observer not observer"),
    ).flatMap { case (current, proposed, shouldBePresent, msg) =>
      val badMediatorIds =
        (if (shouldBePresent) proposed.diff(current)
         else current.intersect(proposed))

      Option.when(badMediatorIds.nonEmpty)(s"mediators ${badMediatorIds.mkString(",")} $msg")
    }.mkString(", ")

    for {
      _ <- Either.cond(error.isEmpty, (), error)
      activeMediatorsInProposal = currentActive.diff(removes) ++ adds
      _ <- Either.cond(
        activeMediatorsInProposal.nonEmpty,
        (),
        "mediator group without active mediators",
      )
      threshold = updateThreshold.getOrElse(currentThreshold)
      _ <- Either.cond(
        activeMediatorsInProposal.size >= threshold.value,
        (),
        s"mediator group threshold ${threshold} larger than active mediator size ${activeMediatorsInProposal.size}",
      )
    } yield ()
  }
}
