// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission.foo

import com.daml.ledger.api.benchtool.submission.{AllocatedParties, RandomnessProvider}
import com.daml.ledger.client.binding.Primitive

class RandomPartySelecting(
    allocatedParties: AllocatedParties,
    randomnessProvider: RandomnessProvider = RandomnessProvider.Default,
) {

  private val observersProbability = probabilitiesByPartyIndex(allocatedParties.observers)
  private val divulgeesProbability = probabilitiesByPartyIndex(allocatedParties.divulgees)
  private val extraSubmittersProbability = probabilitiesByPartyIndex(
    allocatedParties.extraSubmitters
  )

  def nextPartiesForContracts(): PartiesSelection = {
    PartiesSelection(
      observers = pickParties(observersProbability),
      divulgees = pickParties(divulgeesProbability),
    )
  }

  def nextExtraSubmitter(): List[Primitive.Party] = pickParties(extraSubmittersProbability)

  private def pickParties(probabilities: List[(Primitive.Party, Double)]): List[Primitive.Party] =
    probabilities
      .collect { case (party, probability) if randomBoolean(probability) => party }

  private def randomBoolean(truthProbability: Double): Boolean =
    randomnessProvider.randomDouble() <= truthProbability

  private def probabilitiesByPartyIndex(
      orderedParties: List[Primitive.Party]
  ): List[(Primitive.Party, Double)] =
    orderedParties.zipWithIndex.toMap.view.mapValues(probabilityBaseTen).toList

  /** @return probability of a 1/(10**i)
    */
  private def probabilityBaseTen(i: Int): Double = 1.0 / math.pow(10.0, i.toDouble)

}
