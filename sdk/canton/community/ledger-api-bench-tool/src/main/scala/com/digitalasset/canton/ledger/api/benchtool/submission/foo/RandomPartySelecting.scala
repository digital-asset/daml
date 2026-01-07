// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.submission.foo

import com.daml.ledger.javaapi.data.Party
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig.FooSubmissionConfig
import com.digitalasset.canton.ledger.api.benchtool.submission.{
  AllocatedParties,
  RandomnessProvider,
}

class RandomPartySelecting(
    config: FooSubmissionConfig,
    allocatedParties: AllocatedParties,
    randomnessProvider: RandomnessProvider,
) {

  private val observersProbability = probabilitiesByPartyIndex(allocatedParties.observers)
  private val divulgeesProbability = probabilitiesByPartyIndex(allocatedParties.divulgees)
  private val extraSubmittersProbability = probabilitiesByPartyIndex(
    allocatedParties.extraSubmitters
  )
  private val observerPartySetPartiesProbability: List[(Party, Double)] =
    allocatedParties.observerPartySets.flatMap { partySet =>
      val visibility = config.observerPartySets
        .find(_.partyNamePrefix == partySet.mainPartyNamePrefix)
        .fold(
          sys.error(
            s"Could not find visibility for party set ${partySet.mainPartyNamePrefix} in the submission config"
          )
        )(_.visibility)
      partySet.parties.map(party => party -> visibility)
    }

  def nextPartiesForContracts(): PartiesSelection =
    PartiesSelection(
      observers =
        pickParties(observersProbability) ++ pickParties(observerPartySetPartiesProbability),
      divulgees = pickParties(divulgeesProbability),
    )

  def nextExtraSubmitter(): List[Party] = pickParties(extraSubmittersProbability)

  private def pickParties(probabilities: List[(Party, Double)]): List[Party] =
    probabilities
      .collect { case (party, probability) if randomBoolean(probability) => party }

  private def randomBoolean(truthProbability: Double): Boolean =
    randomnessProvider.randomDouble() <= truthProbability

  private def probabilitiesByPartyIndex(
      orderedParties: List[Party]
  ): List[(Party, Double)] =
    orderedParties.zipWithIndex.toMap.view.mapValues(probabilityBaseTen).toList

  /** @return
    *   probability of a 1/(10**i)
    */
  private def probabilityBaseTen(i: Int): Double = math.pow(10.0, -i.toDouble)

}
