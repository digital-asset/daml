// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.client.binding.Primitive

case class AllocatedParties(
    signatoryO: Option[Primitive.Party],
    observers: List[Primitive.Party],
    divulgees: List[Primitive.Party],
    extraSubmitters: List[Primitive.Party],
) {
  val allAllocatedParties: List[Primitive.Party] =
    signatoryO.toList ++ observers ++ divulgees ++ extraSubmitters

  /** NOTE: This is guaranteed to be safe only for runs with synthetic data generated by Benchtool
    */
  def signatory: Primitive.Party = signatoryO.getOrElse(sys.error("Signatory party not found!"))
}

object AllocatedParties {
  def forExistingParties(parties: List[String]): AllocatedParties = {
    val partiesPrefixMap: Map[String, List[Primitive.Party]] = parties
      .groupBy(Names.getPartyNamePrefix)
      .view
      .mapValues(_.map(Primitive.Party(_)))
      .toMap
    AllocatedParties(
      // NOTE: For synthetic streams signatory is always present
      signatoryO = partiesPrefixMap.getOrElse(Names.SignatoryPrefix, List.empty).headOption,
      observers = partiesPrefixMap.getOrElse(Names.ObserverPrefix, List.empty),
      divulgees = partiesPrefixMap.getOrElse(Names.DivulgeePrefix, List.empty),
      extraSubmitters = partiesPrefixMap.getOrElse(Names.ExtraSubmitterPrefix, List.empty),
    )
  }
}
