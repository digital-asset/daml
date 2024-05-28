// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

/**  This class represents the filters used in transactions and contracts fetching based on the
  * templates or interfaces they implement and the parties included.
  *
  * @param relation holds the per template filters, if the value of a specific key (identifier) is
  *                 defined then the filter corresponds only to the specific set of parties, if
  *                 None then all the parties known to the participant are included
  * @param templateWildcardParties represents all the templates (template-wildcard) for the set of
  *                                parties specified if defined or all the parties known to the
  *                                participant if None.
  */
final case class TemplatePartiesFilter(
    relation: Map[Identifier, Option[Set[Party]]],
    templateWildcardParties: Option[Set[Party]],
) {
  val allFilterParties: Option[Set[Party]] = {
    val partiesO = Seq(templateWildcardParties) ++ relation.values
    if (partiesO.exists(_.isEmpty)) {
      None
    } else {
      Some(partiesO.flatten.flatten.toSet)
    }
  }
}
