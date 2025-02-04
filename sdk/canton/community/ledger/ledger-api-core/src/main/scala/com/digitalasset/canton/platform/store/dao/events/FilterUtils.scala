// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.canton.ledger.api.{ParticipantAuthorizationFormat, TopologyFormat}
import com.digitalasset.canton.platform.{Identifier, Party, TemplatePartiesFilter}

final case class DecomposedFilter(party: Option[Party], templateId: Option[Identifier])

object FilterUtils {
  def decomposeFilters(filter: TemplatePartiesFilter): Seq[DecomposedFilter] = {
    val wildcardFilters =
      filter.templateWildcardParties match {
        case Some(parties) =>
          parties.map(party => DecomposedFilter(Some(party), None))
        case None => Seq(DecomposedFilter(None, None))
      }
    val filters = filter.relation.iterator.flatMap {
      case (templateId, Some(parties)) =>
        parties.iterator.map(party => DecomposedFilter(Some(party), Some(templateId)))
      case (templateId, None) =>
        Iterator(DecomposedFilter(None, Some(templateId)))
    }.toVector ++ wildcardFilters
    filters
  }

  // TODO(i23504) cleanup
  def toTopologyFormat(
      filter: TemplatePartiesFilter
  ): TopologyFormat = {
    val filterParties = decomposeFilters(filter).map(_.party)
    val participantAuthorizationFormat = ParticipantAuthorizationFormat(parties =
      if (filterParties.contains(None)) None
      else Some(filterParties.collect { case Some(party) => party }.toSet)
    )
    TopologyFormat(Some(participantAuthorizationFormat))
  }
}
