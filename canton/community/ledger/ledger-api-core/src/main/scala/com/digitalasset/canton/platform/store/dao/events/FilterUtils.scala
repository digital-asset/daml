// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.canton.platform.{Identifier, Party, TemplatePartiesFilter}

final case class DecomposedFilter(party: Party, templateId: Option[Identifier])

object FilterUtils {
  def decomposeFilters(filter: TemplatePartiesFilter): Seq[DecomposedFilter] = {
    val wildcardFilters = filter.wildcardParties.map { party =>
      DecomposedFilter(party, None)
    }
    val filters = filter.relation.iterator.flatMap { case (templateId, parties) =>
      parties.iterator.map(party => DecomposedFilter(party, Some(templateId)))
    }.toVector ++ wildcardFilters
    filters
  }
}
