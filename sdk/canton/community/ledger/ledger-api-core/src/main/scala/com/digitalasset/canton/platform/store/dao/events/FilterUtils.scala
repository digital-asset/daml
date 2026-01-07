// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.canton.platform.{Party, TemplatePartiesFilter}
import com.digitalasset.daml.lf.data.Ref.NameTypeConRef

final case class DecomposedFilter(party: Option[Party], templateId: Option[NameTypeConRef])

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
}
