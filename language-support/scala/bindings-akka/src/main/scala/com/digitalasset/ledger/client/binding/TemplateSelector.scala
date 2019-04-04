// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding

import com.digitalasset.ledger.api.refinements.ApiTypes.TemplateId
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters}
import scalaz.Tag

sealed trait TemplateSelector extends Product with Serializable {
  def toApi: Filters
}

object TemplateSelector {
  case object All extends TemplateSelector {
    val toApi = Filters.defaultInstance
  }

  final case class Templates(templates: Set[TemplateId]) extends TemplateSelector {
    val toApi = Filters(Some(InclusiveFilters(Tag.unsubst(templates).toSeq)))
  }
}
