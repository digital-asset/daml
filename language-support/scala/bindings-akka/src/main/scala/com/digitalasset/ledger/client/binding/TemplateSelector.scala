// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import com.daml.ledger.api.refinements.ApiTypes.TemplateId
import com.daml.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters}
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
