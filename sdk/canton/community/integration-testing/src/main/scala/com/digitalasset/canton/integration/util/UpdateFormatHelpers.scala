// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  TemplateFilter,
  TransactionFormat,
  TransactionShape,
  UpdateFormat,
}
import com.digitalasset.canton.admin.api.client.data.TemplateId
import com.digitalasset.canton.topology.PartyId

object UpdateFormatHelpers {

  private def getEventFormat(
      partyIds: Set[PartyId],
      filterTemplates: Seq[TemplateId],
  ): EventFormat = {
    val filters: Filters = Filters(
      filterTemplates.map(templateId =>
        CumulativeFilter(
          IdentifierFilter.TemplateFilter(
            TemplateFilter(Some(templateId.toIdentifier), includeCreatedEventBlob = false)
          )
        )
      )
    )

    if (partyIds.isEmpty)
      EventFormat(
        filtersByParty = Map.empty,
        filtersForAnyParty = Some(filters),
        verbose = false,
      )
    else
      EventFormat(
        filtersByParty = partyIds.map(_.toLf -> filters).toMap,
        filtersForAnyParty = None,
        verbose = false,
      )
  }

  def getUpdateFormat(
      partyIds: Set[PartyId],
      filterTemplates: Seq[TemplateId] = Seq.empty,
      transactionShape: TransactionShape = TRANSACTION_SHAPE_ACS_DELTA,
      includeReassignments: Boolean = false,
  ): UpdateFormat = {
    val eventFormat = getEventFormat(partyIds, filterTemplates)
    UpdateFormat(
      includeTransactions = Some(
        TransactionFormat(
          eventFormat = Some(eventFormat),
          transactionShape = transactionShape,
        )
      ),
      includeReassignments = if (includeReassignments) Some(eventFormat) else None,
      includeTopologyEvents = None,
    )
  }

}
