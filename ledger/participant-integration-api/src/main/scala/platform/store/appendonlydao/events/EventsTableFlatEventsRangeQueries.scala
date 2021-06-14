// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import java.sql.Connection

import com.daml.lf.data.Ref.{Identifier => ApiIdentifier}
import com.daml.platform.store.backend.EventStorageBackend

private[events] sealed abstract class EventsTableFlatEventsRangeQueries[Offset] {

  import EventsTableFlatEventsRangeQueries.QueryParts

  protected def singleWildcardParty(
      offset: Offset,
      party: Party,
  ): QueryParts

  protected def singlePartyWithTemplates(
      offset: Offset,
      party: Party,
      templateIds: Set[ApiIdentifier],
  ): QueryParts

  protected def onlyWildcardParties(
      offset: Offset,
      parties: Set[Party],
  ): QueryParts

  protected def sameTemplates(
      offset: Offset,
      parties: Set[Party],
      templateIds: Set[ApiIdentifier],
  ): QueryParts

  protected def mixedTemplates(
      offset: Offset,
      partiesAndTemplateIds: Set[(Party, ApiIdentifier)],
  ): QueryParts

  protected def mixedTemplatesWithWildcardParties(
      offset: Offset,
      wildcardParties: Set[Party],
      partiesAndTemplateIds: Set[(Party, ApiIdentifier)],
  ): QueryParts

  protected def offsetRange(offset: Offset): EventsRange[Long]

  final def apply(
      offset: Offset,
      filter: FilterRelation,
      pageSize: Int,
  ): SqlSequence[Vector[EventsTable.Entry[Raw.FlatEvent]]] = {
    require(filter.nonEmpty, "The request must be issued by at least one party")

    // Route the request to the correct underlying query
    val frqK = if (filter.size == 1) {
      val (party, templateIds) = filter.iterator.next()
      if (templateIds.isEmpty) {
        // Single-party request, no specific template identifier
        singleWildcardParty(offset, party)
      } else {
        // Single-party request, restricted to a set of template identifiers
        singlePartyWithTemplates(offset, party, templateIds)
      }
    } else {
      // Multi-party requests
      // If no party requests specific template identifiers
      val parties = filter.keySet
      if (filter.forall(_._2.isEmpty))
        onlyWildcardParties(
          offset = offset,
          parties = parties,
        )
      else {
        // If all parties request the same template identifier
        val templateIds = filter.valuesIterator.flatten.toSet
        if (filter.valuesIterator.forall(_ == templateIds)) {
          sameTemplates(
            offset,
            parties = parties,
            templateIds = templateIds,
          )
        } else {
          // If there are different template identifier but there are no wildcard parties
          val partiesAndTemplateIds = Relation.flatten(filter).toSet
          val wildcardParties = filter.filter(_._2.isEmpty).keySet
          if (wildcardParties.isEmpty) {
            mixedTemplates(
              offset,
              partiesAndTemplateIds = partiesAndTemplateIds,
            )
          } else {
            // If there are wildcard parties and different template identifiers
            mixedTemplatesWithWildcardParties(
              offset,
              wildcardParties,
              partiesAndTemplateIds,
            )
          }
        }
      }
    }

    frqK match {
      case QueryParts.ByArith(read) =>
        EventsRange.readPage(
          read,
          offsetRange(offset),
          pageSize,
        )
      case QueryParts.ByLimit(sql) =>
        SqlSequence.plainQuery(sql(pageSize))
    }
  }
}

private[events] object EventsTableFlatEventsRangeQueries {

  import com.daml.ledger.participant.state.v1.Offset

  private[EventsTableFlatEventsRangeQueries] sealed abstract class QueryParts
      extends Product
      with Serializable
  private[EventsTableFlatEventsRangeQueries] object QueryParts {
    final case class ByArith(
        read: (
            EventsRange[Long],
            Option[Int],
            Option[Int],
        ) => Connection => Vector[EventsTable.Entry[Raw.FlatEvent]]
    ) extends QueryParts
    final case class ByLimit(
        saferRead: Int => Connection => Vector[EventsTable.Entry[Raw.FlatEvent]]
    ) extends QueryParts
  }

  final class GetTransactions(
      storageBackend: EventStorageBackend
  ) extends EventsTableFlatEventsRangeQueries[EventsRange[Long]] {

    override protected def singleWildcardParty(
        range: EventsRange[Long],
        party: Party,
    ): QueryParts =
      QueryParts.ByArith((range, limit, fetchSizeHint) =>
        storageBackend.transactionsEventsSingleWildcardParty(
          startExclusive = range.startExclusive,
          endInclusive = range.endInclusive,
          party = party,
          limit = limit,
          fetchSizeHint = fetchSizeHint,
        )
      )

    override protected def singlePartyWithTemplates(
        range: EventsRange[Long],
        party: Party,
        templateIds: Set[ApiIdentifier],
    ): QueryParts =
      QueryParts.ByArith((range, limit, fetchSizeHint) =>
        storageBackend.transactionsEventsSinglePartyWithTemplates(
          startExclusive = range.startExclusive,
          endInclusive = range.endInclusive,
          party = party,
          templateIds = templateIds,
          limit = limit,
          fetchSizeHint = fetchSizeHint,
        )
      )

    override protected def onlyWildcardParties(
        range: EventsRange[Long],
        parties: Set[Party],
    ): QueryParts =
      QueryParts.ByArith((range, limit, fetchSizeHint) =>
        storageBackend.transactionsEventsOnlyWildcardParties(
          startExclusive = range.startExclusive,
          endInclusive = range.endInclusive,
          parties = parties,
          limit = limit,
          fetchSizeHint = fetchSizeHint,
        )
      )

    override protected def sameTemplates(
        range: EventsRange[Long],
        parties: Set[Party],
        templateIds: Set[ApiIdentifier],
    ): QueryParts =
      QueryParts.ByArith((range, limit, fetchSizeHint) =>
        storageBackend.transactionsEventsSameTemplates(
          startExclusive = range.startExclusive,
          endInclusive = range.endInclusive,
          parties = parties,
          templateIds = templateIds,
          limit = limit,
          fetchSizeHint = fetchSizeHint,
        )
      )

    override protected def mixedTemplates(
        range: EventsRange[Long],
        partiesAndTemplateIds: Set[(Party, ApiIdentifier)],
    ): QueryParts =
      QueryParts.ByArith((range, limit, fetchSizeHint) =>
        storageBackend.transactionsEventsMixedTemplates(
          startExclusive = range.startExclusive,
          endInclusive = range.endInclusive,
          partiesAndTemplateIds = partiesAndTemplateIds,
          limit = limit,
          fetchSizeHint = fetchSizeHint,
        )
      )

    override protected def mixedTemplatesWithWildcardParties(
        range: EventsRange[Long],
        wildcardParties: Set[Party],
        partiesAndTemplateIds: Set[(Party, ApiIdentifier)],
    ): QueryParts = {
      QueryParts.ByArith((range, limit, fetchSizeHint) =>
        storageBackend.transactionsEventsMixedTemplatesWithWildcardParties(
          startExclusive = range.startExclusive,
          endInclusive = range.endInclusive,
          partiesAndTemplateIds = partiesAndTemplateIds,
          wildcardParties = wildcardParties,
          limit = limit,
          fetchSizeHint = fetchSizeHint,
        )
      )
    }

    override protected def offsetRange(offset: EventsRange[Long]) = offset
  }

  final class GetActiveContracts(
      storageBackend: EventStorageBackend
  ) extends EventsTableFlatEventsRangeQueries[EventsRange[(Offset, Long)]] {

    override protected def singleWildcardParty(
        range: EventsRange[(Offset, Long)],
        party: Party,
    ): QueryParts =
      QueryParts.ByLimit(limit =>
        storageBackend.activeContractsEventsSingleWildcardParty(
          startExclusive = range.startExclusive._2,
          endInclusiveSeq = range.endInclusive._2,
          endInclusiveOffset = range.endInclusive._1,
          party = party,
          limit = Some(limit),
          fetchSizeHint = Some(limit),
        )
      )

    override protected def singlePartyWithTemplates(
        range: EventsRange[(Offset, Long)],
        party: Party,
        templateIds: Set[ApiIdentifier],
    ): QueryParts =
      QueryParts.ByLimit(limit =>
        storageBackend.activeContractsEventsSinglePartyWithTemplates(
          startExclusive = range.startExclusive._2,
          endInclusiveSeq = range.endInclusive._2,
          endInclusiveOffset = range.endInclusive._1,
          party = party,
          templateIds = templateIds,
          limit = Some(limit),
          fetchSizeHint = Some(limit),
        )
      )

    override def onlyWildcardParties(
        range: EventsRange[(Offset, Long)],
        parties: Set[Party],
    ): QueryParts =
      QueryParts.ByLimit(limit =>
        storageBackend.activeContractsEventsOnlyWildcardParties(
          startExclusive = range.startExclusive._2,
          endInclusiveSeq = range.endInclusive._2,
          endInclusiveOffset = range.endInclusive._1,
          parties = parties,
          limit = Some(limit),
          fetchSizeHint = Some(limit),
        )
      )

    override def sameTemplates(
        range: EventsRange[(Offset, Long)],
        parties: Set[Party],
        templateIds: Set[ApiIdentifier],
    ): QueryParts =
      QueryParts.ByLimit(limit =>
        storageBackend.activeContractsEventsSameTemplates(
          startExclusive = range.startExclusive._2,
          endInclusiveSeq = range.endInclusive._2,
          endInclusiveOffset = range.endInclusive._1,
          parties = parties,
          templateIds = templateIds,
          limit = Some(limit),
          fetchSizeHint = Some(limit),
        )
      )

    override def mixedTemplates(
        range: EventsRange[(Offset, Long)],
        partiesAndTemplateIds: Set[(Party, ApiIdentifier)],
    ): QueryParts =
      QueryParts.ByLimit(limit =>
        storageBackend.activeContractsEventsMixedTemplates(
          startExclusive = range.startExclusive._2,
          endInclusiveSeq = range.endInclusive._2,
          endInclusiveOffset = range.endInclusive._1,
          partiesAndTemplateIds = partiesAndTemplateIds,
          limit = Some(limit),
          fetchSizeHint = Some(limit),
        )
      )

    override def mixedTemplatesWithWildcardParties(
        range: EventsRange[(Offset, Long)],
        wildcardParties: Set[Party],
        partiesAndTemplateIds: Set[(Party, ApiIdentifier)],
    ): QueryParts =
      QueryParts.ByLimit(limit =>
        storageBackend.activeContractsEventsMixedTemplatesWithWildcardParties(
          startExclusive = range.startExclusive._2,
          endInclusiveSeq = range.endInclusive._2,
          endInclusiveOffset = range.endInclusive._1,
          partiesAndTemplateIds = partiesAndTemplateIds,
          wildcardParties = wildcardParties,
          limit = Some(limit),
          fetchSizeHint = Some(limit),
        )
      )

    override protected def offsetRange(offset: EventsRange[(Offset, Long)]) = offset map (_._2)
  }
}
