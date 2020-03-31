// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao.events

import anorm.{Row, RowParser, SimpleSql, SqlStringInterpolation, ~}
import com.daml.ledger.participant.state.v1.Offset
import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.ledger.TransactionId
import com.digitalasset.ledger.api.v1.event.Event
import com.digitalasset.platform.store.Conversions._

private[events] trait EventsTableFlatEvents { this: EventsTable =>

  private def createdFlatEventParser(verbose: Boolean): RowParser[Entry[Event]] =
    createdEventRow map {
      case eventOffset ~ transactionId ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templatePackageId ~ templateName ~ commandId ~ workflowId ~ eventWitnesses ~ createArgument ~ createSignatories ~ createObservers ~ createAgreementText ~ createKeyValue =>
        Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
          ledgerEffectiveTime = ledgerEffectiveTime,
          commandId = commandId.getOrElse(""),
          workflowId = workflowId.getOrElse(""),
          event = Event(
            Event.Event.Created(
              createdEvent(
                eventId = eventId,
                contractId = contractId,
                templatePackageId = templatePackageId,
                templateName = templateName,
                createArgument = createArgument,
                createSignatories = createSignatories,
                createObservers = createObservers,
                createAgreementText = createAgreementText,
                createKeyValue = createKeyValue,
                eventWitnesses = eventWitnesses,
                verbose = verbose,
              )
            )
          )
        )
    }

  private val archivedFlatEventParser: RowParser[Entry[Event]] =
    archivedEventRow map {
      case eventOffset ~ transactionId ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templatePackageId ~ templateName ~ commandId ~ workflowId ~ eventWitnesses =>
        Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
          ledgerEffectiveTime = ledgerEffectiveTime,
          commandId = commandId.getOrElse(""),
          workflowId = workflowId.getOrElse(""),
          event = Event(
            Event.Event.Archived(
              archivedEvent(
                eventId = eventId,
                contractId = contractId,
                templatePackageId = templatePackageId,
                templateName = templateName,
                eventWitnesses = eventWitnesses,
              )
            )
          )
        )
    }

  private val verboseFlatEventParser: RowParser[Entry[Event]] =
    createdFlatEventParser(verbose = true) | archivedFlatEventParser

  private val succinctFlatEventParser: RowParser[Entry[Event]] =
    createdFlatEventParser(verbose = false) | archivedFlatEventParser

  def flatEventParser(verbose: Boolean): RowParser[Entry[Event]] =
    if (verbose) verboseFlatEventParser else succinctFlatEventParser

  private val selectColumns =
    Seq(
      "event_offset",
      "transaction_id",
      "ledger_effective_time",
      "workflow_id",
      "participant_events.event_id",
      "contract_id",
      "template_package_id",
      "template_name",
      "create_argument",
      "create_signatories",
      "create_observers",
      "create_agreement_text",
      "create_key_value",
      "array_agg(event_witness) as event_witnesses",
    ).mkString(", ")

  private val flatEventsTable =
    "participant_events natural join participant_event_flat_transaction_witnesses"

  private val orderByColumns =
    Seq("event_offset", "transaction_id", "node_index").mkString(", ")

  private val groupByColumns =
    Seq(
      "event_offset",
      "transaction_id",
      "ledger_effective_time",
      "command_id",
      "workflow_id",
      "participant_events.event_id",
      "contract_id",
      "template_package_id",
      "template_name",
      "create_argument",
      "create_signatories",
      "create_observers",
      "create_agreement_text",
      "create_key_value",
    ).mkString(", ")

  def prepareLookupFlatTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): SimpleSql[Row] =
    SQL"select #$selectColumns, case when submitter in ($requestingParties) then command_id else '' end as command_id from #$flatEventsTable where transaction_id = $transactionId and event_witness in ($requestingParties) group by (#$groupByColumns)"

  private object RangeQueries {

    def onlyWildcardParties(
        startExclusive: Offset,
        endInclusive: Offset,
        parties: Set[Party],
        pageSize: Int,
        rowOffset: Long,
    ): SimpleSql[Row] =
      SQL"select #$selectColumns, case when submitter in ($parties) then command_id else '' end as command_id from #$flatEventsTable where event_offset > $startExclusive and event_offset <= $endInclusive and event_witness in ($parties) group by (#$groupByColumns) order by (#$orderByColumns) limit $pageSize offset $rowOffset"

    def sameTemplates(
        startExclusive: Offset,
        endInclusive: Offset,
        parties: Set[Party],
        templateIds: Set[Identifier],
        pageSize: Int,
        rowOffset: Long,
    ): SimpleSql[Row] =
      SQL"select #$selectColumns, case when submitter in ($parties) then command_id else '' end as command_id from #$flatEventsTable where event_offset > $startExclusive and event_offset <= $endInclusive and event_witness in ($parties) and concat(template_package_id, ':', template_name) in ($templateIds) group by (#$groupByColumns) order by (#$orderByColumns) limit $pageSize offset $rowOffset"

    def mixedTemplates(
        startExclusive: Offset,
        endInclusive: Offset,
        partiesAndTemplateIds: Set[(Party, Identifier)],
        pageSize: Int,
        rowOffset: Long,
    ): SimpleSql[Row] = {
      val parties = partiesAndTemplateIds.map(_._1)
      val partiesAndTemplateIdsAsString = partiesAndTemplateIds.map { case (p, i) => s"$p&$i" }
      SQL"select #$selectColumns, case when submitter in ($parties) then command_id else '' end as command_id from #$flatEventsTable where event_offset > $startExclusive and event_offset <= $endInclusive and concat(event_witness, '&', template_package_id, ':', template_name) in ($partiesAndTemplateIdsAsString) group by (#$groupByColumns) order by (#$orderByColumns) limit $pageSize offset $rowOffset"
    }

    def mixedTemplatesWithWildcardParties(
        startExclusive: Offset,
        endInclusive: Offset,
        wildcardParties: Set[Party],
        partiesAndTemplateIds: Set[(Party, Identifier)],
        pageSize: Int,
        rowOffset: Long,
    ): SimpleSql[Row] = {
      val parties = wildcardParties ++ partiesAndTemplateIds.map(_._1)
      val partiesAndTemplateIdsAsString = partiesAndTemplateIds.map { case (p, i) => s"$p&$i" }
      SQL"select #$selectColumns, case when submitter in ($parties) then command_id else '' end as command_id from #$flatEventsTable where event_offset > $startExclusive and event_offset <= $endInclusive and (event_witness in ($wildcardParties) or concat(event_witness, '&', template_package_id, ':', template_name) in ($partiesAndTemplateIdsAsString)) group by (#$groupByColumns) order by (#$orderByColumns) limit $pageSize offset $rowOffset"
    }

  }

  def preparePagedGetFlatTransactions(
      startExclusive: Offset,
      endInclusive: Offset,
      filter: FilterRelation,
      pageSize: Int,
      rowOffset: Long,
  ): SimpleSql[Row] = {
    require(filter.nonEmpty, "The request must be issued by at least one party")

    // Route the request to the correct underlying query
    // 1. If no party requests specific template identifiers
    val parties = filter.keySet
    if (filter.forall(_._2.isEmpty))
      RangeQueries.onlyWildcardParties(startExclusive, endInclusive, parties, pageSize, rowOffset)
    else {
      // 2. If all parties request the same template identifier
      val templateIds = filter.valuesIterator.flatten.toSet
      if (filter.valuesIterator.forall(_ == templateIds)) {
        RangeQueries.sameTemplates(
          startExclusive = startExclusive,
          endInclusive = endInclusive,
          parties = parties,
          templateIds = templateIds,
          pageSize = pageSize,
          rowOffset = rowOffset,
        )
      } else {
        // 3. If there are different template identifier but there are no wildcard parties
        val partiesAndTemplateIds = FilterRelation.flatten(filter).toSet
        val wildcardParties = filter.filter(_._2.isEmpty).keySet
        if (wildcardParties.isEmpty) {
          RangeQueries.mixedTemplates(
            startExclusive = startExclusive,
            endInclusive = endInclusive,
            partiesAndTemplateIds = partiesAndTemplateIds,
            pageSize = pageSize,
            rowOffset = rowOffset,
          )
        } else {
          // 4. If there are wildcard parties and different template identifiers
          RangeQueries.mixedTemplatesWithWildcardParties(
            startExclusive = startExclusive,
            endInclusive = endInclusive,
            wildcardParties = wildcardParties,
            partiesAndTemplateIds = partiesAndTemplateIds,
            pageSize = pageSize,
            rowOffset = rowOffset,
          )
        }
      }
    }
  }

}
