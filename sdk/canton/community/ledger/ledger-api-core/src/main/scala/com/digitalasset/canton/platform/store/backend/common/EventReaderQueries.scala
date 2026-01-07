// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  RawArchivedEvent,
  RawThinCreatedEvent,
}
import com.digitalasset.canton.platform.store.backend.PersistentEventType
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.{
  CompositeSql,
  SqlStringInterpolation,
}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.daml.lf.data.Ref.Party

import java.sql.Connection

class EventReaderQueries(stringInterning: StringInterning) {
  import EventStorageBackendTemplate.*

  type EventSequentialId = Long

  def fetchContractIdEvents(
      internalContractId: Long,
      requestingParties: Option[Set[Party]],
      endEventSequentialId: EventSequentialId,
  )(
      connection: Connection
  ): (Option[RawThinCreatedEvent], Option[RawArchivedEvent]) = {
    def queryByInternalContractId(
        tableName: String,
        eventType: PersistentEventType,
        ascending: Boolean,
    )(columns: CompositeSql) =
      SQL"""
         SELECT $columns
         FROM #$tableName
         WHERE
           internal_contract_id = $internalContractId
           AND event_sequential_id <= $endEventSequentialId
           AND event_type = ${eventType.asInt}
         ORDER BY event_sequential_id #${if (ascending) "ASC" else "DESC"}
         LIMIT 1
         """

    def lookupActivateCreated: Option[RawThinCreatedEvent] =
      RowDefs
        .rawThinCreatedEventParser(
          stringInterning = stringInterning,
          allQueryingPartiesO = requestingParties,
          witnessIsAcsDelta = true,
          eventIsAcsDeltaForParticipant = true,
        )
        .querySingleOptRow(
          queryByInternalContractId(
            tableName = "lapi_events_activate_contract",
            eventType = PersistentEventType.Create,
            ascending = true,
          )
        )(connection)

    def lookupDeactivateArchived: Option[RawArchivedEvent] =
      RowDefs
        .rawArchivedEventParser(
          stringInterning = stringInterning,
          allQueryingPartiesO = requestingParties,
          acsDeltaForParticipant = true,
        )
        .querySingleOptRow(
          queryByInternalContractId(
            tableName = "lapi_events_deactivate_contract",
            eventType = PersistentEventType.ConsumingExercise,
            ascending = false,
          )
        )(connection)

    def lookupWitnessedCreated: Option[RawThinCreatedEvent] =
      RowDefs
        .rawThinCreatedEventParser(
          stringInterning = stringInterning,
          allQueryingPartiesO = requestingParties,
          witnessIsAcsDelta = true,
          eventIsAcsDeltaForParticipant = false,
        )
        .querySingleOptRow(
          queryByInternalContractId(
            tableName = "lapi_events_various_witnessed",
            eventType = PersistentEventType.WitnessedCreate,
            ascending = true,
          )
        )(connection)

    def lookupTransientArchived(createOffset: Long): Option[RawArchivedEvent] =
      RowDefs
        .rawArchivedEventParser(
          stringInterning = stringInterning,
          allQueryingPartiesO = requestingParties,
          acsDeltaForParticipant = false,
        )
        .querySingleOptRow(columns => SQL"""
               SELECT $columns
               FROM lapi_events_various_witnessed
               WHERE
                 internal_contract_id = $internalContractId
                 AND event_sequential_id <= $endEventSequentialId
                 AND event_type = ${PersistentEventType.WitnessedConsumingExercise.asInt}
                 AND event_offset = $createOffset
               ORDER BY event_sequential_id
               LIMIT 1
               """)(connection)

    lookupActivateCreated
      .map(create => Some(create) -> lookupDeactivateArchived)
      .orElse(
        lookupWitnessedCreated.flatMap(create =>
          lookupTransientArchived(
            create.transactionProperties.commonEventProperties.offset
          ).map(transientArchive => Some(create) -> Some(transientArchive))
        )
      )
      .getOrElse(None -> None)
  }
}
