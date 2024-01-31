// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.event_query_service.GetEventsByContractKeyResponse
import com.daml.ledger.api.v2.event_query_service.{Archived, Created, GetEventsByContractIdResponse}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform
import com.digitalasset.canton.platform.store.backend.{EventStorageBackend, ParameterStorageBackend}
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.cache.MutableCacheBackedContractStore.EventSequentialId
import com.digitalasset.canton.platform.store.dao.events.Raw.FlatEvent
import com.digitalasset.canton.platform.store.dao.{
  DbDispatcher,
  EventProjectionProperties,
  LedgerDaoEventsReader,
}

import scala.concurrent.{ExecutionContext, Future}

private[dao] sealed class EventsReader(
    val dbDispatcher: DbDispatcher,
    val eventStorageBackend: EventStorageBackend,
    val parameterStorageBackend: ParameterStorageBackend,
    val metrics: Metrics,
    val lfValueTranslation: LfValueTranslation,
    val ledgerEndCache: LedgerEndCache,
)(implicit ec: ExecutionContext)
    extends LedgerDaoEventsReader {

  protected val dbMetrics: metrics.index.db.type = metrics.index.db

  override def getEventsByContractId(contractId: ContractId, requestingParties: Set[Party])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[GetEventsByContractIdResponse] = {

    val eventProjectionProperties = EventProjectionProperties(
      // Used by LfEngineToApi
      verbose = true,
      // Needed to get create arguments mapped
      wildcardWitnesses = requestingParties.map(_.toString),
    )

    for {
      rawEvents <- dbDispatcher.executeSql(dbMetrics.getEventsByContractId)(
        eventStorageBackend.eventReaderQueries.fetchContractIdEvents(
          contractId,
          requestingParties = requestingParties,
          endEventSequentialId = ledgerEndCache()._2,
        )
      )

      deserialized <- Future.traverse(rawEvents) { event =>
        event.event
          .applyDeserialization(lfValueTranslation, eventProjectionProperties)
          .map(_ -> event.domainId)
      }

      createEvent = deserialized.flatMap { case (event, domainId) =>
        event.event.created.map(create => Created(Some(create), domainId.getOrElse("")))
      }.headOption
      archiveEvent = deserialized.flatMap { case (event, domainId) =>
        event.event.archived.map(archive => Archived(Some(archive), domainId.getOrElse("")))
      }.headOption

    } yield {
      if (
        createEvent
          .flatMap(_.createdEvent)
          .exists(stakeholders(_).exists(requestingParties.map(identity[String])))
      ) {
        GetEventsByContractIdResponse(createEvent, archiveEvent)
      } else {
        GetEventsByContractIdResponse(None, None)
      }
    }
  }

  private def stakeholders(e: CreatedEvent): Set[String] = e.signatories.toSet ++ e.observers

  override def getEventsByContractKey(
      contractKey: Value,
      templateId: Ref.Identifier,
      requestingParties: Set[Party],
      endExclusiveSeqId: Option[EventSequentialId],
      maxIterations: Int,
  )(implicit loggingContext: LoggingContextWithTrace): Future[GetEventsByContractKeyResponse] = {
    val keyHash: String =
      platform.Key.assertBuild(templateId, contractKey, shared = true).hash.bytes.toHexString

    val eventProjectionProperties = EventProjectionProperties(
      // Used by LfEngineToApi
      verbose = true,
      // Needed to get create arguments mapped
      wildcardWitnesses = requestingParties.map(_.toString),
    )

    for {

      (
        rawCreate: Option[FlatEvent.Created],
        rawArchive: Option[FlatEvent.Archived],
        eventSequentialId,
      ) <- dbDispatcher
        .executeSql(dbMetrics.getEventsByContractKey) { conn =>
          eventStorageBackend.eventReaderQueries.fetchNextKeyEvents(
            keyHash,
            requestingParties,
            endExclusiveSeqId.getOrElse(ledgerEndCache()._2 + 1),
            maxIterations,
          )(conn)
        }

      createEvent <- rawCreate.fold(Future[Option[CreatedEvent]](None)) { e =>
        e.deserializeCreateEvent(lfValueTranslation, eventProjectionProperties).map(Some(_))
      }
      archiveEvent = rawArchive.map(_.deserializedArchivedEvent())

      continuationToken = eventSequentialId
        .map(_.toString)
        .getOrElse(GetEventsByContractKeyResponse.defaultInstance.continuationToken)

    } yield {
      GetEventsByContractKeyResponse(createEvent, archiveEvent, continuationToken)
    }
  }

}
