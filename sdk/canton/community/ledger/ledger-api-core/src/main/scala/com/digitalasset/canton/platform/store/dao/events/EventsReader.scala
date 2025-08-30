// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.event_query_service.{Archived, Created, GetEventsByContractIdResponse}
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.InternalEventFormat
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  Entry,
  RawArchivedEvent,
  RawCreatedEvent,
  RawFlatEvent,
}
import com.digitalasset.canton.platform.store.backend.{EventStorageBackend, ParameterStorageBackend}
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.dao.{DbDispatcher, LedgerDaoEventsReader}
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.value.Value.ContractId

import scala.concurrent.{ExecutionContext, Future}

private[dao] sealed class EventsReader(
    val dbDispatcher: DbDispatcher,
    val eventStorageBackend: EventStorageBackend,
    val parameterStorageBackend: ParameterStorageBackend,
    val metrics: LedgerApiServerMetrics,
    val lfValueTranslation: LfValueTranslation,
    val ledgerEndCache: LedgerEndCache,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends LedgerDaoEventsReader
    with NamedLogging {
  private val directEC = DirectExecutionContext(logger)

  protected val dbMetrics: metrics.index.db.type = metrics.index.db

  override def getEventsByContractId(
      contractId: ContractId,
      internalEventFormatO: Option[InternalEventFormat],
  )(implicit loggingContext: LoggingContextWithTrace): Future[GetEventsByContractIdResponse] = {
    implicit val errorLoggingContext: ErrorLoggingContext = ErrorLoggingContext(logger, implicitly)
    internalEventFormatO
      .map { internalEventFormat =>
        for {
          rawEvents <- dbDispatcher.executeSql(dbMetrics.getEventsByContractId)(
            eventStorageBackend.eventReaderQueries.fetchContractIdEvents(
              contractId,
              requestingParties = internalEventFormat.templatePartiesFilter.allFilterParties,
              endEventSequentialId = ledgerEndCache().map(_.lastEventSeqId).getOrElse(0L),
            )
          )
          rawCreatedEvent: Option[Entry[RawCreatedEvent]] = rawEvents.view.collectFirst { entry =>
            entry.event match {
              case created: RawCreatedEvent =>
                entry.copy(event = created)
            }
          }
          rawArchivedEvent: Option[Entry[RawArchivedEvent]] = rawEvents.view.collectFirst { entry =>
            entry.event match {
              case archived: RawArchivedEvent => entry.copy(event = archived)
            }
          }

          rawEventsRestoredWitnesses = restoreWitnessesForTransient(
            rawCreatedEvent,
            rawArchivedEvent,
          )

          rawCreatedEventRestoredWitnesses = rawEventsRestoredWitnesses.view
            .map(_.event)
            .collectFirst { case created: RawCreatedEvent =>
              created
            }

          deserialized <- Future.delegate {
            implicit val ec: ExecutionContext =
              directEC // Scala 2 implicit scope override: shadow the outer scope's implicit by name
            MonadUtil.sequentialTraverse(rawEventsRestoredWitnesses) { event =>
              UpdateReader
                .deserializeRawFlatEvent(
                  internalEventFormat.eventProjectionProperties,
                  lfValueTranslation,
                )(event)
                .map(_ -> event.synchronizerId)
            }
          }

          createEvent = deserialized.flatMap { case (entry, synchronizerId) =>
            entry.event.event.created.map(create => Created(Some(create), synchronizerId))
          }.headOption
          archiveEvent = deserialized.flatMap { case (entry, synchronizerId) =>
            entry.event.event.archived.map(archive => Archived(Some(archive), synchronizerId))
          }.headOption
        } yield {
          Option.when(
            rawCreatedEventRestoredWitnesses
              .exists { createEvent =>
                def witnessesMatch(filter: Option[Set[Party]]): Boolean =
                  filter match {
                    case None =>
                      // wildcard party
                      createEvent.witnessParties.nonEmpty
                    case Some(filterParties) =>
                      filterParties.view.map(_.toString).exists(createEvent.witnessParties)
                  }
                val wildcardPartiesMatch =
                  witnessesMatch(internalEventFormat.templatePartiesFilter.templateWildcardParties)
                def templatePartiesFilterMatch: Boolean =
                  internalEventFormat.templatePartiesFilter.relation
                    .get(createEvent.templateId.toNameTypeConRef)
                    .exists(witnessesMatch)
                wildcardPartiesMatch || templatePartiesFilterMatch
              }
          )(
            GetEventsByContractIdResponse(createEvent, archiveEvent)
          )
        }
      }
      .getOrElse(Future.successful(None))
      .flatMap {
        case Some(result) => Future.successful(result)
        case None =>
          Future.failed(
            RequestValidationErrors.NotFound.ContractEvents
              .Reject(contractId)
              .asGrpcError
          )
      }
  }

  // transient events have empty witnesses, so we need to restore them from the created event
  private def restoreWitnessesForTransient(
      createdEventO: Option[Entry[RawCreatedEvent]],
      archivedEventO: Option[Entry[RawArchivedEvent]],
  ): Seq[Entry[RawFlatEvent]] =
    (createdEventO, archivedEventO) match {
      case (Some(created), Some(archived)) if created.offset == archived.offset =>
        val witnesses = created.event.signatories ++ created.event.observers
        val newCreated = created.copy(event = created.event.copy(witnessParties = witnesses))
        val newArchived = archived.copy(event = archived.event.copy(witnessParties = witnesses))

        Seq(newCreated: Entry[RawFlatEvent], newArchived)
      case _ => (createdEventO.toList: Seq[Entry[RawFlatEvent]]) ++ archivedEventO.toList
    }

  // TODO(i16065): Re-enable getEventsByContractKey tests
//  override def getEventsByContractKey(
//      contractKey: Value,
//      templateId: Ref.Identifier,
//      requestingParties: Set[Party],
//      endExclusiveSeqId: Option[EventSequentialId],
//      maxIterations: Int,
//  )(implicit loggingContext: LoggingContextWithTrace): Future[GetEventsByContractKeyResponse] = {
//    val keyHash: String =
//      platform.Key.assertBuild(templateId, contractKey).hash.bytes.toHexString
//
//    val eventProjectionProperties = EventProjectionProperties(
//      // Used by LfEngineToApi
//      verbose = true,
//      // Needed to get create arguments mapped
//      wildcardWitnesses = requestingParties.map(_.toString),
//    )
//
//    for {
//
//      (
//        rawCreate: Option[FlatEvent.Created],
//        rawArchive: Option[FlatEvent.Archived],
//        eventSequentialId,
//      ) <- dbDispatcher
//        .executeSql(dbMetrics.getEventsByContractKey) { conn =>
//          eventStorageBackend.eventReaderQueries.fetchNextKeyEvents(
//            keyHash,
//            requestingParties,
//            endExclusiveSeqId.getOrElse(ledgerEndCache()._2 + 1),
//            maxIterations,
//          )(conn)
//        }
//
//      createEvent <- rawCreate.fold(Future[Option[CreatedEvent]](None)) { e =>
//        e.deserializeCreateEvent(lfValueTranslation, eventProjectionProperties).map(Some(_))
//      }
//      archiveEvent = rawArchive.map(_.deserializedArchivedEvent())
//
//      continuationToken = eventSequentialId
//        .map(_.toString)
//        .getOrElse(GetEventsByContractKeyResponse.defaultInstance.continuationToken)
//
//    } yield {
//      GetEventsByContractKeyResponse(createEvent, archiveEvent, continuationToken)
//    }
//  }

}
