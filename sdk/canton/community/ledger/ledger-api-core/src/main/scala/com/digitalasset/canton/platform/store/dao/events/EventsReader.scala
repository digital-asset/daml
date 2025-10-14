// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import cats.data.OptionT
import com.daml.ledger.api.v2.event_query_service.{Archived, Created, GetEventsByContractIdResponse}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.participant.store.ContractStore
import com.digitalasset.canton.platform.InternalEventFormat
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  FatCreatedEventProperties,
  RawFatCreatedEvent,
}
import com.digitalasset.canton.platform.store.backend.{EventStorageBackend, ParameterStorageBackend}
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.dao.{DbDispatcher, LedgerDaoEventsReader}
import com.digitalasset.canton.util.FutureInstances
import com.digitalasset.daml.lf.value.Value.ContractId

import scala.concurrent.{ExecutionContext, Future}

private[dao] sealed class EventsReader(
    val dbDispatcher: DbDispatcher,
    val eventStorageBackend: EventStorageBackend,
    val parameterStorageBackend: ParameterStorageBackend,
    val metrics: LedgerApiServerMetrics,
    val lfValueTranslation: LfValueTranslation,
    val contractStore: ContractStore,
    val ledgerEndCache: LedgerEndCache,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends LedgerDaoEventsReader
    with NamedLogging {
  protected val dbMetrics: metrics.index.db.type = metrics.index.db

  override def getEventsByContractId(
      contractId: ContractId,
      internalEventFormatO: Option[InternalEventFormat],
  )(implicit loggingContext: LoggingContextWithTrace): Future[GetEventsByContractIdResponse] = {
    implicit val errorLoggingContext: ErrorLoggingContext = ErrorLoggingContext(logger, implicitly)
    (for {
      internalEventFormat <- OptionT.fromOption[Future](internalEventFormatO)
      internalContractId <- OptionT(
        contractStore
          .lookupBatchedNonCachedInternalIds(List(contractId))
          .failOnShutdownTo(AbortedDueToShutdown.Error().asGrpcError)
          .map(_.values.headOption)
      )
      (create, archiveO) <- OptionT(
        dbDispatcher
          .executeSql(dbMetrics.getEventsByContractId)(
            eventStorageBackend.eventReaderQueries.fetchContractIdEvents(
              internalContractId,
              requestingParties = internalEventFormat.templatePartiesFilter.allFilterParties,
              endEventSequentialId = ledgerEndCache().map(_.lastEventSeqId).getOrElse(0L),
            )
          )
          .map {
            case (None, _) => None
            case (Some(create), archive) => Some(create -> archive)
          }
      )
      // if the fat contract cannot be found we short circuit to none
      fatContract <- OptionT(
        UpdateReader
          .withFatContractIfNeeded(contractStore)(Vector(create))
          .map(_.headOption.flatMap(_._2))
      )
      fatCreatedEvent = RawFatCreatedEvent(
        transactionProperties = create.transactionProperties,
        fatCreatedEventProperties = FatCreatedEventProperties(
          thinCreatedEventProperties = create.thinCreatedEventProperties,
          fatContract = fatContract,
        ),
      )
      // enough to filter the create, they have the same template and witnesses with the archive
      _ <- OptionT.when(
        UpdateReader
          .eventFilter(Some(internalEventFormat.templatePartiesFilter))
          .apply(fatCreatedEvent)
      )(())(FutureInstances.parallelApplicativeFuture)
      deserializedCreateEvent <- OptionT.liftF(
        UpdateReader
          .deserializeRawTransactionEvent(
            eventProjectionProperties = internalEventFormat.eventProjectionProperties,
            lfValueTranslation = lfValueTranslation,
          )(fatCreatedEvent)
          .map(_.getCreated)
      )
      deserializedArchivedEvent = archiveO
        .map(_.copy(filteredStakeholderParties = fatCreatedEvent.witnessParties))
        .map(
          UpdateReader.archivedEvent(
            internalEventFormat.eventProjectionProperties,
            lfValueTranslation,
          )
        )
    } yield GetEventsByContractIdResponse(
      created = Some(
        Created(
          createdEvent = Some(deserializedCreateEvent),
          synchronizerId = fatCreatedEvent.synchronizerId,
        )
      ),
      archived = deserializedArchivedEvent.map(archivedEvent =>
        Archived(
          archivedEvent = Some(archivedEvent),
          synchronizerId = fatCreatedEvent.synchronizerId,
        )
      ),
    )).value.flatMap {
      case Some(result) => Future.successful(result)
      case None =>
        Future.failed(
          RequestValidationErrors.NotFound.ContractEvents
            .Reject(contractId)
            .asGrpcError
        )
    }
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
