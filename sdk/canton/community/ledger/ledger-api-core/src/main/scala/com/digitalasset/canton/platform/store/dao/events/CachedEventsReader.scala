// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v1
import com.daml.ledger.api.v1.event_query_service.GetEventsByContractKeyResponse
import com.daml.ledger.api.v2.event_query_service.GetEventsByContractIdResponse
import com.daml.lf.transaction.GlobalKey
import com.digitalasset.canton.ledger.api.messages.event.KeyContinuationToken
import com.digitalasset.canton.ledger.api.messages.event.KeyContinuationToken.*
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.participant.util.LfEngineToApi
import com.digitalasset.canton.platform.store.cache.EventsByContractKeyCache
import com.digitalasset.canton.platform.store.cache.EventsByContractKeyCache.KeyUpdates
import com.digitalasset.canton.platform.store.dao.{EventProjectionProperties, LedgerDaoEventsReader}
import com.digitalasset.canton.platform.{ContractId, Party}

import scala.concurrent.{ExecutionContext, Future}

class CachedEventsReader(
    delegate: LedgerDaoEventsReader,
    eventsByContractKeyCache: EventsByContractKeyCache,
    lfValueTranslation: LfValueTranslation,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends LedgerDaoEventsReader
    with NamedLogging {

  override def getEventsByContractId(contractId: ContractId, requestingParties: Set[Party])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[GetEventsByContractIdResponse] =
    delegate.getEventsByContractId(contractId, requestingParties)

  override def getEventsByContractKey(
      contractKey: GlobalKey,
      requestingParties: Set[Party],
      keyContinuationToken: KeyContinuationToken,
      maxIterations: Int,
  )(implicit loggingContext: LoggingContextWithTrace): Future[GetEventsByContractKeyResponse] = {
    def fromDb(): Future[GetEventsByContractKeyResponse] = {
      logger.debug(s"Serving getEventsByContractKey from DB")(loggingContext.traceContext)
      delegate
        .getEventsByContractKey(
          contractKey,
          requestingParties,
          keyContinuationToken,
          maxIterations = 1000,
        )
    }

    def processCached: KeyUpdates => Future[GetEventsByContractKeyResponse] = {
      case keyUpdates
          if keyUpdates.create.flatEventWitnesses.intersect(requestingParties).isEmpty =>
        // None of the readers are observers over this contract, hence fallback to DB access
        fromDb()

      case KeyUpdates(_key, createUpdate, exerciseOpt) =>
        logger.debug("Serving getEventsByContractKey from cache")(loggingContext.traceContext)

        val archiveEventOpt = exerciseOpt.map { exercise =>
          v1.event.ArchivedEvent(
            eventId = exercise.eventId.toLedgerString,
            contractId = exercise.contractId.coid,
            templateId = Some(LfEngineToApi.toApiIdentifier(exercise.templateId)),
            witnessParties = exercise.flatEventWitnesses.toSeq,
          )
        }

        val eventProjectionProperties = EventProjectionProperties(
          // Used by LfEngineToApi
          verbose = true,
          // Needed to get create arguments mapped
          wildcardWitnesses = requestingParties.map(_.toString),
        )

        TransactionsReader
          .deserializeCreatedEvent(
            lfValueTranslation,
            requestingParties,
            eventProjectionProperties,
          )(createUpdate)
          .map { create =>
            GetEventsByContractKeyResponse(
              createEvent = Some(create),
              archiveEvent = archiveEventOpt,
              continuationToken = toTokenString(EndExclusiveEventIdToken(createUpdate.eventId)),
            )
          }
    }

    // If there's a continuation token provided we won't be able to serve from the cache, so go to the DB directly
    keyContinuationToken match {
      case NoToken => eventsByContractKeyCache.get(contractKey).fold(fromDb())(processCached)
      case _ => fromDb()
    }
  }
}
