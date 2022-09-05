// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.partymanagement

import java.sql.Connection

import com.daml.api.util.TimeProvider
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.ParticipantParty
import com.daml.ledger.participant.state.index.v2.PartyRecordStore.{
  ConcurrentPartyUpdate,
  MaxAnnotationsSizeExceeded,
  PartyRecordNotFoundOnUpdateException,
  Result,
}
import com.daml.ledger.participant.state.index.v2.{
  AnnotationsUpdate,
  LedgerPartyExists,
  PartyRecordStore,
  PartyRecordUpdate,
}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{DatabaseMetrics, Metrics}
import com.daml.platform.partymanagement.PersistentPartyRecordStore.{
  ConcurrentPartyRecordUpdateDetectedRuntimeException,
  MaxAnnotationsSizeExceededException,
}
import com.daml.ledger.participant.state.index.ResourceAnnotationValidation
import com.daml.platform.store.DbSupport
import com.daml.platform.store.backend.PartyRecordStorageBackend

import scala.concurrent.{ExecutionContext, Future}

object PersistentPartyRecordStore {

  final case class ConcurrentPartyRecordUpdateDetectedRuntimeException(party: Ref.Party)
      extends RuntimeException

  final case class MaxAnnotationsSizeExceededException(party: Ref.Party) extends RuntimeException

}

class PersistentPartyRecordStore(
    dbSupport: DbSupport,
    metrics: Metrics,
    timeProvider: TimeProvider,
    executionContext: ExecutionContext,
) extends PartyRecordStore {

  private implicit val ec: ExecutionContext = executionContext

  private val backend = dbSupport.storageBackendFactory.createPartyRecordStorageBackend
  private val dbDispatcher = dbSupport.dbDispatcher

  private val logger = ContextualizedLogger.get(getClass)

  override def createPartyRecord(partyRecord: domain.ParticipantParty.PartyRecord)(implicit
      loggingContext: LoggingContext
  ): Future[Result[domain.ParticipantParty.PartyRecord]] = {
    inTransaction(_.createPartyRecord) { implicit connection: Connection =>
      for {
        _ <- withoutPartyRecord(id = partyRecord.party) {
          doCreatePartyRecord(partyRecord)(connection)
        }
        createdPartyRecord <- doFetchDomainPartyRecord(party = partyRecord.party)(connection)
      } yield createdPartyRecord
    }.map(tapSuccess { _ =>
      logger.info(
        s"Created new party record in participant local store: ${partyRecord}"
      )
    })(scala.concurrent.ExecutionContext.parasitic)
  }

  override def updatePartyRecord(
      partyRecordUpdate: PartyRecordUpdate,
      ledgerPartyExists: LedgerPartyExists,
  )(implicit
      loggingContext: LoggingContext
  ): Future[Result[domain.ParticipantParty.PartyRecord]] = {
    val party = partyRecordUpdate.party
    // The first transaction attempting to update an existing party record
    val updateFuture = inTransaction(_.getPartyRecord) { implicit connection =>
      backend.getPartyRecord(party = party)(connection) match {
        // Update an existing party record
        case Some(dbPartyRecord) =>
          doUpdatePartyRecord(
            dbPartyRecord = dbPartyRecord,
            partyRecordUpdate = partyRecordUpdate,
          )(connection)
          doFetchDomainPartyRecord(party)(connection)
        // Party record does not exist
        case None =>
          throw PartyRecordNotFoundOnUpdateException
      }
    }.map(tapSuccess { updatePartyRecord =>
      logger.info(s"Updated party record in participant local store: ${updatePartyRecord}")
    })
    updateFuture.recoverWith[Result[domain.ParticipantParty.PartyRecord]] {
      case PartyRecordNotFoundOnUpdateException =>
        onUpdatingNonExistentPartyRecord(
          ledgerPartyExists = ledgerPartyExists,
          party = party,
          annotationsUpdateO = partyRecordUpdate.metadataUpdate.annotationsUpdateO,
        )
    }
  }

  override def getPartyRecord(
      party: Party
  )(implicit loggingContext: LoggingContext): Future[Result[ParticipantParty.PartyRecord]] = {
    inTransaction(_.getPartyRecord) { implicit connection =>
      doFetchDomainPartyRecord(party)
    }
  }

  /** Creates a new party record if the party is known to this participant to exist on a ledger.
    */
  private def onUpdatingNonExistentPartyRecord(
      ledgerPartyExists: LedgerPartyExists,
      party: Party,
      annotationsUpdateO: Option[AnnotationsUpdate],
  )(implicit loggingContext: LoggingContext): Future[Result[ParticipantParty.PartyRecord]] = {
    for {
      // NOTE: We are inspecting a ledger party existence only if we couldn't find the target party record for update.
      partyExistsOnLedger <- ledgerPartyExists.exists(party)
      createdPartyRecord <-
        if (partyExistsOnLedger) {
          // Optional second transaction:
          inTransaction(_.createPartyRecordOnUpdate) { implicit connection =>
            for {
              _ <- withoutPartyRecord(party) {
                val newPartyRecord = domain.ParticipantParty.PartyRecord(
                  party = party,
                  metadata = domain.ObjectMeta(
                    resourceVersionO = None,
                    annotations = annotationsUpdateO.fold(Map.empty[String, String])(_.annotations),
                  ),
                )
                doCreatePartyRecord(newPartyRecord)(connection)
              }
              updatePartyRecord <- doFetchDomainPartyRecord(party)(connection)
            } yield updatePartyRecord
          }.map(tapSuccess { newPartyRecord =>
            logger.info(
              s"Party record to update didn't exist so created a new party record in participant local store: ${newPartyRecord}"
            )
          })(scala.concurrent.ExecutionContext.parasitic)
        } else {
          Future.successful(
            Left(PartyRecordStore.PartyNotFound(party))
          )
        }
    } yield createdPartyRecord
  }

  private def doFetchDomainPartyRecord(
      party: Ref.Party
  )(implicit connection: Connection): Result[ParticipantParty.PartyRecord] = {
    withPartyRecord(id = party) { dbPartyRecord =>
      val annotations = backend.getPartyAnnotations(dbPartyRecord.internalId)(connection)
      toDomainPartyRecord(dbPartyRecord.payload, annotations)
    }
  }

  private def doCreatePartyRecord(
      partyRecord: ParticipantParty.PartyRecord
  )(connection: Connection): Unit = {
    if (
      !ResourceAnnotationValidation
        .isWithinMaxAnnotationsByteSize(partyRecord.metadata.annotations)
    ) {
      throw MaxAnnotationsSizeExceededException(partyRecord.party)
    }
    val now = epochMicroseconds()
    val dbParty = PartyRecordStorageBackend.DbPartyRecordPayload(
      party = partyRecord.party,
      resourceVersion = 0,
      createdAt = now,
    )
    val internalId = backend.createPartyRecord(
      partyRecord = dbParty
    )(connection)
    partyRecord.metadata.annotations.foreach { case (key, value) =>
      backend.addPartyAnnotation(
        internalId = internalId,
        key = key,
        value = value,
        updatedAt = now,
      )(connection)
    }
  }

  private def doUpdatePartyRecord(
      dbPartyRecord: PartyRecordStorageBackend.DbPartyRecord,
      partyRecordUpdate: PartyRecordUpdate,
  )(connection: Connection): Unit = {
    val now = epochMicroseconds()
    // Step 1: Update resource version
    // NOTE: We starts by writing to the 'resource_version' attribute
    //       of 'participant_party_records' to effectively obtain an exclusive lock for
    //       updating this party-record for the rest of the transaction.
    // TODO um-for-hub: See if we can generalize some of this logic between User and PartyRecord stores
    partyRecordUpdate.metadataUpdate.resourceVersionO match {
      case Some(expectedResourceVersion) =>
        if (
          !backend.compareAndIncreaseResourceVersion(
            internalId = dbPartyRecord.internalId,
            expectedResourceVersion = expectedResourceVersion,
          )(connection)
        ) {
          throw ConcurrentPartyRecordUpdateDetectedRuntimeException(
            partyRecordUpdate.party
          )
        }
      case None =>
        backend.increaseResourceVersion(
          internalId = dbPartyRecord.internalId
        )(connection)
    }
    // Step 2: Update annotations
    partyRecordUpdate.metadataUpdate.annotationsUpdateO.foreach { annotationsUpdate =>
      val updatedAnnotations = annotationsUpdate match {
        case AnnotationsUpdate.Merge(newAnnotations) => {
          val existingAnnotations =
            backend.getPartyAnnotations(dbPartyRecord.internalId)(connection)
          existingAnnotations.concat(newAnnotations)
        }
        case AnnotationsUpdate.Replace(newAnnotations) => newAnnotations
      }
      if (
        !ResourceAnnotationValidation
          .isWithinMaxAnnotationsByteSize(updatedAnnotations)
      ) {
        throw MaxAnnotationsSizeExceededException(partyRecordUpdate.party)
      }
      backend.deletePartyAnnotations(internalId = dbPartyRecord.internalId)(connection)
      updatedAnnotations.iterator.foreach { case (key, value) =>
        backend.addPartyAnnotation(
          internalId = dbPartyRecord.internalId,
          key = key,
          value = value,
          updatedAt = now,
        )(connection)
      }
    }
  }

  private def toDomainPartyRecord(
      payload: PartyRecordStorageBackend.DbPartyRecordPayload,
      annotations: Map[String, String],
  ): domain.ParticipantParty.PartyRecord = {
    domain.ParticipantParty.PartyRecord(
      party = payload.party,
      metadata = domain.ObjectMeta(
        resourceVersionO = Some(payload.resourceVersion),
        annotations = annotations,
      ),
    )
  }

  private def withPartyRecord[T](
      id: Ref.Party
  )(
      f: PartyRecordStorageBackend.DbPartyRecord => T
  )(implicit connection: Connection): Result[T] = {
    backend.getPartyRecord(party = id)(connection) match {
      case Some(partyRecord) => Right(f(partyRecord))
      case None => Left(PartyRecordStore.PartyRecordNotFound(party = id))
    }
  }

  private def withoutPartyRecord[T](
      id: Ref.Party
  )(t: => T)(implicit connection: Connection): Result[T] = {
    backend.getPartyRecord(party = id)(connection) match {
      case Some(partyRecord) =>
        Left(PartyRecordStore.PartyRecordExists(party = partyRecord.payload.party))
      case None => Right(t)
    }
  }

  private def inTransaction[T](
      dbMetric: metrics.daml.partyRecordStore.type => DatabaseMetrics
  )(thunk: Connection => Result[T])(implicit loggingContext: LoggingContext): Future[Result[T]] = {
    // TODO um-for-hub: Consider making inTransaction fail transaction when thunk returns a Left. Then we would not have to throw exceptions and do the recovery
    dbDispatcher
      .executeSql(dbMetric(metrics.daml.partyRecordStore))(thunk)
      .recover[Result[T]] {
        case ConcurrentPartyRecordUpdateDetectedRuntimeException(userId) =>
          Left(ConcurrentPartyUpdate(userId))
        case MaxAnnotationsSizeExceededException(userId) => Left(MaxAnnotationsSizeExceeded(userId))
      }(ExecutionContext.parasitic)
  }

  private def tapSuccess[T](f: T => Unit)(r: Result[T]): Result[T] = {
    r.foreach(f)
    r
  }

  private def epochMicroseconds(): Long = {
    val now = timeProvider.getCurrentTime
    (now.getEpochSecond * 1000 * 1000) + (now.getNano / 1000)
  }
}
