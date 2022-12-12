// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import java.sql.Connection
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.IdentityProviderId
import com.daml.platform.localstore.api.PartyRecordStore.{
  ConcurrentPartyUpdate,
  MaxAnnotationsSizeExceeded,
  Result,
}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{DatabaseMetrics, Metrics}
import com.daml.platform.localstore.PersistentPartyRecordStore.{
  ConcurrentPartyRecordUpdateDetectedRuntimeException,
  MaxAnnotationsSizeExceededException,
}
import com.daml.platform.localstore.api.{
  LedgerPartyExists,
  PartyRecord,
  PartyRecordStore,
  PartyRecordUpdate,
}
import com.daml.platform.localstore.utils.LocalAnnotationsUtils
import com.daml.platform.server.api.validation.ResourceAnnotationValidation
import com.daml.platform.store.DbSupport
import com.daml.platform.store.backend.localstore.PartyRecordStorageBackend

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

  override def createPartyRecord(partyRecord: PartyRecord)(implicit
      loggingContext: LoggingContext
  ): Future[Result[PartyRecord]] = {
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
  ): Future[Result[PartyRecord]] = {
    val party = partyRecordUpdate.party
    for {
      partyExistsOnLedger <- ledgerPartyExists.exists(party)
      updatedPartyRecord <- inTransaction(_.updatePartyRecord) { implicit connection =>
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
            if (partyExistsOnLedger) {
              for {
                _ <- withoutPartyRecord(party) {
                  val newPartyRecord = PartyRecord(
                    party = party,
                    identityProviderId = partyRecordUpdate.identityProviderId,
                    metadata = domain.ObjectMeta(
                      resourceVersionO = None,
                      annotations = partyRecordUpdate.metadataUpdate.annotationsUpdateO.getOrElse(
                        Map.empty[String, String]
                      ),
                    ),
                  )
                  doCreatePartyRecord(newPartyRecord)(connection)
                }
                updatePartyRecord <- doFetchDomainPartyRecord(party)(connection)
              } yield updatePartyRecord
            } else {
              Left(PartyRecordStore.PartyNotFound(party))
            }
        }
      }.map(tapSuccess { updatePartyRecord =>
        logger.info(s"Updated party record in participant local store: ${updatePartyRecord}")
      })
    } yield updatedPartyRecord
  }

  override def getPartyRecordO(
      party: Party
  )(implicit
      loggingContext: LoggingContext
  ): Future[Result[Option[PartyRecord]]] = {
    inTransaction(_.getPartyRecord) { implicit connection =>
      doFetchDomainPartyRecordO(party)
    }
  }

  private def doFetchDomainPartyRecord(
      party: Ref.Party
  )(implicit connection: Connection): Result[PartyRecord] = {
    withPartyRecord(id = party) { dbPartyRecord =>
      val annotations = backend.getPartyAnnotations(dbPartyRecord.internalId)(connection)
      toDomainPartyRecord(dbPartyRecord.payload, annotations)
    }
  }

  private def doFetchDomainPartyRecordO(
      party: Ref.Party
  )(implicit connection: Connection): Result[Option[PartyRecord]] = {
    backend.getPartyRecord(party = party)(connection) match {
      case Some(dbPartyRecord) =>
        val annotations = backend.getPartyAnnotations(dbPartyRecord.internalId)(connection)
        Right(Some(toDomainPartyRecord(dbPartyRecord.payload, annotations)))
      case None => Right(None)
    }
  }

  private def doCreatePartyRecord(
      partyRecord: PartyRecord
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
      identityProviderId = partyRecord.identityProviderId.toDb,
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
    partyRecordUpdate.metadataUpdate.annotationsUpdateO.foreach { newAnnotations =>
      val existingAnnotations = backend.getPartyAnnotations(dbPartyRecord.internalId)(connection)
      val updatedAnnotations = LocalAnnotationsUtils.calculateUpdatedAnnotations(
        newValue = newAnnotations,
        existing = existingAnnotations,
      )
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
  ): PartyRecord = {
    PartyRecord(
      party = payload.party,
      identityProviderId = IdentityProviderId.fromDb(payload.identityProviderId),
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
      case None => Left(PartyRecordStore.PartyRecordNotFoundFatal(party = id))
    }
  }

  private def withoutPartyRecord[T](
      id: Ref.Party
  )(t: => T)(implicit connection: Connection): Result[T] = {
    backend.getPartyRecord(party = id)(connection) match {
      case Some(partyRecord) =>
        Left(PartyRecordStore.PartyRecordExistsFatal(party = partyRecord.payload.party))
      case None => Right(t)
    }
  }

  private def inTransaction[T](
      dbMetric: metrics.daml.partyRecordStore.type => DatabaseMetrics
  )(thunk: Connection => Result[T])(implicit loggingContext: LoggingContext): Future[Result[T]] = {
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
