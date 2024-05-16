// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.localstore

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.metrics.DatabaseMetrics
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.domain.IdentityProviderId
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.api.validation.ResourceAnnotationValidator
import com.digitalasset.canton.ledger.localstore.PersistentPartyRecordStore.{
  ConcurrentPartyRecordUpdateDetectedRuntimeException,
  MaxAnnotationsSizeExceededException,
}
import com.digitalasset.canton.ledger.localstore.api.PartyRecordStore.{
  ConcurrentPartyUpdate,
  MaxAnnotationsSizeExceeded,
  Result,
}
import com.digitalasset.canton.ledger.localstore.api.{
  PartyRecord,
  PartyRecordStore,
  PartyRecordUpdate,
}
import com.digitalasset.canton.ledger.localstore.utils.LocalAnnotationsUtils
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.DbSupport
import com.digitalasset.canton.platform.store.backend.localstore.PartyRecordStorageBackend

import java.sql.Connection
import scala.concurrent.{ExecutionContext, Future}

object PersistentPartyRecordStore {

  final case class ConcurrentPartyRecordUpdateDetectedRuntimeException(party: Ref.Party)
      extends RuntimeException

  final case class MaxAnnotationsSizeExceededException(party: Ref.Party) extends RuntimeException
}

class PersistentPartyRecordStore(
    dbSupport: DbSupport,
    metrics: LedgerApiServerMetrics,
    timeProvider: TimeProvider,
    executionContext: ExecutionContext,
    val loggerFactory: NamedLoggerFactory,
) extends PartyRecordStore
    with NamedLogging {

  private implicit val ec: ExecutionContext = executionContext

  private val directEc = DirectExecutionContext(noTracingLogger)

  private val backend = dbSupport.storageBackendFactory.createPartyRecordStorageBackend
  private val dbDispatcher = dbSupport.dbDispatcher

  override def createPartyRecord(partyRecord: PartyRecord)(implicit
      loggingContext: LoggingContextWithTrace
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
    })(directEc)
  }

  override def updatePartyRecord(
      partyRecordUpdate: PartyRecordUpdate,
      ledgerPartyIsLocal: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[PartyRecord]] = {
    val party = partyRecordUpdate.party
    for {
      updatedPartyRecord <- inTransaction(_.updatePartyRecord) { implicit connection =>
        backend.getPartyRecord(party = party)(connection) match {
          // Update an existing party record
          case Some(dbPartyRecord) =>
            doUpdatePartyRecord(
              dbPartyRecord = dbPartyRecord,
              partyRecordUpdate = partyRecordUpdate,
            )(connection)
            doFetchDomainPartyRecord(party)(connection)
          // Party record does not exist, but party is local to participant
          case None =>
            if (ledgerPartyIsLocal) {
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

  override def updatePartyRecordIdp(
      party: Party,
      ledgerPartyIsLocal: Boolean,
      sourceIdp: IdentityProviderId,
      targetIdp: IdentityProviderId,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[PartyRecord]] = {
    for {
      updatedPartyRecord <- inTransaction(_.updatePartyRecordIdp) { implicit connection =>
        backend.getPartyRecord(party = party)(connection) match {
          // Update an existing party record
          case Some(dbPartyRecord) =>
            if (dbPartyRecord.payload.identityProviderId != sourceIdp.toDb) {
              Left(PartyRecordStore.PartyNotFound(party = party))
            } else {
              backend
                .updatePartyRecordIdp(
                  internalId = dbPartyRecord.internalId,
                  identityProviderId = targetIdp.toDb,
                )(connection)
                .discard
              doFetchDomainPartyRecord(party)(connection)
            }
          case None =>
            // Party record does not exist, but party is local to participant
            if (ledgerPartyIsLocal) {
              // When party record doesn't exist
              // it means that the party implicitly belongs to the default idp.
              if (sourceIdp != IdentityProviderId.Default) {
                Left(PartyRecordStore.PartyNotFound(party = party))
              } else {
                for {
                  _ <- withoutPartyRecord(party) {
                    val newPartyRecord = PartyRecord(
                      party = party,
                      identityProviderId = targetIdp,
                      metadata = domain.ObjectMeta.empty,
                    )
                    doCreatePartyRecord(newPartyRecord)(connection)
                  }
                  updatePartyRecord <- doFetchDomainPartyRecord(party)(connection)
                } yield updatePartyRecord
              }
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
      loggingContext: LoggingContextWithTrace
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
      !ResourceAnnotationValidator
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
    //       of 'lapi_party_records' to effectively obtain an exclusive lock for
    //       updating this party-record for the rest of the transaction.
    val _ = partyRecordUpdate.metadataUpdate.resourceVersionO match {
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
        !ResourceAnnotationValidator
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
      dbMetric: metrics.partyRecordStore.type => DatabaseMetrics
  )(
      thunk: Connection => Result[T]
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[T]] = {
    dbDispatcher
      .executeSql(dbMetric(metrics.partyRecordStore))(thunk)
      .recover[Result[T]] {
        case ConcurrentPartyRecordUpdateDetectedRuntimeException(userId) =>
          Left(ConcurrentPartyUpdate(userId))
        case MaxAnnotationsSizeExceededException(userId) => Left(MaxAnnotationsSizeExceeded(userId))
      }(directEc)
  }

  private def tapSuccess[T](f: T => Unit)(r: Result[T]): Result[T] = {
    r.foreach(f)
    r
  }

  private def epochMicroseconds(): Long = {
    val now = timeProvider.getCurrentTime
    (now.getEpochSecond * 1000 * 1000) + (now.getNano / 1000)
  }

  override def filterExistingParties(parties: Set[Party], identityProviderId: IdentityProviderId)(
      implicit loggingContext: LoggingContextWithTrace
  ): Future[Set[Party]] = inTransaction(_.partiesExist) { implicit connection =>
    Right(backend.filterExistingParties(parties, identityProviderId.toDb)(connection))
  }.map {
    case Right(value) => value
    case Left(_) => Set.empty // It is always `Right`, see few lines above
  }

  override def filterExistingParties(parties: Set[Party])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Set[Party]] = inTransaction(_.partiesExist) { implicit connection =>
    Right(backend.filterExistingParties(parties)(connection))
  }.map {
    case Right(value) => value
    case Left(_) => Set.empty // It is always `Right`, see few lines above
  }
}
