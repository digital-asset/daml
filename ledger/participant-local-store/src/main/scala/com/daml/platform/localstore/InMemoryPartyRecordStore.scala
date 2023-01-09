// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{IdentityProviderId, ObjectMeta}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.localstore.api.PartyRecordStore.{
  MaxAnnotationsSizeExceeded,
  PartyRecordExistsFatal,
  Result,
}
import com.daml.platform.localstore.api.{PartyRecord, PartyRecordStore, PartyRecordUpdate}
import com.daml.platform.localstore.utils.LocalAnnotationsUtils
import com.daml.platform.server.api.validation.ResourceAnnotationValidation

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

object InMemoryPartyRecordStore {
  case class PartyRecordInfo(
      party: Ref.Party,
      resourceVersion: Long,
      annotations: Map[String, String],
      identityProviderId: IdentityProviderId,
  )

  def toPartyRecord(info: PartyRecordInfo): PartyRecord =
    PartyRecord(
      party = info.party,
      metadata = ObjectMeta(
        resourceVersionO = Some(info.resourceVersion),
        annotations = info.annotations,
      ),
      identityProviderId = info.identityProviderId,
    )

}

class InMemoryPartyRecordStore(executionContext: ExecutionContext) extends PartyRecordStore {
  import InMemoryPartyRecordStore._

  implicit private val ec: ExecutionContext = executionContext
  private val logger = ContextualizedLogger.get(getClass)

  private val state: mutable.TreeMap[Ref.Party, PartyRecordInfo] = mutable.TreeMap()

  override def getPartyRecordO(
      party: Party
  )(implicit
      loggingContext: LoggingContext
  ): Future[Result[Option[PartyRecord]]] = {
    withState(
      state.get(party) match {
        case Some(info) => Right(Some(toPartyRecord(info)))
        case None => Right(None)
      }
    )
  }

  override def createPartyRecord(
      partyRecord: PartyRecord
  )(implicit loggingContext: LoggingContext): Future[Result[PartyRecord]] = {
    withState(withoutPartyRecord(partyRecord.party) {
      for {
        info <- doCreatePartyRecord(partyRecord)
      } yield toPartyRecord(info)
    })
  }

  override def updatePartyRecord(
      partyRecordUpdate: PartyRecordUpdate,
      ledgerPartyIsLocal: Boolean,
  )(implicit loggingContext: LoggingContext): Future[Result[PartyRecord]] = {
    val party = partyRecordUpdate.party
    for {
      updatedPartyRecord <- withState(
        state.get(party) match {
          case Some(info) =>
            for {
              updatedInfo <- doUpdatePartyRecord(
                partyRecordUpdate = partyRecordUpdate,
                party = party,
                info = info,
              )
            } yield toPartyRecord(updatedInfo)
          case None =>
            if (ledgerPartyIsLocal) {
              val newPartyRecord = PartyRecord(
                party = party,
                metadata = domain.ObjectMeta(
                  resourceVersionO = None,
                  annotations = partyRecordUpdate.metadataUpdate.annotationsUpdateO.getOrElse(
                    Map.empty[String, String]
                  ),
                ),
                identityProviderId = partyRecordUpdate.identityProviderId,
              )
              for {
                info <- doCreatePartyRecord(newPartyRecord)
              } yield toPartyRecord(info)
            } else {
              Left(PartyRecordStore.PartyNotFound(party))
            }
        }
      ).map(tapSuccess { updatePartyRecord =>
        logger.info(s"Updated party record in a participant local store: ${updatePartyRecord}")
      })
    } yield updatedPartyRecord
  }

  private def doUpdatePartyRecord(
      partyRecordUpdate: PartyRecordUpdate,
      party: Party,
      info: PartyRecordInfo,
  ): Result[PartyRecordInfo] = {
    val existingAnnotations = info.annotations
    val updatedAnnotations =
      partyRecordUpdate.metadataUpdate.annotationsUpdateO.fold(existingAnnotations) {
        newAnnotations =>
          LocalAnnotationsUtils.calculateUpdatedAnnotations(
            newValue = newAnnotations,
            existing = existingAnnotations,
          )
      }
    val currentResourceVersion = info.resourceVersion
    val newResourceVersionEither = partyRecordUpdate.metadataUpdate.resourceVersionO match {
      case None => Right(currentResourceVersion + 1)
      case Some(requestResourceVersion) =>
        if (requestResourceVersion == currentResourceVersion) {
          Right(currentResourceVersion + 1)
        } else {
          Left(PartyRecordStore.ConcurrentPartyUpdate(partyRecordUpdate.party))
        }
    }
    for {
      _ <- validateAnnotationsSize(updatedAnnotations, party)
      newResourceVersion <- newResourceVersionEither
    } yield {
      val updatedInfo = PartyRecordInfo(
        party = party,
        resourceVersion = newResourceVersion,
        annotations = updatedAnnotations,
        identityProviderId = partyRecordUpdate.identityProviderId,
      )
      state.put(party, updatedInfo)
      updatedInfo
    }
  }

  private def doCreatePartyRecord(
      partyRecord: PartyRecord
  ): Result[PartyRecordInfo] = {
    for {
      _ <- validateAnnotationsSize(partyRecord.metadata.annotations, partyRecord.party)
    } yield {
      val info = PartyRecordInfo(
        party = partyRecord.party,
        resourceVersion = 0,
        annotations = partyRecord.metadata.annotations,
        identityProviderId = partyRecord.identityProviderId,
      )
      state.update(partyRecord.party, info)
      info
    }
  }

  private def withState[T](t: => T): Future[T] =
    state.synchronized(
      Future(t)
    )

  private def withoutPartyRecord[T](party: Ref.Party)(t: => Result[T]): Result[T] =
    state.get(party) match {
      case Some(_) => Left(PartyRecordExistsFatal(party))
      case None => t
    }

  private def tapSuccess[T](f: T => Unit)(r: Result[T]): Result[T] = {
    r.foreach(f)
    r
  }

  private def validateAnnotationsSize(
      annotations: Map[String, String],
      party: Ref.Party,
  ): Result[Unit] = {
    if (!ResourceAnnotationValidation.isWithinMaxAnnotationsByteSize(annotations)) {
      Left(MaxAnnotationsSizeExceeded(party))
    } else {
      Right(())
    }
  }

  override def partiesExist(parties: Set[Party], identityProviderId: IdentityProviderId)(implicit
      loggingContext: LoggingContext
  ): Future[Set[Party]] = {
    withState {
      parties.map(party => (party, state.get(party))).collect {
        case (party, Some(record)) if record.identityProviderId == identityProviderId => party
      }
    }
  }
}
