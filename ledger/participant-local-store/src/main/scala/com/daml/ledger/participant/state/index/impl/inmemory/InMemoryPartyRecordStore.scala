// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.impl.inmemory

import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{ObjectMeta, ParticipantParty}
import com.daml.ledger.participant.state.index.{LocalAnnotationsUtils, ResourceAnnotationValidation}
import com.daml.ledger.participant.state.index.v2.PartyRecordStore.{
  MaxAnnotationsSizeExceeded,
  PartyRecordExistsFatal,
  Result,
}
import com.daml.ledger.participant.state.index.v2.{
  LedgerPartyExists,
  PartyRecordStore,
  PartyRecordUpdate,
}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.logging.{ContextualizedLogger, LoggingContext}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

object InMemoryPartyRecordStore {
  case class PartyRecordInfo(
      party: Ref.Party,
      resourceVersion: Long,
      annotations: Map[String, String],
  )

  def toPartyRecord(info: PartyRecordInfo): ParticipantParty.PartyRecord =
    ParticipantParty.PartyRecord(
      party = info.party,
      metadata = ObjectMeta(
        resourceVersionO = Some(info.resourceVersion),
        annotations = info.annotations,
      ),
    )

}

// TODO um-for-hub: Consider unifying InMemoryPartyRecordStore and PersistentPartyRecordStore, such that InMemoryPartyRecordStore is obtained by having a in-memory storage backend
class InMemoryPartyRecordStore(executionContext: ExecutionContext) extends PartyRecordStore {
  import InMemoryPartyRecordStore._

  implicit private val ec: ExecutionContext = executionContext
  private val logger = ContextualizedLogger.get(getClass)

  private val state: mutable.TreeMap[Ref.Party, PartyRecordInfo] = mutable.TreeMap()

  override def getPartyRecordO(
      party: Party
  )(implicit
      loggingContext: LoggingContext
  ): Future[Result[Option[ParticipantParty.PartyRecord]]] = {
    withState(
      state.get(party) match {
        case Some(info) => Right(Some(toPartyRecord(info)))
        case None => Right(None)
      }
    )
  }

  override def createPartyRecord(
      partyRecord: ParticipantParty.PartyRecord
  )(implicit loggingContext: LoggingContext): Future[Result[ParticipantParty.PartyRecord]] = {
    withState(withoutPartyRecord(partyRecord.party) {
      for {
        info <- doCreatePartyRecord(partyRecord)
      } yield toPartyRecord(info)
    })
  }

  // TODO um-for-hub: Add a conformance test exercising a race conditions: multiple update on the same non-existing party-record (which exists on the ledger and is indexed by this participant) calls
  override def updatePartyRecord(
      partyRecordUpdate: PartyRecordUpdate,
      ledgerPartyExists: LedgerPartyExists,
  )(implicit loggingContext: LoggingContext): Future[Result[ParticipantParty.PartyRecord]] = {
    val party = partyRecordUpdate.party
    for {
      partyExistsOnLedger <- ledgerPartyExists.exists(party)
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
            if (partyExistsOnLedger) {
              val newPartyRecord = domain.ParticipantParty.PartyRecord(
                party = party,
                metadata = domain.ObjectMeta(
                  resourceVersionO = None,
                  annotations = partyRecordUpdate.metadataUpdate.annotationsUpdateO.getOrElse(
                    Map.empty[String, String]
                  ),
                ),
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
      )
      state.put(party, updatedInfo)
      updatedInfo
    }
  }

  private def doCreatePartyRecord(
      partyRecord: ParticipantParty.PartyRecord
  ): Result[PartyRecordInfo] = {
    for {
      _ <- validateAnnotationsSize(partyRecord.metadata.annotations, partyRecord.party)
    } yield {
      val info = PartyRecordInfo(
        party = partyRecord.party,
        resourceVersion = 0,
        annotations = partyRecord.metadata.annotations,
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
}
