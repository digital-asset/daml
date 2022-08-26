// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.impl.inmemory

import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{ObjectMeta, ParticipantParty}
import com.daml.ledger.participant.state.index.v2.ParticipantPartyRecordStore.{
  PartyRecordExists,
  PartyRecordNotFound,
  PartyRecordNotFoundOnUpdateException,
  Result,
}
import com.daml.ledger.participant.state.index.v2.{
  AnnotationsUpdate,
  LedgerPartyExists,
  ParticipantPartyRecordStore,
  PartyRecordUpdate,
}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.logging.{ContextualizedLogger, LoggingContext}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

object InMemoryParticipantPartyRecordStore {
  case class PartyRecordInfo(
      party: Ref.Party,
      resourceVersion: Int,
      annotations: Map[String, String],
  )

  def toPartyRecord(info: PartyRecordInfo): ParticipantParty.PartyRecord =
    ParticipantParty.PartyRecord(
      party = info.party,
      metadata = ObjectMeta(
        resourceVersionO = Some(info.resourceVersion.toString),
        annotations = info.annotations,
      ),
    )

}

class InMemoryParticipantPartyRecordStore(executionContext: ExecutionContext)
    extends ParticipantPartyRecordStore {
  import InMemoryParticipantPartyRecordStore._

  implicit private val ec: ExecutionContext = executionContext
  private val logger = ContextualizedLogger.get(getClass)

  private val state: mutable.TreeMap[Ref.Party, PartyRecordInfo] = mutable.TreeMap()

  override def getPartyRecord(
      party: Party
  )(implicit loggingContext: LoggingContext): Future[Result[ParticipantParty.PartyRecord]] = {
    withState(withPartyRecord[ParticipantParty.PartyRecord](party)(toPartyRecord))
  }

  override def createPartyRecord(
      partyRecord: ParticipantParty.PartyRecord
  )(implicit loggingContext: LoggingContext): Future[Result[ParticipantParty.PartyRecord]] = {
    withState(withoutPartyRecord(partyRecord.party) {
      val info = doCreatePartyRecord(partyRecord)
      toPartyRecord(info)
    })
  }

  override def updatePartyRecord(
      partyRecordUpdate: PartyRecordUpdate,
      ledgerPartyExists: LedgerPartyExists,
  )(implicit loggingContext: LoggingContext): Future[Result[ParticipantParty.PartyRecord]] = {
    val party = partyRecordUpdate.party
    val attemptedUpdateF = withState(
      state.get(party) match {
        case Some(info) =>
          val updatedInfo: PartyRecordInfo = doUpdatePartyRecord(
            partyRecordUpdate = partyRecordUpdate,
            party = party,
            info = info,
          )
          Right(toPartyRecord(updatedInfo))
        case None =>
          throw PartyRecordNotFoundOnUpdateException
      }
    ).map(tapSuccess { updatePartyRecord =>
      logger.info(s"Updated party record in a participant local store: ${updatePartyRecord}")
    })
    attemptedUpdateF.recoverWith[Result[domain.ParticipantParty.PartyRecord]] {
      case PartyRecordNotFoundOnUpdateException =>
        for {
          partyExistsOnLedger <- ledgerPartyExists.exists(party)
          createdPartyRecord <-
            if (partyExistsOnLedger) {
              withState {
                val newPartyRecord = domain.ParticipantParty.PartyRecord(
                  party = party,
                  metadata = domain.ObjectMeta.empty,
                )
                for {
                  _ <- withoutPartyRecord(party = newPartyRecord.party) {
                    doCreatePartyRecord(newPartyRecord)
                  }
                  _ <- withPartyRecord(party) { info =>
                    val updatedInfo: PartyRecordInfo = doUpdatePartyRecord(
                      partyRecordUpdate = partyRecordUpdate,
                      party = party,
                      info = info,
                    )
                    toPartyRecord(updatedInfo)
                  }
                  updatePartyRecord <- {
                    withPartyRecord(party) { updatedInfo =>
                      toPartyRecord(updatedInfo)
                    }
                  }
                } yield updatePartyRecord
              }.map(tapSuccess { newPartyRecord =>
                logger.error(
                  s"Created a new party record in a participant local store: ${newPartyRecord}"
                )
              })(scala.concurrent.ExecutionContext.parasitic)
            } else {
              Future.successful(
                Left(ParticipantPartyRecordStore.PartyNotFound(party))
              )
            }
        } yield createdPartyRecord
    }
  }

  private def doUpdatePartyRecord(
      partyRecordUpdate: PartyRecordUpdate,
      party: Party,
      info: PartyRecordInfo,
  ): PartyRecordInfo = {
    val existingAnnotations = info.annotations
    val updatedAnnotations =
      partyRecordUpdate.metadataUpdate.annotationsUpdateO.fold(existingAnnotations) {
        case AnnotationsUpdate.Merge(newAnnotations) => existingAnnotations.concat(newAnnotations)
        case AnnotationsUpdate.Replace(newAnnotations) => newAnnotations
      }
    val updatedInfo = PartyRecordInfo(
      party = party,
      resourceVersion = info.resourceVersion + 1,
      annotations = updatedAnnotations,
    )
    state.put(party, updatedInfo)
    updatedInfo
  }

  private def doCreatePartyRecord(
      partyRecord: ParticipantParty.PartyRecord
  ): PartyRecordInfo = {
    val info = PartyRecordInfo(
      party = partyRecord.party,
      resourceVersion = 0,
      annotations = partyRecord.metadata.annotations,
    )
    state.update(partyRecord.party, info)
    info
  }

  private def withState[T](t: => T): Future[T] =
    state.synchronized(
      Future(t)
    )

  private def withPartyRecord[T](
      party: Ref.Party
  )(f: InMemoryParticipantPartyRecordStore.PartyRecordInfo => T): Result[T] =
    state.get(party) match {
      case Some(partyRecord) => Right(f(partyRecord))
      case None => Left(PartyRecordNotFound(party))
    }

  private def withoutPartyRecord[T](party: Ref.Party)(t: => T): Result[T] =
    state.get(party) match {
      case Some(_) => Left(PartyRecordExists(party))
      case None => Right(t)
    }

  private def tapSuccess[T](f: T => Unit)(r: Result[T]): Result[T] = {
    r.foreach(f)
    r
  }
}
