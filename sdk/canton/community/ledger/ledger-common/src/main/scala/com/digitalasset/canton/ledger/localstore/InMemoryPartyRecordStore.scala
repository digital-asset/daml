// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.localstore

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.domain.{IdentityProviderId, ObjectMeta}
import com.digitalasset.canton.ledger.api.validation.ResourceAnnotationValidator
import com.digitalasset.canton.ledger.localstore.api.PartyRecordStore.{
  MaxAnnotationsSizeExceeded,
  PartyRecordExistsFatal,
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
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.Party

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, blocking}

object InMemoryPartyRecordStore {
  final case class PartyRecordInfo(
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

class InMemoryPartyRecordStore(
    executionContext: ExecutionContext,
    val loggerFactory: NamedLoggerFactory,
) extends PartyRecordStore
    with NamedLogging {
  import InMemoryPartyRecordStore.*

  implicit private val ec: ExecutionContext = executionContext

  private val state: mutable.TreeMap[Ref.Party, PartyRecordInfo] = mutable.TreeMap()

  override def getPartyRecordO(
      party: Party
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[Option[PartyRecord]]] =
    withState(
      state.get(party) match {
        case Some(info) => Right(Some(toPartyRecord(info)))
        case None => Right(None)
      }
    )

  override def createPartyRecord(
      partyRecord: PartyRecord
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[PartyRecord]] =
    withState(withoutPartyRecord(partyRecord.party) {
      for {
        info <- doCreatePartyRecord(partyRecord)
      } yield toPartyRecord(info)
    })

  override def updatePartyRecord(
      partyRecordUpdate: PartyRecordUpdate,
      ledgerPartyIsLocal: Boolean,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[PartyRecord]] = {
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
        logger.info(s"Updated party record in a participant local store: $updatePartyRecord.")
      })
    } yield updatedPartyRecord
  }

  override def updatePartyRecordIdp(
      party: Party,
      ledgerPartyIsLocal: Boolean,
      sourceIdp: IdentityProviderId,
      targetIdp: IdentityProviderId,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[PartyRecord]] =
    withState {
      state.get(party) match {
        case Some(info) =>
          if (info.identityProviderId != sourceIdp) {
            Left(PartyRecordStore.PartyNotFound(party = party))
          } else {
            val updatedInfo = info.copy(identityProviderId = targetIdp)
            state.put(party, updatedInfo).discard
            Right(toPartyRecord(updatedInfo))
          }
        case None =>
          if (ledgerPartyIsLocal) {
            // When party record doesn't exist
            // it implicitly means that the party belongs to the default idp.
            if (sourceIdp != IdentityProviderId.Default) {
              Left(PartyRecordStore.PartyNotFound(party = party))
            } else {
              val newPartyRecord = PartyRecord(
                party = party,
                metadata = domain.ObjectMeta.empty,
                identityProviderId = targetIdp,
              )
              for {
                info <- doCreatePartyRecord(newPartyRecord)
              } yield toPartyRecord(info)
            }
          } else {
            Left(PartyRecordStore.PartyNotFound(party))
          }
      }
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
      state.put(party, updatedInfo).discard
      updatedInfo
    }
  }

  private def doCreatePartyRecord(
      partyRecord: PartyRecord
  ): Result[PartyRecordInfo] =
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

  private def withState[T](t: => T): Future[T] =
    blocking(
      state.synchronized(
        Future(t)
      )
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
  ): Result[Unit] =
    if (!ResourceAnnotationValidator.isWithinMaxAnnotationsByteSize(annotations)) {
      Left(MaxAnnotationsSizeExceeded(party))
    } else {
      Right(())
    }

  override def filterExistingParties(parties: Set[Party], identityProviderId: IdentityProviderId)(
      implicit loggingContext: LoggingContextWithTrace
  ): Future[Set[Party]] =
    withState {
      parties.map(party => (party, state.get(party))).collect {
        case (party, Some(record)) if record.identityProviderId == identityProviderId => party
      }
    }

  override def filterExistingParties(parties: Set[Party])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Set[Party]] =
    withState {
      parties.map(party => (party, state.get(party))).collect { case (party, Some(_)) =>
        party
      }
    }
}
