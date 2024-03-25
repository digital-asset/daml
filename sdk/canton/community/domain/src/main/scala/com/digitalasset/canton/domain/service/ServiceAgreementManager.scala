// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.service

import better.files.File
import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.common.domain.{ServiceAgreement, ServiceAgreementId}
import com.digitalasset.canton.config.CantonRequireTypes.String256M
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{HashOps, HashPurpose, Signature}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.admin.v0
import com.digitalasset.canton.domain.service.store.{
  ServiceAgreementAcceptanceStore,
  ServiceAgreementAcceptanceStoreError,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import slick.jdbc.GetResult

import java.io.IOException
import scala.concurrent.{ExecutionContext, Future}

final case class ServiceAgreementAcceptance(
    agreementId: ServiceAgreementId,
    participantId: ParticipantId,
    signature: Signature,
    timestamp: CantonTimestamp,
) {
  def toProtoV0: v0.ServiceAgreementAcceptance =
    v0.ServiceAgreementAcceptance(
      agreementId = agreementId.toProtoPrimitive,
      participantId = participantId.toProtoPrimitive,
      signature = Some(signature.toProtoV0),
      timestamp = Some(timestamp.toProtoPrimitive),
    )
}

object ServiceAgreementAcceptance {

  implicit def getResultServiceAgreementAcceptance(implicit
      getResultByteArray: GetResult[Array[Byte]]
  ): GetResult[ServiceAgreementAcceptance] =
    GetResult { r =>
      ServiceAgreementAcceptance(ServiceAgreementId.tryCreate(r.<<), r.<<, r.<<, r.<<)
    }

  def fromProtoV0(
      acceptance: v0.ServiceAgreementAcceptance
  ): ParsingResult[ServiceAgreementAcceptance] =
    for {
      signature <- ProtoConverter.parseRequired(
        Signature.fromProtoV0,
        "signature",
        acceptance.signature,
      )
      timestamp <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoPrimitive,
        "timestamp",
        acceptance.timestamp,
      )
      agreementId <- ServiceAgreementId.fromProtoPrimitive(acceptance.agreementId)
      participantId <- ParticipantId.fromProtoPrimitive(acceptance.participantId, "participant_id")
    } yield ServiceAgreementAcceptance(agreementId, participantId, signature, timestamp)

}

/** Manages the agreement of the participants of the Terms of Service. */
class ServiceAgreementManager private (
    val agreement: ServiceAgreement,
    store: ServiceAgreementAcceptanceStore,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  /** Insert that the participant has accepted and signed the agreement. */
  def insertAcceptance(
      agreementId: ServiceAgreementId,
      participantId: ParticipantId,
      signature: Signature,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): EitherT[Future, ServiceAgreementManagerError, Unit] =
    for {
      _ <- EitherT.cond[Future](
        agreement.id == agreementId,
        (),
        ServiceAgreementManagerError.ServiceAgreementMismatch(agreement.id, agreementId),
      )
      acceptance = ServiceAgreementAcceptance(agreementId, participantId, signature, timestamp)
      _ <- store
        .insertAcceptance(acceptance)
        .leftMap[ServiceAgreementManagerError](
          ServiceAgreementManagerError.ServiceAgreementStoreError
        )
    } yield ()

  def listAcceptances()(implicit
      traceContext: TraceContext
  ): EitherT[Future, ServiceAgreementManagerError, Seq[ServiceAgreementAcceptance]] =
    store
      .listAcceptances()
      .leftMap(ServiceAgreementManagerError.ServiceAgreementStoreError)

}

object ServiceAgreementManager {
  def create(
      agreementFile: File,
      storage: Storage,
      hasher: HashOps,
      protocolVersion: ProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): Either[String, ServiceAgreementManager] = {
    for {
      agreementText <- Either
        .catchOnly[IOException](agreementFile.contentAsString)
        .leftMap(err => s"Unable to load service agreement file: $err")
      agreementTextLenLimit <- String256M.create(agreementText)
      hash = hasher.build(HashPurpose.AgreementId).addWithoutLengthPrefix(agreementText).finish()
      agreementId <- ServiceAgreementId.create(hash.toHexString)
      agreement = ServiceAgreement(agreementId, agreementTextLenLimit)
      store = ServiceAgreementAcceptanceStore.create(
        storage,
        protocolVersion,
        timeouts,
        loggerFactory,
      )
    } yield new ServiceAgreementManager(agreement, store, loggerFactory)
  }
}

sealed trait ServiceAgreementManagerError

object ServiceAgreementManagerError {

  final case class ServiceAgreementStoreError(error: ServiceAgreementAcceptanceStoreError)
      extends ServiceAgreementManagerError

  final case class ServiceAgreementMismatch(
      configuredId: ServiceAgreementId,
      acceptedId: ServiceAgreementId,
  ) extends ServiceAgreementManagerError

}
