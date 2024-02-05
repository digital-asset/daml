// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.ProtoDeserializationError.{FieldNotSet, ProtoDeserializationFailure}
import com.digitalasset.canton.admin.participant.v30.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.TransferService
import com.digitalasset.canton.participant.protocol.transfer.TransferData
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.{LfContractId, TransferId}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.OptionUtil

import scala.concurrent.{ExecutionContext, Future}

class GrpcTransferService(
    service: TransferService,
    participantId: ParticipantId,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends TransferServiceGrpc.TransferService
    with NamedLogging {

  override def transferSearch(
      searchRequest: AdminTransferSearchQuery
  ): Future[AdminTransferSearchResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val AdminTransferSearchQuery(
      searchDomainP,
      filterSourceDomainP,
      filterTimestampP,
      filterSubmitterP,
      limit,
    ) = searchRequest

    for {
      filterSourceDomain <- Future(
        DomainAlias
          .create(filterSourceDomainP)
          .valueOr(err => throw ProtoDeserializationFailure.WrapNoLoggingStr(err).asGrpcError)
      )
      filterDomain = if (filterSourceDomainP == "") None else Some(filterSourceDomain)
      searchDomain <- Future(
        DomainAlias
          .create(searchDomainP)
          .valueOr(err => throw ProtoDeserializationFailure.WrapNoLoggingStr(err).asGrpcError)
      )
      filterSubmitterO <- Future(
        OptionUtil
          .emptyStringAsNone(filterSubmitterP)
          .map(ProtoConverter.parseLfPartyId)
          .sequence
          .valueOr(err => throw ProtoDeserializationFailure.WrapNoLogging(err).asGrpcError)
      )
      filterTimestampO <- Future(
        filterTimestampP
          .map(CantonTimestamp.fromProtoPrimitive)
          .sequence
          .valueOr(err => throw ProtoDeserializationFailure.WrapNoLogging(err).asGrpcError)
      )
      transferData <-
        service
          .transferSearch(
            searchDomain,
            filterDomain,
            filterTimestampO,
            filterSubmitterO,
            limit.toInt,
          )
          .valueOr(err => throw ProtoDeserializationFailure.WrapNoLoggingStr(err).asGrpcError)
    } yield {
      val searchResultsP = transferData.map(TransferSearchResult(_).toProtoV30)
      AdminTransferSearchResponse(results = searchResultsP)
    }
  }
}

final case class TransferSearchResult(
    transferId: TransferId,
    submittingParty: String,
    targetDomain: String,
    sourceDomain: String,
    contractId: LfContractId,
    readyForTransferIn: Boolean,
    targetTimeProofO: Option[CantonTimestamp],
) {
  def toProtoV30: AdminTransferSearchResponse.TransferSearchResult =
    AdminTransferSearchResponse.TransferSearchResult(
      contractId = contractId.toProtoPrimitive,
      transferId = Some(transferId.toAdminProto),
      originDomain = sourceDomain,
      targetDomain = targetDomain,
      submittingParty = submittingParty,
      readyForTransferIn = readyForTransferIn,
      targetTimeProof = targetTimeProofO.map(_.toProtoPrimitive),
    )
}

object TransferSearchResult {
  def fromProtoV30(
      resultP: AdminTransferSearchResponse.TransferSearchResult
  ): ParsingResult[TransferSearchResult] =
    resultP match {
      case AdminTransferSearchResponse
            .TransferSearchResult(
              contractIdP,
              transferIdP,
              sourceDomain,
              targetDomain,
              submitter,
              ready,
              targetTimeProofOP,
            ) =>
        for {
          _ <- Either.cond(contractIdP.nonEmpty, (), FieldNotSet("contractId"))
          contractId <- ProtoConverter.parseLfContractId(contractIdP)
          transferId <- ProtoConverter
            .required("transferId", transferIdP)
            .flatMap(TransferId.fromAdminProto30)
          targetTimeProofO <- targetTimeProofOP.traverse(CantonTimestamp.fromProtoPrimitive)
          _ <- Either.cond(sourceDomain.nonEmpty, (), FieldNotSet("originDomain"))
          _ <- Either.cond(targetDomain.nonEmpty, (), FieldNotSet("targetDomain"))
          _ <- Either.cond(submitter.nonEmpty, (), FieldNotSet("submitter"))
        } yield TransferSearchResult(
          transferId,
          submitter,
          targetDomain,
          sourceDomain,
          contractId,
          ready,
          targetTimeProofO,
        )
    }

  def apply(transferData: TransferData): TransferSearchResult =
    TransferSearchResult(
      transferData.transferId,
      transferData.transferOutRequest.submitter,
      transferData.targetDomain.toProtoPrimitive,
      transferData.sourceDomain.toProtoPrimitive,
      transferData.contract.contractId,
      transferData.transferOutResult.isDefined,
      Some(transferData.transferOutRequest.targetTimeProof.timestamp),
    )
}
