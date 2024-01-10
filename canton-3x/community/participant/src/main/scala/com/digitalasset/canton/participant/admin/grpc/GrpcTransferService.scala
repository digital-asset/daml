// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.syntax.traverse.*
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.admin.participant.v0.*
import com.digitalasset.canton.data.{CantonTimestamp, TransferSubmitterMetadata}
import com.digitalasset.canton.participant.admin.TransferService
import com.digitalasset.canton.participant.protocol.transfer.TransferData
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.{LfContractId, TransferId}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.{EitherTUtil, OptionUtil}

import scala.concurrent.{ExecutionContext, Future}

class GrpcTransferService(service: TransferService, participantId: ParticipantId)(implicit
    ec: ExecutionContext
) extends TransferServiceGrpc.TransferService {

  import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*

  override def transferOut(request: AdminTransferOutRequest): Future[AdminTransferOutResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val AdminTransferOutRequest(
      submittingPartyP,
      contractIdP,
      sourceDomainP,
      targetDomainP,
      applicationIdP,
      submissionIdP,
      workflowIdP,
      commandIdP,
    ) = request
    val res = for {
      sourceDomain <- mapErr(DomainAlias.create(sourceDomainP))
      targetDomain <- mapErr(DomainAlias.create(targetDomainP))
      contractId <- mapErr(ProtoConverter.parseLfContractId(contractIdP))
      submittingParty <- mapErr(
        EitherT.fromEither[Future](ProtoConverter.parseLfPartyId(submittingPartyP))
      )

      applicationId <- mapErr(ProtoConverter.parseLFApplicationId(applicationIdP))
      submissionId <- mapErr(ProtoConverter.parseLFSubmissionIdO(submissionIdP))
      workflowId <- mapErr(ProtoConverter.parseLFWorkflowIdO(workflowIdP))
      commandId <- mapErr(ProtoConverter.parseCommandId(commandIdP))

      submitterMetadata = TransferSubmitterMetadata(
        submittingParty,
        applicationId,
        participantId.toLf,
        commandId,
        submissionId,
        workflowId,
      )
      transferId <- mapErr(
        service.transferOut(
          submitterMetadata,
          contractId,
          sourceDomain,
          targetDomain,
        )
      )
    } yield AdminTransferOutResponse(transferId = Some(transferId.toAdminProto))
    EitherTUtil.toFuture(res)
  }

  override def transferIn(request: AdminTransferInRequest): Future[AdminTransferInResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val AdminTransferInRequest(
      submittingPartyIdP,
      targetDomainP,
      transferIdP,
      applicationIdP,
      submissionIdP,
      workflowIdP,
      commandIdP,
    ) = request
    val res = for {
      targetDomain <- mapErr(DomainAlias.create(targetDomainP))
      submittingParty <- mapErr(ProtoConverter.parseLfPartyId(submittingPartyIdP))
      transferId <- transferIdP
        .map(id => mapErr(TransferId.fromAdminProtoV0(id)))
        .getOrElse(mapErr(Left(invalidArgument("TransferId not set in transfer-in request"))))

      applicationId <- mapErr(ProtoConverter.parseLFApplicationId(applicationIdP))
      submissionId <- mapErr(ProtoConverter.parseLFSubmissionIdO(submissionIdP))
      workflowId <- mapErr(ProtoConverter.parseLFWorkflowIdO(workflowIdP))
      commandId <- mapErr(ProtoConverter.parseCommandId(commandIdP))
      submitterMetadata = TransferSubmitterMetadata(
        submittingParty,
        applicationId,
        participantId.toLf,
        commandId,
        submissionId,
        workflowId,
      )
      _result <- mapErr(
        service.transferIn(
          submitterMetadata,
          targetDomain,
          transferId,
        )
      )
    } yield AdminTransferInResponse()
    EitherTUtil.toFuture(res)
  }

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

    val res = for {
      filterSourceDomain <- mapErr(DomainAlias.create(filterSourceDomainP))
      filterDomain = if (filterSourceDomainP == "") None else Some(filterSourceDomain)
      searchDomain <- mapErr(DomainAlias.create(searchDomainP))
      filterSubmitterO <- mapErr(
        OptionUtil
          .emptyStringAsNone(filterSubmitterP)
          .map(ProtoConverter.parseLfPartyId)
          .sequence
      )
      filterTimestampO <- mapErr(
        filterTimestampP.map(CantonTimestamp.fromProtoPrimitive).sequence
      )
      transferData <- mapErr(
        service.transferSearch(
          searchDomain,
          filterDomain,
          filterTimestampO,
          filterSubmitterO,
          limit.toInt,
        )
      )
    } yield {
      val searchResultsP = transferData.map(TransferSearchResult(_).toProtoV0)
      AdminTransferSearchResponse(results = searchResultsP)
    }
    EitherTUtil.toFuture(res)
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
  def toProtoV0: AdminTransferSearchResponse.TransferSearchResult =
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
  def fromProtoV0(
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
            .flatMap(TransferId.fromAdminProtoV0)
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
