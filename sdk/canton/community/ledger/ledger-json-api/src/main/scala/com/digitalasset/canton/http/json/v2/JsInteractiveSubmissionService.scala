// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.ledger.api.v2.interactive.interactive_submission_service
import com.daml.ledger.api.v2.interactive.interactive_submission_service.InteractiveSubmissionServiceGrpc
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput, v2Endpoint}
import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf
import io.circe.*
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.{AnyEndpoint, Schema, stringToPath}

import scala.concurrent.{ExecutionContext, Future}

class JsInteractiveSubmissionService(
    ledgerClient: LedgerClient,
    protocolConverters: ProtocolConverters,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext
) extends Endpoints
    with NamedLogging {

  private def interactiveSubmissionServiceClient(token: Option[String])(implicit
      traceContext: TraceContext
  ): interactive_submission_service.InteractiveSubmissionServiceGrpc.InteractiveSubmissionServiceStub =
    ledgerClient.serviceClient(InteractiveSubmissionServiceGrpc.stub, token)

  @SuppressWarnings(
    Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable")
  ) // no idea why this service is affected by this wart (compare to others)
  def endpoints() = List(
    withServerLogic(
      JsInteractiveSubmissionService.prepareEndpoint,
      prepare,
    ),
    withServerLogic(
      JsInteractiveSubmissionService.executeEndpoint,
      execute,
    ),
  )

  def prepare(callerContext: CallerContext): TracedInput[JsPrepareSubmissionRequest] => Future[
    Either[JsCantonError, JsPrepareSubmissionResponse]
  ] = req => {
    implicit val token: Option[String] = callerContext.token()
    implicit val tc: TraceContext = req.traceContext
    for {
      grpcReq <- protocolConverters.PrepareSubmissionRequest.fromJson(req.in)
      grpcResp <- interactiveSubmissionServiceClient(token).prepareSubmission(grpcReq)
      jsResp <- protocolConverters.PrepareSubmissionResponse.toJson(grpcResp).resultToRight
    } yield jsResp
  }

  def execute(callerContext: CallerContext): TracedInput[JsExecuteSubmissionRequest] => Future[
    Either[JsCantonError, interactive_submission_service.ExecuteSubmissionResponse]
  ] = req => {
    implicit val token: Option[String] = callerContext.token()
    implicit val tc: TraceContext = req.traceContext
    for {
      grpcReq <- protocolConverters.ExecuteSubmissionRequest.fromJson(req.in)
      grpcResp <- interactiveSubmissionServiceClient(token).executeSubmission(grpcReq).resultToRight
    } yield grpcResp
  }
}

final case class JsPrepareSubmissionRequest(
    userId: String,
    commandId: String,
    commands: Seq[JsCommand.Command],
    minLedgerTime: Option[interactive_submission_service.MinLedgerTime],
    actAs: Seq[String],
    readAs: Seq[String],
    disclosedContracts: Seq[com.daml.ledger.api.v2.commands.DisclosedContract],
    synchronizerId: String,
    packageIdSelectionPreference: Seq[String],
    verboseHashing: Boolean,
)

final case class JsPrepareSubmissionResponse(
    preparedTransaction: Option[protobuf.ByteString],
    preparedTransactionHash: protobuf.ByteString,
    hashingSchemeVersion: interactive_submission_service.HashingSchemeVersion,
    hashingDetails: Option[String],
)

final case class JsExecuteSubmissionRequest(
    preparedTransaction: Option[protobuf.ByteString],
    partySignatures: Option[interactive_submission_service.PartySignatures],
    deduplicationPeriod: interactive_submission_service.ExecuteSubmissionRequest.DeduplicationPeriod,
    submissionId: String,
    userId: String,
    hashingSchemeVersion: interactive_submission_service.HashingSchemeVersion,
)

object JsInteractiveSubmissionService extends DocumentationEndpoints {
  import JsInteractiveSubmissionServiceCodecs.*
  private lazy val interactiveSubmission =
    v2Endpoint.in(sttp.tapir.stringToPath("interactive-submission"))
  val prepareEndpoint = interactiveSubmission.post
    .in(stringToPath("prepare"))
    .in(jsonBody[JsPrepareSubmissionRequest])
    .out(jsonBody[JsPrepareSubmissionResponse])
    .description("Prepare commands for signing")

  val executeEndpoint = interactiveSubmission.post
    .in(stringToPath("execute"))
    .in(jsonBody[JsExecuteSubmissionRequest])
    .out(jsonBody[interactive_submission_service.ExecuteSubmissionResponse])
    .description("Execute a signed transaction")

  override def documentation: Seq[AnyEndpoint] = Seq(prepareEndpoint, executeEndpoint)
}

object JsInteractiveSubmissionServiceCodecs {
  import JsCommandServiceCodecs.*

  implicit val timeRW: Codec[interactive_submission_service.MinLedgerTime.Time] = deriveCodec
  implicit val timeMinLedgerTimeRelRW
      : Codec[interactive_submission_service.MinLedgerTime.Time.MinLedgerTimeRel] = deriveCodec
  implicit val timeMinLedgerTimeAbsRW
      : Codec[interactive_submission_service.MinLedgerTime.Time.MinLedgerTimeAbs] = deriveCodec
  implicit val minLedgerTimeRW: Codec[interactive_submission_service.MinLedgerTime] = deriveCodec

  implicit val jsPrepareSubmissionRequestRW: Codec[JsPrepareSubmissionRequest] = deriveCodec

  implicit val prepareSubmissionResponseRW: Codec[JsPrepareSubmissionResponse] = deriveCodec

  implicit val hashingSchemeVersionRW: Codec[interactive_submission_service.HashingSchemeVersion] =
    deriveCodec
  implicit val hashingSchemeVersionRecognizedRW
      : Codec[interactive_submission_service.HashingSchemeVersion.Recognized] = deriveCodec
  implicit val hashingSchemeVersionUnrecognizedRW
      : Codec[interactive_submission_service.HashingSchemeVersion.Unrecognized] = deriveCodec

  implicit val executeSubmissionResponseRW
      : Codec[interactive_submission_service.ExecuteSubmissionResponse] =
    deriveCodec

  implicit val esrDeduplicationPeriodRW
      : Codec[interactive_submission_service.ExecuteSubmissionRequest.DeduplicationPeriod] =
    deriveCodec

  implicit val partySignaturesRW: Codec[interactive_submission_service.PartySignatures] =
    deriveCodec

  implicit val singlePartySignaturesRW
      : Codec[interactive_submission_service.SinglePartySignatures] =
    deriveCodec

  implicit val signatureRW: Codec[interactive_submission_service.Signature] =
    deriveCodec

  implicit val signingAlgorithmSpecUnrecognizedRW
      : Codec[interactive_submission_service.SigningAlgorithmSpec.Unrecognized] =
    deriveCodec

  implicit val signingAlgorithmSpecRecognizedRW
      : Codec[interactive_submission_service.SigningAlgorithmSpec.Recognized] =
    deriveCodec

  implicit val signingAlgorithmSpecRW: Codec[interactive_submission_service.SigningAlgorithmSpec] =
    deriveCodec

  implicit val signatureFormatRW: Codec[interactive_submission_service.SignatureFormat] =
    deriveCodec

  implicit val signatureFormatUnrecognizedRW
      : Codec[interactive_submission_service.SignatureFormat.Unrecognized] =
    deriveCodec

  implicit val signatureFormatRecognizedRW
      : Codec[interactive_submission_service.SignatureFormat.Recognized] =
    deriveCodec

  implicit val jsExecuteSubmissionRequestRW: Codec[JsExecuteSubmissionRequest] =
    deriveCodec

  // Schema mappings are added to align generated tapir docs with a circe mapping of ADTs
  // I cannot force oneOfWrapped schema on those 2 types - strangely they are no different to for instance HashingSchemeVersion which works fine
//  implicit val signatureFormatSchema: Schema[interactive_submission_service.SignatureFormat] =
//    Schema.oneOfWrapped

//  implicit val signingAlgorithmSpec: Schema[interactive_submission_service.SigningAlgorithmSpec] =
//    Schema.oneOfWrapped

  implicit val timeSchema: Schema[interactive_submission_service.MinLedgerTime.Time] =
    Schema.oneOfWrapped

  implicit val esrDeduplicationPeriodSchema
      : Schema[interactive_submission_service.ExecuteSubmissionRequest.DeduplicationPeriod] =
    Schema.oneOfWrapped

  implicit val hashingSchemeVersionSchema
      : Schema[interactive_submission_service.HashingSchemeVersion] =
    Schema.oneOfWrapped
}
