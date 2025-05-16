// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.ledger.api.v2.interactive.interactive_submission_service
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  GetPreferredPackageVersionRequest,
  InteractiveSubmissionServiceGrpc,
  MinLedgerTime,
}
import com.daml.ledger.api.v2.package_reference
import com.digitalasset.canton.http.json.v2.CirceRelaxedCodec.deriveRelaxedCodec
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput, v2Endpoint}
import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json.v2.JsSchema.{
  JsCantonError,
  stringDecoderForEnum,
  stringEncoderForEnum,
}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf
import io.circe.*
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.{AnyEndpoint, Schema, stringToPath}

import java.time.Instant
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
    withServerLogic(
      JsInteractiveSubmissionService.preferredPackageVersionEndpoint,
      preferredPackageVersion,
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

  private def preferredPackageVersion(
      callerContext: CallerContext
  ): TracedInput[(List[String], String, Option[Instant], Option[String])] => Future[
    Either[JsCantonError, interactive_submission_service.GetPreferredPackageVersionResponse]
  ] = { (tracedInput: TracedInput[(List[String], String, Option[Instant], Option[String])]) =>
    implicit val token: Option[String] = callerContext.token()
    implicit val tc: TraceContext = tracedInput.traceContext
    val (parties, packageName, vettingValidAt, synchronizerId) = tracedInput.in
    interactiveSubmissionServiceClient(token)
      .getPreferredPackageVersion(
        GetPreferredPackageVersionRequest(
          parties = parties,
          packageName = packageName,
          vettingValidAt = vettingValidAt.map(ProtoConverter.InstantConverter.toProtoPrimitive),
          synchronizerId = synchronizerId.getOrElse(""),
        )
      )
      .resultToRight
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
    minLedgerTime: Option[MinLedgerTime] = None,
)

object JsInteractiveSubmissionService extends DocumentationEndpoints {
  import JsInteractiveSubmissionServiceCodecs.*
  private lazy val interactiveSubmission =
    v2Endpoint.in(sttp.tapir.stringToPath("interactive-submission"))

  private lazy val preferredPackageVersion =
    interactiveSubmission.in(sttp.tapir.stringToPath("preferred-package-version"))

  private val partiesQueryParam = "parties"
  private val packageNameQueryParam = "package-name"
  private val timestampVettingValidityQueryParam = "vetting_valid_at"
  private val synchronizerIdQueryParam = "synchronizer-id"

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

  val preferredPackageVersionEndpoint =
    preferredPackageVersion.get
      .in(sttp.tapir.query[List[String]](partiesQueryParam))
      .in(sttp.tapir.query[String](packageNameQueryParam))
      .in(sttp.tapir.query[Option[Instant]](timestampVettingValidityQueryParam))
      .in(sttp.tapir.query[Option[String]](synchronizerIdQueryParam))
      .out(jsonBody[interactive_submission_service.GetPreferredPackageVersionResponse])
      .description(
        "Get the preferred package version for constructing a command submission"
      )

  override def documentation: Seq[AnyEndpoint] =
    Seq(prepareEndpoint, executeEndpoint, preferredPackageVersionEndpoint)
}

object JsInteractiveSubmissionServiceCodecs {
  import JsCommandServiceCodecs.*
  import JsSchema.config

  implicit val timeRW: Codec[interactive_submission_service.MinLedgerTime.Time] =
    deriveConfiguredCodec // ADT
  implicit val timeMinLedgerTimeRelRW
      : Codec[interactive_submission_service.MinLedgerTime.Time.MinLedgerTimeRel] =
    deriveRelaxedCodec
  implicit val timeMinLedgerTimeAbsRW
      : Codec[interactive_submission_service.MinLedgerTime.Time.MinLedgerTimeAbs] =
    deriveRelaxedCodec
  implicit val minLedgerTimeRW: Codec[interactive_submission_service.MinLedgerTime] =
    deriveRelaxedCodec

  implicit val jsPrepareSubmissionRequestRW: Codec[JsPrepareSubmissionRequest] =
    deriveConfiguredCodec

  implicit val prepareSubmissionResponseRW: Codec[JsPrepareSubmissionResponse] =
    deriveConfiguredCodec

  implicit val hashingSchemeVersionEncoder
      : Encoder[interactive_submission_service.HashingSchemeVersion] =
    stringEncoderForEnum()

  implicit val hashingSchemeVersionDecoder
      : Decoder[interactive_submission_service.HashingSchemeVersion] =
    stringDecoderForEnum()

  implicit val executeSubmissionResponseRW
      : Codec[interactive_submission_service.ExecuteSubmissionResponse] =
    deriveRelaxedCodec

  implicit val esrDeduplicationDurationRW: Codec[
    interactive_submission_service.ExecuteSubmissionRequest.DeduplicationPeriod.DeduplicationDuration
  ] =
    deriveRelaxedCodec

  implicit val esrDeduplicationOffsetRW: Codec[
    interactive_submission_service.ExecuteSubmissionRequest.DeduplicationPeriod.DeduplicationOffset
  ] =
    deriveRelaxedCodec

  implicit val esrDeduplicationPeriodRW
      : Codec[interactive_submission_service.ExecuteSubmissionRequest.DeduplicationPeriod] =
    deriveConfiguredCodec // ADT

  implicit val partySignaturesRW: Codec[interactive_submission_service.PartySignatures] =
    deriveRelaxedCodec

  implicit val singlePartySignaturesRW
      : Codec[interactive_submission_service.SinglePartySignatures] =
    deriveRelaxedCodec

  implicit val signatureRW: Codec[interactive_submission_service.Signature] =
    deriveRelaxedCodec

  implicit val signingAlgorithmSpecEncoder
      : Encoder[interactive_submission_service.SigningAlgorithmSpec] =
    stringEncoderForEnum()
  implicit val signingAlgorithmSpecDecoder
      : Decoder[interactive_submission_service.SigningAlgorithmSpec] =
    stringDecoderForEnum()

  implicit val signatureFormatDecoder: Decoder[interactive_submission_service.SignatureFormat] =
    stringDecoderForEnum()
  implicit val signatureFormatEncoder: Encoder[interactive_submission_service.SignatureFormat] =
    stringEncoderForEnum()

  implicit val jsExecuteSubmissionRequestRW: Codec[JsExecuteSubmissionRequest] =
    deriveConfiguredCodec

  implicit val packageReference: Codec[package_reference.PackageReference] =
    deriveCodec
  implicit val packagePreference: Codec[interactive_submission_service.PackagePreference] =
    deriveRelaxedCodec
  implicit val getPreferredPackageVersionResponse
      : Codec[interactive_submission_service.GetPreferredPackageVersionResponse] =
    deriveRelaxedCodec

  // Schema mappings are added to align generated tapir docs with a circe mapping of ADTs
  implicit val signatureFormatSchema: Schema[interactive_submission_service.SignatureFormat] =
    Schema.string

  implicit val signingAlgorithmSpec: Schema[interactive_submission_service.SigningAlgorithmSpec] =
    Schema.string

  implicit val timeSchema: Schema[interactive_submission_service.MinLedgerTime.Time] =
    Schema.oneOfWrapped

  implicit val esrDeduplicationPeriodSchema
      : Schema[interactive_submission_service.ExecuteSubmissionRequest.DeduplicationPeriod] =
    Schema.oneOfWrapped

  implicit val hashingSchemeVersionSchema
      : Schema[interactive_submission_service.HashingSchemeVersion] =
    Schema.string
}
