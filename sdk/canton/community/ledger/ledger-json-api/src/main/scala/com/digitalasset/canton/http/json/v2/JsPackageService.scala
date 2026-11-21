// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.ledger.api.v2.admin.package_management_service
import com.daml.ledger.api.v2.admin.package_management_service.UploadDarFileResponse
import com.daml.ledger.api.v2.{package_reference, package_service}
import com.digitalasset.canton.auth.AuthInterceptor
import com.digitalasset.canton.http.json.v2.CirceRelaxedCodec.deriveRelaxedCodec
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput}
import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json.v2.JsSchema.{
  JsCantonError,
  stringDecoderForEnum,
  stringEncoderForEnum,
  stringSchemaForEnum,
}
import com.digitalasset.canton.ledger.client.services.admin.PackageManagementClient
import com.digitalasset.canton.ledger.client.services.pkg.PackageClient
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.audit.ApiRequestLogger
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import io.circe.{Codec, Decoder, Encoder}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Source, StreamConverters}
import org.apache.pekko.util
import sttp.capabilities.pekko.PekkoStreams
import sttp.model.StatusCode
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.jsonBody
import sttp.tapir.{
  AnyEndpoint,
  CodecFormat,
  Endpoint,
  Schema,
  SchemaType,
  path,
  query,
  streamBinaryBody,
}

import scala.annotation.unused
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.IteratorHasAsScala

import JsPackageCodecs.*

class JsPackageService(
    packageClient: PackageClient,
    packageManagementClient: PackageManagementClient,
    override protected val requestLogger: ApiRequestLogger,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext,
    materializer: Materializer,
    val authInterceptor: AuthInterceptor,
) extends Endpoints {
  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  def endpoints() =
    List(
      // TODO(#27556): Extract validateDar and uploadDar in a separate service (JsDarService)
      withServerLogic(
        JsPackageService.validateDar,
        validateDar,
      ),
      withServerLogic(
        JsPackageService.uploadDar,
        upload,
      ),
      withServerLogic(
        JsPackageService.uploadDarOld,
        upload,
      ),
      withServerLogic(
        JsPackageService.listPackagesEndpoint,
        list,
      ),
      withServerLogic(
        JsPackageService.downloadPackageEndpoint,
        getPackage,
      ),
      withServerLogic(
        JsPackageService.packageStatusEndpoint,
        status,
      ),
      withServerLogic(
        JsPackageService.listVettedPackagesEndpoint,
        listVettedPackages,
      ),
      withServerLogic(
        JsPackageService.updateVettedPackagesEndpoint,
        updateVettedPackages,
      ),
    )
  private def list(
      caller: CallerContext
  ): TracedInput[Unit] => Future[Either[JsCantonError, package_service.ListPackagesResponse]] = {
    _ =>
      packageClient.listPackages(caller.token())(caller.traceContext()).resultToRight
  }

  private def status(
      @unused caller: CallerContext
  ): TracedInput[String] => Future[
    Either[JsCantonError, package_service.GetPackageStatusResponse]
  ] = req =>
    packageClient.getPackageStatus(req.in, caller.token())(caller.traceContext()).resultToRight

  private def listVettedPackages(
      @unused caller: CallerContext
  ): TracedInput[package_service.ListVettedPackagesRequest] => Future[
    Either[JsCantonError, package_service.ListVettedPackagesResponse]
  ] = req =>
    packageClient.listVettedPackages(req.in, caller.token())(caller.traceContext()).resultToRight

  private def updateVettedPackages(
      @unused caller: CallerContext
  ): TracedInput[package_management_service.UpdateVettedPackagesRequest] => Future[
    Either[JsCantonError, package_management_service.UpdateVettedPackagesResponse]
  ] = req =>
    packageManagementClient
      .updateVettedPackages(req.in, caller.token())(caller.traceContext())
      .resultToRight

  private def validateDar(caller: CallerContext) = {
    (tracedInput: TracedInput[(Source[util.ByteString, Any], Option[String])]) =>
      implicit val traceContext: TraceContext = caller.traceContext()
      val (bytesSource, synchronizerIdO) = tracedInput.in
      val inputStream = bytesSource.runWith(StreamConverters.asInputStream())(materializer)
      val bs = protobuf.ByteString.readFrom(inputStream)
      packageManagementClient
        .validateDarFile(
          darFile = bs,
          token = caller.token(),
          synchronizerId = synchronizerIdO,
        )
        .resultToRight
  }

  private def upload(caller: CallerContext) = {
    (tracedInput: TracedInput[(Source[util.ByteString, Any], Option[Boolean], Option[String])]) =>
      implicit val traceContext: TraceContext = caller.traceContext()
      val (bytesSource, vetAllPackagesO, synchronizerIdO) = tracedInput.in
      val inputStream = bytesSource.runWith(StreamConverters.asInputStream())(materializer)
      val bs = protobuf.ByteString.readFrom(inputStream)
      packageManagementClient
        .uploadDarFile(
          darFile = bs,
          token = caller.token(),
          vetAllPackages = vetAllPackagesO.getOrElse(true),
          synchronizerId = synchronizerIdO,
        )
        .map { _ =>
          package_management_service.UploadDarFileResponse()
        }
        .resultToRight
  }

  private def getPackage(caller: CallerContext) = { (tracedInput: TracedInput[String]) =>
    packageClient
      .getPackage(tracedInput.in, caller.token())(caller.traceContext())
      .map(response =>
        (
          Source.fromIterator(() =>
            response.archivePayload
              .asReadOnlyByteBufferList()
              .iterator
              .asScala
              .map(org.apache.pekko.util.ByteString(_))
          ),
          response.hash,
        )
      )
      .resultToRight
  }
}

object JsPackageService extends DocumentationEndpoints {
  import Endpoints.*
  lazy val packages = v2Endpoint.in(sttp.tapir.stringToPath("packages"))
  lazy val packageVetting = v2Endpoint.in(sttp.tapir.stringToPath("package-vetting"))
  lazy val dars = v2Endpoint.in(sttp.tapir.stringToPath("dars"))
  private val packageIdPath = "package-id"

  val validateDar =
    dars.post
      .in(streamBinaryBody(PekkoStreams)(CodecFormat.OctetStream()).toEndpointIO)
      .in(sttp.tapir.stringToPath("validate"))
      .in(query[Option[String]]("synchronizerId"))
      .protoRef(package_management_service.PackageManagementServiceGrpc.METHOD_VALIDATE_DAR_FILE)

  val uploadDar = uploadDarEndpoint(dars).description("Upload a DAR to the participant node")

  private val uploadDarOld =
    uploadDarEndpoint(packages)
      .description(s"""
                 |Behaves the same as /dars. This endpoint will be deprecated and removed in a future release.
                 |${createProtoRef(
          package_management_service.PackageManagementServiceGrpc.METHOD_UPLOAD_DAR_FILE
        )}
                """.stripMargin.trim)

  private def uploadDarEndpoint(
      endpointDef: Endpoint[CallerContext, Unit, (StatusCode, JsCantonError), Unit, Any]
  ) =
    endpointDef.post
      .in(streamBinaryBody(PekkoStreams)(CodecFormat.OctetStream()).toEndpointIO)
      .in(query[Option[Boolean]]("vetAllPackages"))
      .in(query[Option[String]]("synchronizerId"))
      .out(jsonBody[UploadDarFileResponse])
      .protoRef(package_management_service.PackageManagementServiceGrpc.METHOD_UPLOAD_DAR_FILE)

  val listPackagesEndpoint =
    packages.get
      .out(jsonBody[package_service.ListPackagesResponse])
      .protoRef(package_service.PackageServiceGrpc.METHOD_LIST_PACKAGES)

  val downloadPackageEndpoint =
    packages.get
      .in(path[String](packageIdPath))
      .out(streamBinaryBody(PekkoStreams)(CodecFormat.OctetStream()))
      .out(
        sttp.tapir.header[String]("Canton-Package-Hash")
      ) // Non standard header used for hash output
      .protoRef(package_service.PackageServiceGrpc.METHOD_GET_PACKAGE)

  val packageStatusEndpoint =
    packages.get
      .in(path[String](packageIdPath))
      .in(sttp.tapir.stringToPath("status"))
      .out(jsonBody[package_service.GetPackageStatusResponse])
      .protoRef(package_service.PackageServiceGrpc.METHOD_GET_PACKAGE_STATUS)

  val listVettedPackagesEndpoint =
    packageVetting.get
      .in(jsonBody[package_service.ListVettedPackagesRequest])
      .out(jsonBody[package_service.ListVettedPackagesResponse])
      .protoRef(
        package_service.PackageServiceGrpc.METHOD_LIST_VETTED_PACKAGES
      )

  val updateVettedPackagesEndpoint =
    packageVetting.post
      .in(jsonBody[package_management_service.UpdateVettedPackagesRequest])
      .out(jsonBody[package_management_service.UpdateVettedPackagesResponse])
      .protoRef(
        package_management_service.PackageManagementServiceGrpc.METHOD_UPDATE_VETTED_PACKAGES
      )

  override def documentation: Seq[AnyEndpoint] =
    Seq(
      validateDar,
      uploadDar,
      uploadDarOld,
      listPackagesEndpoint,
      downloadPackageEndpoint,
      packageStatusEndpoint,
      listVettedPackagesEndpoint,
      updateVettedPackagesEndpoint,
    )

}

object JsPackageCodecs {
  import JsSchema.config

  implicit val listPackagesResponse: Codec[package_service.ListPackagesResponse] =
    deriveRelaxedCodec
  implicit val getPackageStatusResponse: Codec[package_service.GetPackageStatusResponse] =
    deriveRelaxedCodec
  implicit val vettedPackages: Codec[package_reference.VettedPackages] =
    deriveRelaxedCodec
  implicit val vettedPackage: Codec[package_reference.VettedPackage] =
    deriveRelaxedCodec
  implicit val updateVettedPackagesResponse
      : Codec[package_management_service.UpdateVettedPackagesResponse] =
    deriveRelaxedCodec
  implicit val vettedPackagesChangeRef: Codec[package_management_service.VettedPackagesRef] =
    deriveRelaxedCodec
  implicit val vettedPackagesChangeUnvet
      : Codec[package_management_service.VettedPackagesChange.Unvet] =
    deriveRelaxedCodec
  implicit val vettedPackagesChangeVet: Codec[package_management_service.VettedPackagesChange.Vet] =
    deriveRelaxedCodec
  implicit val vettedPackagesChangeOperation
      : Codec[package_management_service.VettedPackagesChange.Operation] =
    deriveConfiguredCodec
  implicit val vettedPackagesChangeOperationSchema
      : Schema[package_management_service.VettedPackagesChange.Operation] =
    Schema.oneOfWrapped

  implicit val topologySerial: Codec[package_reference.PriorTopologySerial] =
    deriveRelaxedCodec

  implicit val topologySerialSerial: Codec[package_reference.PriorTopologySerial.Serial] =
    deriveConfiguredCodec

  implicit val topologySerialSerialPriorOneOf
      : Codec[package_reference.PriorTopologySerial.Serial.Prior] =
    deriveRelaxedCodec

  implicit val topologySerialSerialNoPriorOneOf
      : Codec[package_reference.PriorTopologySerial.Serial.NoPrior] =
    Codec.from(
      Decoder.decodeUnit.map(_ =>
        package_reference.PriorTopologySerial.Serial.NoPrior(com.google.protobuf.empty.Empty())
      ),
      Encoder.encodeUnit.contramap[package_reference.PriorTopologySerial.Serial.NoPrior](_ => ()),
    )

  implicit val updateVettedPackagesForceFlagEncoder
      : Encoder[package_management_service.UpdateVettedPackagesForceFlag] =
    stringEncoderForEnum()
  implicit val updateVettedPackagesForceFlagDecoder
      : Decoder[package_management_service.UpdateVettedPackagesForceFlag] =
    stringDecoderForEnum()

  implicit val vettedPackagesChange: Codec[package_management_service.VettedPackagesChange] =
    deriveRelaxedCodec
  implicit val updateVettedPackagesRequest
      : Codec[package_management_service.UpdateVettedPackagesRequest] =
    deriveRelaxedCodec
  implicit val packageMetadataFilter: Codec[package_service.PackageMetadataFilter] =
    deriveRelaxedCodec
  implicit val topologyStateFilter: Codec[package_service.TopologyStateFilter] =
    deriveRelaxedCodec
  implicit val listVettedPackagesRequest: Codec[package_service.ListVettedPackagesRequest] =
    deriveRelaxedCodec
  implicit val listVettedPackagesResponse: Codec[package_service.ListVettedPackagesResponse] =
    deriveRelaxedCodec

  implicit val vettedPackagesChangeUnvetOneOf
      : Codec[package_management_service.VettedPackagesChange.Operation.Unvet] =
    deriveRelaxedCodec
  implicit val vettedPackagesChangeVetOneOf
      : Codec[package_management_service.VettedPackagesChange.Operation.Vet] =
    deriveRelaxedCodec

  implicit val uploadDarFileResponseRW: Codec[package_management_service.UploadDarFileResponse] =
    deriveRelaxedCodec
  implicit val packageStatusEncoder: Encoder[package_service.PackageStatus] =
    stringEncoderForEnum()
  implicit val packageStatusDecoder: Decoder[package_service.PackageStatus] =
    stringDecoderForEnum()

  // Schema mappings are added to align generated tapir docs with a circe mapping of ADTs
  implicit val packageStatusRecognizedSchema: Schema[package_service.PackageStatus.Recognized] =
    Schema.oneOfWrapped

  implicit val packageStatusSchema: Schema[package_service.PackageStatus] = stringSchemaForEnum()

  // Schema mappings are added to align generated tapir docs with a circe mapping of ADTs
  implicit val updateVettedPackagesForceFlagRecognizedSchema
      : Schema[package_management_service.UpdateVettedPackagesForceFlag.Recognized] =
    Schema.oneOfWrapped
  implicit val updateVettedPackagesForceFlagSchema
      : Schema[package_management_service.UpdateVettedPackagesForceFlag] =
    stringSchemaForEnum()

  implicit val topologySerialSerialNoPriorSchema
      : Schema[package_reference.PriorTopologySerial.Serial.NoPrior] =
    Schema(
      schemaType =
        SchemaType.SProduct[package_reference.PriorTopologySerial.Serial.NoPrior](List.empty),
      name = Some(Schema.SName("NoPrior")),
    )

  implicit val topologySerialSerialSchema: Schema[package_reference.PriorTopologySerial.Serial] =
    Schema.oneOfWrapped

}
