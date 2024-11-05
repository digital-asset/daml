// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.digitalasset.canton.ledger.client.services.admin.PackageManagementClient
import com.digitalasset.canton.ledger.client.services.pkg.PackageClient
import com.daml.ledger.api.v2.package_service
import com.daml.ledger.api.v2.admin.package_management_service
import com.google.protobuf
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Source, StreamConverters}
import org.apache.pekko.util
import sttp.tapir.{CodecFormat, path, streamBinaryBody}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.IteratorHasAsScala
import JsPackageCodecs.*
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput}
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.TraceContext
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.capabilities.pekko.PekkoStreams
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.jsonBody

class JsPackageService(
    packageClient: PackageClient,
    packageManagementClient: PackageManagementClient,
    val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext, materializer: Materializer)
    extends Endpoints {
  import JsPackageService.*

  def endpoints() =
    List(
      withServerLogic(
        JsPackageService.listPackagesEndpoint,
        list,
      ),
      withServerLogic(
        JsPackageService.downloadPackageEndpoint,
        getPackage,
      ),
      withServerLogic(
        uploadDar,
        upload,
      ),
      withServerLogic(
        JsPackageService.packageStatusEndpoint,
        status,
      ),
    )
  private def list(
      caller: CallerContext
  ): TracedInput[Unit] => Future[Either[JsCantonError, package_service.ListPackagesResponse]] = {
    req =>
      packageClient.listPackages(caller.token())(req.traceContext).resultToRight
  }

  private def status(
      caller: CallerContext
  ): TracedInput[String] => Future[
    Either[JsCantonError, package_service.GetPackageStatusResponse]
  ] = req => packageClient.getPackageStatus(req.in)(req.traceContext).resultToRight

  private def upload(caller: CallerContext) = {
    (tracedInput: TracedInput[Source[util.ByteString, Any]]) =>
      implicit val traceContext: TraceContext = tracedInput.traceContext
      val inputStream = tracedInput.in.runWith(StreamConverters.asInputStream())(materializer)
      val bs = protobuf.ByteString.readFrom(inputStream)
      packageManagementClient
        .uploadDarFile(bs, caller.jwt.map(_.token))
        .map { _ =>
          package_management_service.UploadDarFileResponse()
        }
        .resultToRight

  }

  private def getPackage(caller: CallerContext) = { (tracedInput: TracedInput[String]) =>
    packageClient
      .getPackage(tracedInput.in, caller.jwt.map(_.token))(tracedInput.traceContext)
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

object JsPackageService {
  import Endpoints.*
  lazy val packages = v2Endpoint.in(sttp.tapir.stringToPath("packages"))
  private val packageIdPath = "package-id"

  val uploadDar =
    packages.post
      .in(streamBinaryBody(PekkoStreams)(CodecFormat.OctetStream()).toEndpointIO)
      .out(jsonBody[package_management_service.UploadDarFileResponse])
      .description("Upload a DAR to the participant node")

  val listPackagesEndpoint =
    packages.get
      .out(jsonBody[package_service.ListPackagesResponse])
      .description("List all packages uploaded on the participant node")

  val downloadPackageEndpoint =
    packages.get
      .in(path[String](packageIdPath))
      .out(streamBinaryBody(PekkoStreams)(CodecFormat.OctetStream()))
      .out(
        sttp.tapir.header[String]("Canton-Package-Hash")
      ) // Non standard header used for hash output
      .description("Download the package for the requested package-id")

  val packageStatusEndpoint =
    packages.get
      .in(path[String](packageIdPath))
      .in(sttp.tapir.stringToPath("status"))
      .out(jsonBody[package_service.GetPackageStatusResponse])
      .description("Get package status")

}

object JsPackageCodecs {
  implicit val listPackagesResponse: Codec[package_service.ListPackagesResponse] = deriveCodec
  implicit val getPackageStatusResponse: Codec[package_service.GetPackageStatusResponse] =
    deriveCodec
  implicit val uploadDarFileResponseRW: Codec[package_management_service.UploadDarFileResponse] =
    deriveCodec
  implicit val packageStatus: Codec[package_service.PackageStatus] = deriveCodec
}
