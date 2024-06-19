// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json2

import com.digitalasset.canton.http.json2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.ledger.client.services.admin.PackageManagementClient
import com.digitalasset.canton.ledger.client.services.pkg.PackageClient
import com.daml.ledger.api.v2.package_service
import com.google.protobuf
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Source, StreamConverters}
import org.apache.pekko.util
import sttp.tapir.path

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.IteratorHasAsScala
import JsPackageCodecs.*
import com.digitalasset.canton.http.json2.JsSchema.JsCantonError
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.generic.auto.*

class JsPackageService(
    packageClient: PackageClient,
    packageManagementClient: PackageManagementClient,
)(implicit val executionContext: ExecutionContext, materializer: Materializer)
    extends Endpoints {

  private val packages = baseEndpoint.in("packages")
  private val packageIdPath = "package-id"
  def endpoints() =
    List(
      json(
        packages.get
          .description("List all packages uploaded on the participant node"),
        list,
      ),
      downloadByteString(
        packages
          .in(path[String](packageIdPath))
          .description("Download the package for the requested package-id"),
        getPackage,
      ),
      uploadByteString(
        packages
          .description("Upload a DAR to the participant node"),
        upload,
      ),
      json(
        packages.get
          .in(path[String](packageIdPath))
          .in("status")
          .description("Get package status"),
        status,
      ),
    )
  private def list(
      caller: CallerContext
  ): TracedInput[Unit] => Future[Either[JsCantonError, package_service.ListPackagesResponse]] = {
    req =>
      packageClient.listPackages(caller.token())(req.traceContext).toRight
  }

  private def status(
      caller: CallerContext
  ): TracedInput[String] => Future[
    Either[JsCantonError, package_service.GetPackageStatusResponse]
  ] = req => packageClient.getPackageStatus(req.in)(req.traceContext).toRight

  private def upload(caller: CallerContext) = {
    (tracedInput: TracedInput[Source[util.ByteString, Any]]) =>
      {
        implicit val traceContext = tracedInput.traceContext
        val inputStream = tracedInput.in.runWith(StreamConverters.asInputStream())(materializer)
        val bs = protobuf.ByteString.readFrom(inputStream)
        packageManagementClient.uploadDarFile(bs, caller.jwt.map(_.token))
      }
  }

  private def getPackage(caller: CallerContext) = { (tracedInput: TracedInput[String]) =>
    packageClient
      .getPackage(tracedInput.in, caller.jwt.map(_.token))(tracedInput.traceContext)
      .map(response =>
        Source.fromIterator(() =>
          response.archivePayload
            .asReadOnlyByteBufferList()
            .iterator
            .asScala
            .map(org.apache.pekko.util.ByteString(_))
        )
      )
  }
}

object JsPackageCodecs {
  implicit val listPackagesResponse: Codec[package_service.ListPackagesResponse] = deriveCodec
  implicit val getPackageStatusResponse: Codec[package_service.GetPackageStatusResponse] =
    deriveCodec
  implicit val packageStatus: Codec[package_service.PackageStatus] = deriveCodec
}
