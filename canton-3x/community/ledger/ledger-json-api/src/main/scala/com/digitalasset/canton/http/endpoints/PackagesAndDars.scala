// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.endpoints

import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.NotUsed
import com.digitalasset.canton.http.Endpoints.ET
import com.digitalasset.canton.http.util.FutureUtil.{either, eitherT, rightT}
import com.digitalasset.canton.http.util.Logging.{InstanceUUID, RequestID}
import com.digitalasset.canton.http.util.ProtobufByteStrings
import com.daml.jwt.domain.Jwt
import scalaz.EitherT
import scalaz.std.scalaFuture.*

import scala.concurrent.{ExecutionContext, Future}
import com.daml.logging.LoggingContextOf
import com.digitalasset.canton.http.admin
import com.digitalasset.canton.http.{PackageManagementService, domain}
import com.digitalasset.canton.http.metrics.HttpApiMetrics

class PackagesAndDars(routeSetup: RouteSetup, packageManagementService: PackageManagementService)(
    implicit ec: ExecutionContext
) {
  import routeSetup.*, RouteSetup.*

  def uploadDarFile(httpRequest: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): ET[domain.SyncResponse[Unit]] = {
    for {
      parseAndDecodeTimer <- getParseAndDecodeTimerCtx()
      t2 <- either(routeSetup.inputSource(httpRequest))
      (jwt, source) = t2
      _ <- EitherT.pure(parseAndDecodeTimer.stop())

      _ <- eitherT(
        handleFutureFailure(
          packageManagementService.uploadDarFile(
            jwt,
            source.mapMaterializedValue(_ => NotUsed),
          )
        )
      ): ET[Unit]
    } yield domain.OkResponse(())
  }

  def listPackages(jwt: Jwt)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[Seq[String]]] =
    rightT(packageManagementService.listPackages(jwt)).map(domain.OkResponse(_))

  def downloadPackage(jwt: Jwt, packageId: String)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[HttpResponse] = {
    val pkgResp: Future[admin.GetPackageResponse] =
      packageManagementService.getPackage(jwt, packageId)
    pkgResp.map { x =>
      HttpResponse(
        entity = HttpEntity.apply(
          ContentTypes.`application/octet-stream`,
          ProtobufByteStrings.toSource(x.archivePayload),
        )
      )
    }
  }
}
