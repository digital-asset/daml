// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.endpoints

import akka.http.scaladsl.model.*
import akka.NotUsed
import com.digitalasset.canton.http.Endpoints.ET
import com.digitalasset.canton.http.util.FutureUtil.{eitherT, rightT}
import com.digitalasset.canton.http.util.Logging.{InstanceUUID, RequestID}
import com.digitalasset.canton.http.util.{ProtobufByteStrings, toLedgerId}
import com.daml.jwt.domain.Jwt
import scalaz.EitherT
import scalaz.std.scalaFuture.*

import scala.concurrent.{ExecutionContext, Future}
import com.digitalasset.canton.ledger.api.domain as LedgerApiDomain
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
      t2 <- eitherT(routeSetup.inputSource(httpRequest))
      (jwt, payload, source) = t2
      _ <- EitherT.pure(parseAndDecodeTimer.stop())

      _ <- eitherT(
        handleFutureFailure(
          packageManagementService.uploadDarFile(
            jwt,
            toLedgerId(payload.ledgerId),
            source.mapMaterializedValue(_ => NotUsed),
          )
        )
      ): ET[Unit]
    } yield domain.OkResponse(())
  }

  def listPackages(jwt: Jwt, ledgerId: LedgerApiDomain.LedgerId)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[Seq[String]]] =
    rightT(packageManagementService.listPackages(jwt, ledgerId)).map(domain.OkResponse(_))

  def downloadPackage(jwt: Jwt, ledgerId: LedgerApiDomain.LedgerId, packageId: String)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[HttpResponse] = {
    val pkgResp: Future[admin.GetPackageResponse] =
      packageManagementService.getPackage(jwt, ledgerId, packageId)
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
