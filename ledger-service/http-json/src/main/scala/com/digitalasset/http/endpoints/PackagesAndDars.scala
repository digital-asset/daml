// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package endpoints

import akka.NotUsed
import akka.http.scaladsl.model._
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import EndpointsCompanion._
import Endpoints.ET
import com.daml.scalautil.Statement.discard
import domain.JwtPayloadLedgerIdOnly
import util.FutureUtil.{either, eitherT}
import util.Logging.{InstanceUUID, RequestID}
import util.{ProtobufByteStrings, toLedgerId}
import com.daml.jwt.domain.Jwt
import scalaz.std.scalaFuture._
import scalaz.{-\/, EitherT, \/, \/-}

import scala.concurrent.{ExecutionContext, Future}
import com.daml.logging.LoggingContextOf
import com.daml.metrics.Metrics

class PackagesAndDars(routeSetup: RouteSetup, packageManagementService: PackageManagementService)(
    implicit
    ec: ExecutionContext,
    mat: Materializer,
) {
  import routeSetup._, RouteSetup._

  def uploadDarFile(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: Metrics,
  ): ET[domain.SyncResponse[Unit]] =
    for {
      parseAndDecodeTimer <- getParseAndDecodeTimerCtx()
      _ <- EitherT.pure(metrics.daml.HttpJsonApi.uploadPackagesThroughput.mark())
      t2 <- inputSource(req)
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

  private[this] def inputSource(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[(Jwt, JwtPayloadLedgerIdOnly, Source[ByteString, Any])] =
    either(findJwt(req))
      .leftMap { e =>
        discard { req.entity.discardBytes(mat) }
        e: Error
      }
      .flatMap(j =>
        withJwtPayload[Source[ByteString, Any], JwtPayloadLedgerIdOnly]((j, req.entity.dataBytes))
          .leftMap(it => it: Error)
      )

  def listPackages(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[domain.SyncResponse[Seq[String]]] =
    proxyWithoutCommand(packageManagementService.listPackages)(req).map(domain.OkResponse(_))

  def downloadPackage(req: HttpRequest, packageId: String)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[HttpResponse] = {
    val et: ET[admin.GetPackageResponse] =
      proxyWithoutCommand((jwt, ledgerId) =>
        packageManagementService.getPackage(jwt, ledgerId, packageId)
      )(req)
    val fa: Future[Error \/ admin.GetPackageResponse] = et.run
    fa.map {
      case -\/(e) =>
        httpResponseError(e)
      case \/-(x) =>
        HttpResponse(
          entity = HttpEntity.apply(
            ContentTypes.`application/octet-stream`,
            ProtobufByteStrings.toSource(x.archivePayload),
          )
        )
    }
  }

}
