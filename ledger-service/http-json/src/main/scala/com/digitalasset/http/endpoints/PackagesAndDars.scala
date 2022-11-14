// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package endpoints

import akka.http.scaladsl.model._
import Endpoints.ET
import util.FutureUtil.rightT
import util.Logging.{InstanceUUID, RequestID}
import util.ProtobufByteStrings
import com.daml.jwt.domain.Jwt
import scalaz.std.scalaFuture._

import scala.concurrent.{ExecutionContext, Future}
import com.daml.ledger.api.{domain => LedgerApiDomain}
import com.daml.logging.LoggingContextOf

class PackagesAndDars(packageManagementService: PackageManagementService)(implicit
    ec: ExecutionContext
) {
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
