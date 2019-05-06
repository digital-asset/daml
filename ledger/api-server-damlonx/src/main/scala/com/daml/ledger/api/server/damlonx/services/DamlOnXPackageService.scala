// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.services

import com.daml.ledger.participant.state.index.v1.IndexService
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml_lf.DamlLf.{Archive, HashFunction}
import com.digitalasset.ledger.api.v1.package_service.HashFunction.{
  SHA256 => APISHA256,
  Unrecognized => APIUnrecognized
}
import com.digitalasset.ledger.api.v1.package_service.PackageServiceGrpc.PackageService
import com.digitalasset.ledger.api.v1.package_service.{
  GetPackageResponse,
  PackageStatus,
  HashFunction => APIHashFunction,
  _
}
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.server.api.validation.PackageServiceValidation
import io.grpc.{BindableService, Status}

import scala.concurrent.{ExecutionContext, Future}

class DamlOnXPackageService private (indexService: IndexService)
    extends PackageService
    with AutoCloseable {

  override def close(): Unit = ()

  implicit val ec: ExecutionContext = DirectExecutionContext // FIXME(JM): what should we use?

  private def toGetPackageResponse(archive: Archive): GetPackageResponse = {
    val hashF: APIHashFunction = archive.getHashFunction match {
      case HashFunction.SHA256 => APISHA256
      case _ => APIUnrecognized(-1)
    }
    GetPackageResponse(hashF, archive.getPayload, archive.getHash)
  }

  override def listPackages(request: ListPackagesRequest): Future[ListPackagesResponse] =
    indexService.listPackages.map { packageIds =>
      ListPackagesResponse(packageIds)
    }

  override def getPackage(request: GetPackageRequest): Future[GetPackageResponse] = {
    indexService
      .getPackage(PackageId.assertFromString(request.packageId))
      .flatMap { optArchive =>
        optArchive.fold(Future.failed[GetPackageResponse](Status.NOT_FOUND.asRuntimeException()))(
          archive => Future.successful(toGetPackageResponse(archive)))
      }
  }

  override def getPackageStatus(
      request: GetPackageStatusRequest): Future[GetPackageStatusResponse] =
    indexService.listPackages
      .map { packageIds =>
        GetPackageStatusResponse {
          if (packageIds.contains(PackageId.assertFromString(request.packageId)))
            PackageStatus.REGISTERED
          else
            PackageStatus.UNKNOWN
        }
      }

}

object DamlOnXPackageService {
  def apply(indexService: IndexService, ledgerId: String)(implicit ec: ExecutionContext)
    : PackageService with BindableService with PackageServiceLogging =
    new PackageServiceValidation(new DamlOnXPackageService(indexService), ledgerId)
    with PackageServiceLogging
}
