// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import com.daml.ledger.participant.state.index.v2.IndexPackagesService
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml_lf.DamlLf.{Archive, HashFunction}
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.v1.package_service.PackageServiceGrpc.PackageService
import com.digitalasset.ledger.api.v1.package_service.{
  GetPackageResponse,
  HashFunction => APIHashFunction,
  _
}
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}
import com.digitalasset.platform.server.api.validation.PackageServiceValidation
import io.grpc.{BindableService, ServerServiceDefinition, Status}
import com.digitalasset.ledger.api.v1.package_service.HashFunction.{
  SHA256 => APISHA256,
  Unrecognized => APIUnrecognized
}

import scala.concurrent.{ExecutionContext, Future}

class ApiPackageService private (backend: IndexPackagesService)
    extends PackageService
    with GrpcApiService {
  override def bindService(): ServerServiceDefinition =
    PackageServiceGrpc.bindService(this, DEC)

  override def close(): Unit = ()

  override def listPackages(request: ListPackagesRequest): Future[ListPackagesResponse] =
    backend.listPackages().map(p => ListPackagesResponse(p.keys.toSeq))(DEC)

  override def getPackage(request: GetPackageRequest): Future[GetPackageResponse] =
    withValidatedPackageId(
      request.packageId,
      pId =>
        backend
          .getPackage(pId)
          .flatMap(_.fold(Future.failed[GetPackageResponse](Status.NOT_FOUND.asRuntimeException()))(
            archive => Future.successful(toGetPackageResponse(archive))))(DEC)
    )

  override def getPackageStatus(
      request: GetPackageStatusRequest): Future[GetPackageStatusResponse] =
    withValidatedPackageId(
      request.packageId,
      pId =>
        backend
          .listPackages()
          .map { packages =>
            val result = if (packages.contains(pId)) {
              PackageStatus.REGISTERED
            } else {
              PackageStatus.UNKNOWN
            }
            GetPackageStatusResponse(result)
          }(DEC)
    )

  private def withValidatedPackageId[T](packageId: String, block: Ref.PackageId.T => Future[T]) =
    Ref.PackageId
      .fromString(packageId)
      .fold(
        error =>
          Future.failed[T](
            Status.INVALID_ARGUMENT
              .withDescription(error)
              .asRuntimeException()),
        pId => block(pId)
      )

  private def toGetPackageResponse(archive: Archive): GetPackageResponse = {
    val hashF: APIHashFunction = archive.getHashFunction match {
      case HashFunction.SHA256 => APISHA256
      case _ => APIUnrecognized(-1)
    }
    GetPackageResponse(hashF, archive.getPayload, archive.getHash)
  }

}

object ApiPackageService {
  def create(ledgerId: LedgerId, backend: IndexPackagesService)(implicit ec: ExecutionContext)
    : PackageService with BindableService with PackageServiceLogging =
    new PackageServiceValidation(new ApiPackageService(backend), ledgerId) with BindableService
    with PackageServiceLogging {
      override def bindService(): ServerServiceDefinition =
        PackageServiceGrpc.bindService(this, DEC)
    }
}
