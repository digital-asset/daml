// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver.services

import com.daml.ledger.participant.state.index.v2.IndexPackagesService
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml_lf_dev.DamlLf.{Archive, HashFunction}
import com.digitalasset.dec.{DirectExecutionContext => DEC}
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.v1.package_service.HashFunction.{
  SHA256 => APISHA256,
  Unrecognized => APIUnrecognized
}
import com.digitalasset.ledger.api.v1.package_service.PackageServiceGrpc.PackageService
import com.digitalasset.ledger.api.v1.package_service.{HashFunction => APIHashFunction, _}
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.validation.PackageServiceValidation
import io.grpc.{BindableService, ServerServiceDefinition, Status}

import scala.concurrent.Future

final class ApiPackageService private (backend: IndexPackagesService)(
    implicit logCtx: LoggingContext)
    extends PackageService
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(this.getClass)

  override def bindService(): ServerServiceDefinition =
    PackageServiceGrpc.bindService(this, DEC)

  override def close(): Unit = ()

  override def listPackages(request: ListPackagesRequest): Future[ListPackagesResponse] =
    backend
      .listLfPackages()
      .map(p => ListPackagesResponse(p.keys.toSeq))(DEC)
      .andThen(logger.logErrorsOnCall[ListPackagesResponse])(DEC)

  override def getPackage(request: GetPackageRequest): Future[GetPackageResponse] =
    withValidatedPackageId(
      request.packageId,
      pId =>
        backend
          .getLfArchive(pId)
          .flatMap(_.fold(Future.failed[GetPackageResponse](Status.NOT_FOUND.asRuntimeException()))(
            archive => Future.successful(toGetPackageResponse(archive))))(DEC)
          .andThen(logger.logErrorsOnCall[GetPackageResponse])(DEC)
    )

  override def getPackageStatus(
      request: GetPackageStatusRequest): Future[GetPackageStatusResponse] =
    withValidatedPackageId(
      request.packageId,
      pId =>
        backend
          .listLfPackages()
          .map { packages =>
            val result = if (packages.contains(pId)) {
              PackageStatus.REGISTERED
            } else {
              PackageStatus.UNKNOWN
            }
            GetPackageStatusResponse(result)
          }(DEC)
          .andThen(logger.logErrorsOnCall[GetPackageStatusResponse])(DEC)
    )

  private def withValidatedPackageId[T](packageId: String, block: Ref.PackageId => Future[T]) =
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
  def create(ledgerId: LedgerId, backend: IndexPackagesService)(
      implicit logCtx: LoggingContext): PackageService with GrpcApiService =
    new PackageServiceValidation(new ApiPackageService(backend), ledgerId) with BindableService {
      override def bindService(): ServerServiceDefinition =
        PackageServiceGrpc.bindService(this, DEC)
    }
}
