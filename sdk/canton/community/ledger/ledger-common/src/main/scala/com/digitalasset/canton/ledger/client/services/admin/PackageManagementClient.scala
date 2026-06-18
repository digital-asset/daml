// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.admin

import com.daml.ledger.api.v2.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementServiceStub
import com.daml.ledger.api.v2.admin.package_management_service.{
  ListKnownPackagesRequest,
  PackageDetails,
  UpdateVettedPackagesRequest,
  UpdateVettedPackagesResponse,
  UploadDarFileRequest,
  ValidateDarFileRequest,
}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

object PackageManagementClient {

  private val listKnownPackagesRequest = ListKnownPackagesRequest()

}

final class PackageManagementClient(
    service: PackageManagementServiceStub,
    getDefaultToken: () => Option[String] = () => None,
)(implicit
    ec: ExecutionContext
) {

  def listKnownPackages(
      token: Option[String] = None
  )(implicit traceContext: TraceContext): Future[Seq[PackageDetails]] =
    LedgerClient
      .stubWithTracing(service, token.orElse(getDefaultToken()))
      .listKnownPackages(PackageManagementClient.listKnownPackagesRequest)
      .map(_.packageDetails)

  def uploadDarFile(
      darFile: ByteString,
      token: Option[String] = None,
      vetAllPackages: Boolean = true,
      synchronizerId: Option[String] = None,
  )(implicit traceContext: TraceContext): Future[Unit] =
    LedgerClient
      .stubWithTracing(service, token.orElse(getDefaultToken()))
      .uploadDarFile(
        UploadDarFileRequest(
          darFile = darFile,
          submissionId = "",
          vettingChange =
            if (vetAllPackages)
              UploadDarFileRequest.VettingChange.VETTING_CHANGE_VET_ALL_PACKAGES
            else
              UploadDarFileRequest.VettingChange.VETTING_CHANGE_DONT_VET_ANY_PACKAGES,
          synchronizerId = synchronizerId.getOrElse(""),
        )
      )
      .map(_ => ())

  def validateDarFile(
      darFile: ByteString,
      token: Option[String] = None,
      synchronizerId: Option[String] = None,
  )(implicit traceContext: TraceContext): Future[Unit] =
    LedgerClient
      .stubWithTracing(service, token.orElse(getDefaultToken()))
      .validateDarFile(
        ValidateDarFileRequest(
          darFile = darFile,
          submissionId = "",
          synchronizerId = synchronizerId.getOrElse(""),
        )
      )
      .map(_ => ())

  def updateVettedPackages(
      request: UpdateVettedPackagesRequest,
      token: Option[String] = None,
  )(implicit traceContext: TraceContext): Future[UpdateVettedPackagesResponse] =
    LedgerClient
      .stubWithTracing(service, token)
      .updateVettedPackages(request)
}
