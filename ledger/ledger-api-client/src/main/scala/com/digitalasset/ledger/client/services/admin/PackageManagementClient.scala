// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.services.admin

import com.digitalasset.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementServiceStub
import com.digitalasset.ledger.api.v1.admin.package_management_service.{
  ListKnownPackagesRequest,
  PackageDetails,
  UploadDarFileRequest
}
import com.digitalasset.ledger.client.LedgerClient
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

object PackageManagementClient {

  private val listKnownPackagesRequest = ListKnownPackagesRequest()

}

final class PackageManagementClient(service: PackageManagementServiceStub)(
    implicit ec: ExecutionContext) {

  def listKnownPackages(token: Option[String] = None): Future[Seq[PackageDetails]] =
    LedgerClient
      .stub(service, token)
      .listKnownPackages(PackageManagementClient.listKnownPackagesRequest)
      .map(_.packageDetails)

  def uploadDarFile(darFile: ByteString, token: Option[String] = None): Future[Unit] =
    LedgerClient
      .stub(service, token)
      .uploadDarFile(UploadDarFileRequest(darFile))
      .map(_ => ())

}
