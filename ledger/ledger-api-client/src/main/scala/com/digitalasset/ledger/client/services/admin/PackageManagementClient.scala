// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.services.admin

import com.digitalasset.ledger.api.v1.admin.package_management_service.{
  ListKnownPackagesRequest,
  PackageDetails,
  UploadDarFileRequest
}
import com.digitalasset.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

class PackageManagementClient(service: PackageManagementService)(implicit ec: ExecutionContext) {
  def listKnownPackages(): Future[Seq[PackageDetails]] = {
    service
      .listKnownPackages(ListKnownPackagesRequest())
      .map(_.packageDetails)
  }

  def uploadDarFile(darFile: ByteString): Future[Unit] = {
    service
      .uploadDarFile(UploadDarFileRequest(darFile))
      .map(_ => ())
  }
}
