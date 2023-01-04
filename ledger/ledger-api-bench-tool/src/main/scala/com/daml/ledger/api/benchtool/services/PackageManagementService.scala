// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.services

import com.daml.ledger.api.benchtool.AuthorizationHelper
import com.daml.ledger.api.v1.admin.package_management_service.{
  PackageManagementServiceGrpc,
  UploadDarFileRequest,
}
import com.google.protobuf.ByteString
import io.grpc.Channel

import scala.concurrent.{ExecutionContext, Future}

class PackageManagementService(channel: Channel, authorizationToken: Option[String]) {
  private val service =
    AuthorizationHelper.maybeAuthedService(authorizationToken)(
      PackageManagementServiceGrpc.stub(channel)
    )

  def uploadDar(bytes: ByteString, submissionId: String)(implicit
      ec: ExecutionContext
  ): Future[Unit] =
    service
      .uploadDarFile(new UploadDarFileRequest(bytes, submissionId))
      .map(_ => ())

}
