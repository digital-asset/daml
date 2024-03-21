// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import java.io.FileInputStream

import com.daml.ledger.api.v1.admin.package_management_service._
import com.google.protobuf.ByteString

import scala.concurrent.Future

final class UploadDarAuthIT extends AdminServiceCallAuthTests {

  override def serviceCallName: String = "PackageManagementService#UploadDar"

  private val request = new UploadDarFileRequest(ByteString.readFrom(new FileInputStream(darFile)))

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    stub(PackageManagementServiceGrpc.stub(channel), token).uploadDarFile(request)

}
