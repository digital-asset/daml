// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.package_management_service.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.util.BinaryFileUtil

import scala.concurrent.Future

final class UploadDarFileAuthIT extends AdminServiceCallAuthTests {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String = "PackageManagementService#UploadDarFile"

  lazy private val request = {
    val darData = BinaryFileUtil
      .readByteStringFromFile(CantonExamplesPath)
      .valueOrFail("could not load examples")

    UploadDarFileRequest(darData, submissionId = "")
  }

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] =
    stub(PackageManagementServiceGrpc.stub(channel), context.token).uploadDarFile(request)
}
