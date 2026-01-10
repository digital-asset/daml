// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.package_management_service.*
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.util.BinaryFileUtil

import scala.concurrent.Future

final class ValidateDarFileAuthIT extends AdminServiceCallAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "PackageManagementService#ValidateDarFile"

  lazy private val request = {
    val darData = BinaryFileUtil
      .readByteStringFromFile(CantonExamplesPath)
      .valueOrFail("could not load examples")

    ValidateDarFileRequest(darData, submissionId = "", synchronizerId = "")
  }

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] =
    stub(PackageManagementServiceGrpc.stub(channel), context.token).validateDarFile(request)
}
