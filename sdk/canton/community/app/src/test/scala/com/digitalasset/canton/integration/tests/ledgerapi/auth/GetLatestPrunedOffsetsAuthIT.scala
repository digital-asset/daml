// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.state_service.{GetLatestPrunedOffsetsRequest, StateServiceGrpc}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}

import scala.concurrent.Future

class GetLatestPrunedOffsetsAuthIT extends PublicServiceCallAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "StateService#GetLatestPrunedOffsets"

  private val request = GetLatestPrunedOffsetsRequest()

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] =
    stub(StateServiceGrpc.stub(channel), context.token).getLatestPrunedOffsets(request)

}
