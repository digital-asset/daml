// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.update_service.{
  GetUpdateTreesResponse,
  GetUpdatesRequest,
  UpdateServiceGrpc,
}
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.services.SubmitAndWaitDummyCommand
import io.grpc.stub.StreamObserver

import scala.annotation.nowarn

// TODO(#23504) remove
@nowarn("cat=deprecation")
final class GetUpdateTreesAuthIT
    extends ExpiringStreamServiceCallAuthTestsLegacy[GetUpdateTreesResponse]
    with SubmitAndWaitDummyCommand {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String = "UpdateService#GetUpdateTrees"

  private def request(mainActorId: String) =
    new GetUpdatesRequest(
      beginExclusive = participantBegin,
      endInclusive = None,
      filter = txFilterFor(mainActorId),
      verbose = false,
      updateFormat = None,
    )

  override protected def stream(
      context: ServiceCallContext,
      mainActorId: String,
      env: TestConsoleEnvironment,
  ): StreamObserver[GetUpdateTreesResponse] => Unit =
    observer =>
      stub(UpdateServiceGrpc.stub(channel), context.token)
        .getUpdateTrees(request(mainActorId), observer)

}
