// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.update_service.{GetTransactionByIdRequest, UpdateServiceGrpc}
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import io.grpc.Status
import org.scalatest.Assertion

import java.util.UUID
import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

// TODO(#23504) remove
@nowarn("cat=deprecation")
final class GetTransactionTreeByIdAuthIT extends ReadOnlyServiceCallAuthTests {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String = "UpdateService#GetTransactionTreeById"

  override def successfulBehavior(f: Future[Any])(implicit ec: ExecutionContext): Assertion =
    expectFailure(f, Status.Code.NOT_FOUND)

  private def request(mainActorId: String) =
    new GetTransactionByIdRequest(UUID.randomUUID.toString, List(mainActorId), None)

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] =
    stub(UpdateServiceGrpc.stub(channel), context.token)
      .getTransactionTreeById(request(getMainActorId))

}
