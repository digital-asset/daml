// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.update_service.{GetTransactionByOffsetRequest, UpdateServiceGrpc}
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import io.grpc.Status
import org.scalatest.Assertion

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

// TODO(#23504) remove
@nowarn("cat=deprecation")
final class GetTransactionByOffsetAuthIT extends ReadOnlyServiceCallAuthTests {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String = "UpdateService#GetTransactionByOffset"

  override def successfulBehavior(f: Future[Any])(implicit ec: ExecutionContext): Assertion =
    expectFailure(f, Status.Code.NOT_FOUND)

  private def request(mainActorId: String) =
    new GetTransactionByOffsetRequest(
      Random.nextLong(Long.MaxValue) + 1,
      List(mainActorId),
      None,
    )

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] =
    stub(UpdateServiceGrpc.stub(channel), context.token)
      .getTransactionByOffset(request(getMainActorId))

}
