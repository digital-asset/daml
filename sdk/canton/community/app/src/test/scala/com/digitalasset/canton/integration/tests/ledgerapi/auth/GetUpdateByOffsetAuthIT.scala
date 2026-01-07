// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.transaction_filter.UpdateFormat
import com.daml.ledger.api.v2.update_service.{GetUpdateByOffsetRequest, UpdateServiceGrpc}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import io.grpc.Status
import org.scalatest.Assertion

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

final class GetUpdateByOffsetAuthIT extends ReadOnlyServiceCallAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "UpdateService#GetUpdateByOffset"

  override def successfulBehavior(f: Future[Any])(implicit ec: ExecutionContext): Assertion =
    expectFailure(f, Status.Code.NOT_FOUND)

  private def request(updateFormat: Option[UpdateFormat]) =
    new GetUpdateByOffsetRequest(
      offset = Random.nextLong(Long.MaxValue) + 1,
      updateFormat = updateFormat,
    )

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] =
    stub(UpdateServiceGrpc.stub(channel), context.token)
      .getUpdateByOffset(request(updateFormat(Some(getMainActorId))))

}
