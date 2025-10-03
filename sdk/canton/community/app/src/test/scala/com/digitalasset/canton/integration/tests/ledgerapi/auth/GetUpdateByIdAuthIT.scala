// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.transaction_filter.UpdateFormat
import com.daml.ledger.api.v2.update_service.{GetUpdateByIdRequest, UpdateServiceGrpc}
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.protocol.TestUpdateId
import io.grpc.Status
import org.scalatest.Assertion

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

final class GetUpdateByIdAuthIT extends ReadOnlyServiceCallAuthTests {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String = "UpdateService#GetUpdateById"

  override def successfulBehavior(f: Future[Any])(implicit ec: ExecutionContext): Assertion =
    expectFailure(f, Status.Code.NOT_FOUND)

  private def request(updateFormat: Option[UpdateFormat]) =
    new GetUpdateByIdRequest(
      updateId = TestUpdateId(UUID.randomUUID.toString).toHexString,
      updateFormat = updateFormat,
    )

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] =
    stub(UpdateServiceGrpc.stub(channel), context.token)
      .getUpdateById(request(updateFormat(Some(getMainActorId))))

}
