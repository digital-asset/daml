// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.version_service.{GetLedgerApiVersionRequest, VersionServiceGrpc}

import scala.concurrent.Future

final class GetLedgerApiVersionAuthIT extends UnsecuredServiceCallAuthTests {

  override def serviceCallName: String = "VersionService#GetLedgerApiVersion"

  override def serviceCall(context: ServiceCallContext): Future[Any] =
    stub(VersionServiceGrpc.stub(channel), context.token)
      .getLedgerApiVersion(new GetLedgerApiVersionRequest())

}
