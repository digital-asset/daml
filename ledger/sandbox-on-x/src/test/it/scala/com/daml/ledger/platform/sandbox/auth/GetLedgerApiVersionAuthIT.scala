// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.version_service.{GetLedgerApiVersionRequest, VersionServiceGrpc}

import scala.concurrent.Future

final class GetLedgerApiVersionAuthIT extends UnsecuredServiceCallAuthTests {

  override def serviceCallName: String = "VersionService#GetLedgerApiVersion"

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    stub(VersionServiceGrpc.stub(channel), token)
      .getLedgerApiVersion(new GetLedgerApiVersionRequest())

}
