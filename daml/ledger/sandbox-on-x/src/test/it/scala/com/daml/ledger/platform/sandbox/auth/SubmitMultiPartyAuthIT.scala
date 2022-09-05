// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.platform.sandbox.services.SubmitMultiPartyDummyCommand

import scala.concurrent.Future

final class SubmitMultiPartyAuthIT
    extends MultiPartyServiceCallAuthTests
    with SubmitMultiPartyDummyCommand {

  override def serviceCallName: String = "CommandSubmissionService#Submit"

  override def serviceCallWithToken(
      token: Option[String],
      requestSubmitters: RequestSubmitters,
  ): Future[Any] =
    submit(token, requestSubmitters.party, requestSubmitters.actAs, requestSubmitters.readAs)

}
