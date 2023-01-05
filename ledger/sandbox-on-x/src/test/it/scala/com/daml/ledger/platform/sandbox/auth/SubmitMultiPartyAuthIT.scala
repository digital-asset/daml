// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.platform.sandbox.services.SubmitMultiPartyDummyCommand

import scala.concurrent.Future

final class SubmitMultiPartyAuthIT
    extends MultiPartyServiceCallAuthTests
    with SubmitMultiPartyDummyCommand {

  override def serviceCallName: String = "CommandSubmissionService#Submit"

  override def serviceCall(
      context: ServiceCallContext,
      requestSubmitters: RequestSubmitters,
  ): Future[Any] =
    submit(
      context.token,
      requestSubmitters.party,
      requestSubmitters.actAs,
      requestSubmitters.readAs,
    )

}
