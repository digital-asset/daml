// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.platform.sandbox.services.SubmitAndWaitMultiPartyDummyCommand

import scala.concurrent.Future

final class SubmitAndWaitMultiPartyAuthIT
    extends MultiPartyServiceCallAuthTests
    with SubmitAndWaitMultiPartyDummyCommand {

  override def serviceCallName: String = "CommandService#SubmitAndWait"

  override def serviceCallWithToken(
      token: Option[String],
      requestSubmitters: RequestSubmitters,
  ): Future[Any] =
    submitAndWait(token, requestSubmitters.party, requestSubmitters.actAs, requestSubmitters.readAs)

}
