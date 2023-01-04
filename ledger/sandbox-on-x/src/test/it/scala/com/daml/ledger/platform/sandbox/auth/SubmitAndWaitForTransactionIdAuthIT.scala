// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.platform.sandbox.services.SubmitAndWaitDummyCommand

import scala.concurrent.Future

final class SubmitAndWaitForTransactionIdAuthIT
    extends ReadWriteServiceCallAuthTests
    with SubmitAndWaitDummyCommand {

  override def serviceCallName: String = "CommandService#SubmitAndWaitForTransactionId"

  override def serviceCall(context: ServiceCallContext): Future[Any] =
    submitAndWaitForTransactionId(
      context.token,
      context.applicationId(serviceCallName),
      party = mainActor,
    )
}
