// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.platform.sandbox.services.SubmitDummyCommand

import scala.concurrent.Future

final class SubmitAuthIT extends ReadWriteServiceCallAuthTests with SubmitDummyCommand {

  override def serviceCallName: String = "CommandSubmissionService#Submit"

  override def serviceCall(context: ServiceCallContext): Future[Any] =
    submit(context.token, context.applicationId(serviceCallName))
}
