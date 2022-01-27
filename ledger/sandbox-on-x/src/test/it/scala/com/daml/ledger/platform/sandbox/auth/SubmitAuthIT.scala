// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.platform.sandbox.services.SubmitDummyCommand

import scala.concurrent.Future

final class SubmitAuthIT extends ReadWriteServiceCallAuthTests with SubmitDummyCommand {

  override def serviceCallName: String = "CommandSubmissionService#Submit"

  override def serviceCallWithToken(token: Option[String]): Future[Any] = submit(token)

  override def serviceCallWithoutApplicationId(token: Option[String]): Future[Any] =
    submit(token, "")
}
