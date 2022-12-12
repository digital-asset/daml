// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import scala.concurrent.Future

final class GetTimeAuthIT extends PublicServiceCallAuthTests with TimeAuth {

  override def serviceCallName: String = "TimeService#GetTime"

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    loadTimeNow(token)

}
