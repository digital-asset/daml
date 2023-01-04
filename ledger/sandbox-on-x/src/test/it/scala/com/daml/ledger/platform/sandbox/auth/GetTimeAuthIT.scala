// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import scala.concurrent.Future

final class GetTimeAuthIT extends PublicServiceCallAuthTests with TimeAuth {

  override def serviceCallName: String = "TimeService#GetTime"

  override def serviceCall(context: ServiceCallContext): Future[Any] =
    loadTimeNow(context.token)

}
