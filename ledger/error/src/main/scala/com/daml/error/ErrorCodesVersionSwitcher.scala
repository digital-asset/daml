// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error
import io.grpc.StatusRuntimeException

import scala.concurrent.Future

class ErrorCodesVersionSwitcher(enableSelfServiceErrorCodes: Boolean)
    extends ValueSwitch[StatusRuntimeException](enableSelfServiceErrorCodes) {

  def chooseAsFailedFuture[T](
      v1: => StatusRuntimeException,
      v2: => StatusRuntimeException,
  ): Future[T] = Future.failed(choose(v1 = v1, v2 = v2))
}
