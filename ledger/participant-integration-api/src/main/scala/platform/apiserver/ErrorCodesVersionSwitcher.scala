// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import com.daml.error.ValueSwitch
import io.grpc.StatusRuntimeException

import scala.concurrent.Future

/** A mechanism to switch between the legacy error codes (v1) and the new self-service error codes (v2).
 * This class is intended to facilitate transition to self-service error codes.
 * Once the previous error codes are removed, this class should be dropped as well.
 */
final class ErrorCodesVersionSwitcher(enableSelfServiceErrorCodes: Boolean)
    extends ValueSwitch[StatusRuntimeException](enableSelfServiceErrorCodes){

  def chooseAsFailedFuture[T](
                               v1: => StatusRuntimeException,
                               v2: => StatusRuntimeException,
                             ): Future[T] = Future.failed(choose(v1 = v1, v2 = v2))
}
