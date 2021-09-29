// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import io.grpc.StatusRuntimeException

/** A mechanism to switch between the previous error codes (v1) and the new self-service error codes (v2).
  * This class is intended to facilitate transition to self-service error codes.
  * Once the previous error codes are remvoed and this class should be dropped as well.
  */
final class ErrorCodesVersionSwitcher(enableSelfServiceErrorCodes: Boolean) {
  def choose(
      v1: => StatusRuntimeException,
      v2: => StatusRuntimeException,
  ): StatusRuntimeException = {
    if (enableSelfServiceErrorCodes) {
      v2
    } else {
      v1
    }
  }
}
