// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import io.grpc.StatusRuntimeException

final case class ErrorCodesVersionSwitcher(enableErrorCodesV2: Boolean) {
  def choose(
      v1: => StatusRuntimeException,
      v2: => StatusRuntimeException,
  ): StatusRuntimeException = {
    if (enableErrorCodesV2) {
      v2
    } else {
      v1
    }
  }
}
