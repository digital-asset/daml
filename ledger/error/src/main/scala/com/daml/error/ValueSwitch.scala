// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

/** A mechanism to switch between the legacy error codes (v1) and the new self-service error codes (v2).
  * This class is intended to facilitate transition to self-service error codes.
  * Once the previous error codes are removed, this class and its specializations should be dropped as well.
  */
class ValueSwitch(val enableSelfServiceErrorCodes: Boolean) {
  def choose[X](
      v1: => X,
      v2: => X,
  ): X =
    if (enableSelfServiceErrorCodes) {
      v2
    } else {
      v1
    }
}
