// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.base.error

/** Trait to log on creation */
trait LogOnCreation {
  def logOnCreation: Boolean = true
  def logError(): Unit
  if (logOnCreation) {
    logError()
  }
}
