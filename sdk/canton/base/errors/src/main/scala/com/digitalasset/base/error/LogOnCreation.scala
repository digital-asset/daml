// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.base.error

/** Trait to log on creation */
trait LogOnCreation {

  self: BaseError =>

  implicit def logger: ContextualizedErrorLogger

  def logOnCreation: Boolean = true

  def logError(): Unit = logWithContext()(logger)

  if (logOnCreation) {
    logError()
  }

}
