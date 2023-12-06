// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.daml.error.{BaseError, ContextualizedErrorLogger}

trait LogOnCreation { self: BaseError =>

  implicit def logger: ContextualizedErrorLogger

  // automatically log the error on creation
  logWithContext()
}
