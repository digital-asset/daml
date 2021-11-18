// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import com.daml.concurrent.ExecutionContext

package object dec {

  // Starting from Scala 2.13 this can deleted and replaced by `parasitic`
  val DirectExecutionContext: ExecutionContext[Nothing] =
    ExecutionContext(scala.concurrent.ExecutionContext.parasitic)

}
