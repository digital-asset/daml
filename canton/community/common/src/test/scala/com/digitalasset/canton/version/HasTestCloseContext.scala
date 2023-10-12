// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.config.{DefaultProcessingTimeouts, ProcessingTimeout}
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable}
import com.digitalasset.canton.logging.{NamedLogging, TracedLogger}
import com.digitalasset.canton.version.HasTestCloseContext.makeTestCloseContext

object HasTestCloseContext {
  def makeTestCloseContext(loggerP: TracedLogger): CloseContext = CloseContext(new FlagCloseable {
    override protected def timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing
    override protected def logger: TracedLogger = loggerP
  })
}

trait HasNonImplicitTestCloseContext { self: NamedLogging =>
  protected val testCloseContext: CloseContext = makeTestCloseContext(self.logger)
}

trait HasTestCloseContext { self: NamedLogging =>
  implicit protected val testCloseContext: CloseContext = makeTestCloseContext(self.logger)
}
