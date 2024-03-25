// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.logging.TracedLogger

/** Utility mixin for creating a single trace context from a batch of traced items */
object BatchTracing {
  def withTracedBatch[A <: HasTraceContext, B](logger: TracedLogger, items: NonEmpty[Seq[A]])(
      fn: TraceContext => NonEmpty[Seq[A]] => B
  ): B =
    fn(TraceContext.ofBatch(items)(logger))(items)
}
