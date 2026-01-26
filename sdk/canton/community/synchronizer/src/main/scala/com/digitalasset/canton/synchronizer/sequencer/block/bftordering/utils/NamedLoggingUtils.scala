// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils

import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.tracing.TraceContext

trait NamedLoggingUtils { self: NamedLogging =>

  protected final def debug(s: => String)(implicit traceContext: TraceContext): Unit =
    logger.debug(s)

  protected final def warn(s: => String)(implicit traceContext: TraceContext): Unit =
    logger.warn(s)

  protected final def logUnexpected(s: => String)(implicit traceContext: TraceContext): Unit =
    logger.warn(s"[UNEXPECTED] $s")
}
