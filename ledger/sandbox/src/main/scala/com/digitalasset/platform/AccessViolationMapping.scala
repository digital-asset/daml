// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.dao.events.QueryNonPruned.OutOfRangeAccessViolation

// TODO(oliver) - is there a better place or way to share code mapping the internal pruning
//  error to the grpc error?
trait AccessViolationMapping {

  final protected def mapAccessViolations(logger: ContextualizedLogger)(
      implicit logCtx: LoggingContext): PartialFunction[Throwable, Throwable] = {
    case OutOfRangeAccessViolation(pruningOffsetUpToInclusive, error) =>
      val message =
        s"Out of range offset access violation before or at ${pruningOffsetUpToInclusive.toHexString}: $error"
      logger.error(message)
      ErrorFactories.participantPrunedDataAccessed(message)
  }

}
