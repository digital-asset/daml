// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.scheduler

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality

trait IgnoresTransientSchedulerErrors {
  this: BaseTest =>

  // Scheduled pruning can optionally produce warnings if nodes are temporarily not ready to be pruned. #14223
  protected def ignoreTransientSchedulerErrors[T](
      pruningScheduler: String
  )(code: => T): T =
    loggerFactory.assertLogsUnorderedOptional(
      code,
      (
        LogEntryOptionality.OptionalMany,
        (entry: LogEntry) => {
          entry.loggerName should include(pruningScheduler)
          entry.warningMessage should include regex "Backing off .* or until next window after error"
        },
      ),
    )
}
