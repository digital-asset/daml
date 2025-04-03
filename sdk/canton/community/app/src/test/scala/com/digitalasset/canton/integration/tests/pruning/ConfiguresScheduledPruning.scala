// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.pruning

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.admin.api.client.data.PruningSchedule
import com.digitalasset.canton.console.commands.PruningSchedulerAdministration

private[pruning] trait ConfiguresScheduledPruning {
  this: BaseTest =>

  /** Sets and reads pruning schedule asserting that schedule has been applied
    */
  def setAndVerifyPruningSchedule(
      pruning: PruningSchedulerAdministration[?],
      schedule: PruningSchedule,
  ): Unit = {
    pruning.set_schedule(schedule.cron, schedule.maxDuration, schedule.retention)
    pruning.get_schedule() shouldBe Some(schedule)
  }

  /** From a list of values (timestamps or offsets), infer the number of times pruning has happened
    */
  def countPrunings[T <: Ordered[?]](start: T)(values: Seq[T]): Int = values
    .foldLeft((0, start)) { case ((pruningCount, previousValue), nextValue) =>
      val newPruningCount = pruningCount + (if (nextValue > previousValue) 1 else 0)
      (newPruningCount, nextValue)
    }
    ._1
}
