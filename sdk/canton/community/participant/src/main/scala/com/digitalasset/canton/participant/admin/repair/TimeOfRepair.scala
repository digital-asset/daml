// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import com.digitalasset.canton.RepairCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.util.TimeOfChange

private[repair] final case class TimeOfRepair(
    timestamp: CantonTimestamp,
    repairCounter: RepairCounter,
) {
  def toToc: TimeOfChange = TimeOfChange(timestamp, Some(repairCounter))

  def incrementRepairCounter: Either[String, TimeOfRepair] =
    repairCounter.increment.map(TimeOfRepair(timestamp, _))
}
