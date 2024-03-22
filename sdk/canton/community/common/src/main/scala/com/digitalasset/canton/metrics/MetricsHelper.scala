// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.Gauge
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.time.Clock

object MetricsHelper {

  /** Updates a gauge representing an age in hours based on the difference of the provided timestamp and the
    * current time or with zero if the timestamp is None. The difference is rounded down to the hour.
    *
    * @param clock clock to determine the current time
    * @param metric gauge metric representing the age in hours
    * @param timestamp optional timestamp whose age to assess, if None age is considered zero
    */
  def updateAgeInHoursGauge(
      clock: Clock,
      metric: Gauge[Long],
      timestamp: Option[CantonTimestamp],
  ): Unit =
    metric.updateValue(timestamp.fold(0L)(ts => (clock.now - ts).toHours))

}
