// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import com.digitalasset.canton.logging.ErrorLoggingContext

object InFlightAggregations {
  def tryApplyUpdates(
      inFlightAggregations: InFlightAggregations,
      updates: InFlightAggregationUpdates,
      ignoreInFlightAggregationErrors: Boolean,
  )(implicit loggingContext: ErrorLoggingContext): InFlightAggregations =
    updates.foldLeft(inFlightAggregations) { case (aggregations, (aggregationId, update)) =>
      aggregations.updatedWith(aggregationId) { previousO =>
        Some(
          InFlightAggregation.tryApplyUpdate(
            aggregationId,
            previousO,
            update,
            ignoreInFlightAggregationErrors,
          )
        )
      }
    }
}
