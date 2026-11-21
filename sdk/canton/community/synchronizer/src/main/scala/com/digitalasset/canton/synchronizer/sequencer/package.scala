// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer

import com.digitalasset.canton.sequencing.protocol.AggregationId

package object sequencer {

  type InFlightAggregations = Map[AggregationId, InFlightAggregation]
  type InFlightAggregationUpdates = Map[AggregationId, InFlightAggregationUpdate]
}
