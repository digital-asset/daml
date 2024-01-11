// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing

import com.digitalasset.canton.sequencing.protocol.AggregationId

package object sequencer {

  type InFlightAggregations = Map[AggregationId, InFlightAggregation]
  type InFlightAggregationUpdates = Map[AggregationId, InFlightAggregationUpdate]
}
