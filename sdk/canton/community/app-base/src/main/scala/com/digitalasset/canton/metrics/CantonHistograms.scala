// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.{HistogramInventory, MetricName}
import com.digitalasset.canton.domain.metrics.{MediatorHistograms, SequencerHistograms}
import com.digitalasset.canton.participant.metrics.ParticipantHistograms

/** Pre-register histogram metrics
  *
  * Open telemetry requires us to define the histogram buckets before defining the actual metric.
  * Therefore, we define the name here and ensure that the same name is known wherever the metric is used.
  */
class CantonHistograms()(implicit val inventory: HistogramInventory) {

  val prefix: MetricName = MetricName.Daml
  private[metrics] val participant: ParticipantHistograms =
    new ParticipantHistograms(prefix)
  private[metrics] val mediator: MediatorHistograms = new MediatorHistograms(prefix)
  private[canton] val sequencer: SequencerHistograms = new SequencerHistograms(prefix)

}
