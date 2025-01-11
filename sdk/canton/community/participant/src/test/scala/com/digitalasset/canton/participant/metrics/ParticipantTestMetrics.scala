// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName}
import com.digitalasset.canton.SynchronizerAlias

object ParticipantTestMetrics
    extends ParticipantMetrics(
      new ParticipantHistograms(MetricName("test"))(new HistogramInventory),
      new NoOpMetricsFactory,
    ) {

  val synchronizer: SyncDomainMetrics = this.domainMetrics(SynchronizerAlias.tryCreate("test"))
}
