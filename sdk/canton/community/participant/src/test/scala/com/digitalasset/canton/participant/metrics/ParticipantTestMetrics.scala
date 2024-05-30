// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.daml.metrics.api.MetricName
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.DomainAlias
import com.daml.metrics.api.HistogramInventory

object ParticipantTestMetrics
    extends ParticipantMetrics(
      new ParticipantHistograms(MetricName("test"))(new HistogramInventory),
      new NoOpMetricsFactory,
    ) {

  val domain: SyncDomainMetrics = this.domainMetrics(DomainAlias.tryCreate("test"))
}
