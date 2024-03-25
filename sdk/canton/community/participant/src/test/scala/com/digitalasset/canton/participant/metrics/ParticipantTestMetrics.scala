// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.api.MetricName
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.metrics.InMemoryMetricsFactory
import com.digitalasset.canton.metrics.MetricHandle.CantonDropwizardMetricsFactory

object ParticipantTestMetrics
    extends ParticipantMetrics(
      "test_participant",
      MetricName("test"),
      new CantonDropwizardMetricsFactory(new MetricRegistry()),
      new InMemoryMetricsFactory,
      new MetricRegistry,
    ) {

  val domain: SyncDomainMetrics = this.domainMetrics(DomainAlias.tryCreate("test"))

}
