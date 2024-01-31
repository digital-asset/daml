// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.daml.metrics.api.MetricName
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.metrics.InMemoryMetricsFactory

object ParticipantTestMetrics
    extends ParticipantMetrics(
      MetricName("test"),
      new InMemoryMetricsFactory,
    ) {

  val domain: SyncDomainMetrics = this.domainMetrics(DomainAlias.tryCreate("test"))
}
