// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.CacheMetrics
import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.MetricName

class SessionKeyStoreMetrics(
    prefix: MetricName,
    labeledMetricsFactory: LabeledMetricsFactory,
) {
  val sessionKeyCacheSenderMetrics =
    new CacheMetrics(prefix :+ "session_key_cache_sender", labeledMetricsFactory)
  val sessionKeyCacheRecipientMetrics =
    new CacheMetrics(prefix :+ "session_key_cache_recipient", labeledMetricsFactory)
}
