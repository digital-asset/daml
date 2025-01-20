// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.testing.InMemoryMetricsFactory as DamlInMemoryMetricsFactory
import com.digitalasset.canton.metrics.MetricHandle.LabeledMetricsFactory

class InMemoryMetricsFactory extends DamlInMemoryMetricsFactory with LabeledMetricsFactory
