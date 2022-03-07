// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import io.prometheus.client.hotspot.DefaultExports

object JvmMetrics {
  def initialize(): Unit = DefaultExports.initialize()
}
