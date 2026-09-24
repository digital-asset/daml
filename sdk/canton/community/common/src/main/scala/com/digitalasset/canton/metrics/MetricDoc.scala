// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricQualification

object MetricDoc {

  final case class Item(
      name: String,
      summary: String,
      description: String,
      metricType: String,
      qualification: MetricQualification,
      labelsWithDescription: Map[String, String],
  )

}
