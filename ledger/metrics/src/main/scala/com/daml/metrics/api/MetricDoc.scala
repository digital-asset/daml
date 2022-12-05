// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api

import scala.annotation.StaticAnnotation

object MetricDoc {

  sealed trait MetricQualification
  object MetricQualification {
    case object Latency extends MetricQualification
    case object Traffic extends MetricQualification
    case object Errors extends MetricQualification
    case object Saturation extends MetricQualification
    case object Debug extends MetricQualification
  }

  case class Tag(
      summary: String,
      description: String,
      qualification: MetricQualification,
  ) extends StaticAnnotation

  // The GroupTag can be defined for metrics that share similar names and should be grouped using a
  // wildcard (the representative).
  case class GroupTag(representative: String, groupableClass: Class[_]) extends StaticAnnotation

}
