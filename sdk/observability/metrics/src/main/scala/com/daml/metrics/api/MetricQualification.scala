// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api

/** Categorize metrics by type */
sealed trait MetricQualification
object MetricQualification {

  /** Any metric measuring processing time of some sort */
  case object Latency extends MetricQualification

  /** Any metric proportional to the normal usage of the system */
  case object Traffic extends MetricQualification

  /** Any metric that counts or relates to errors */
  case object Errors extends MetricQualification

  /** Any metric that measures the saturation of a resource
    *
    * A good example might be the length of a processing queue.
    */
  case object Saturation extends MetricQualification

  /** Any metric not relevant to the normal application operator */
  case object Debug extends MetricQualification
}
