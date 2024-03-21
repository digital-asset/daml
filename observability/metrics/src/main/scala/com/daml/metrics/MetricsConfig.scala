// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

/** Bucket boundary definitions for histograms
  *
  * @param name Instrument name that may contain the wildcard characters * and ?
  * @param bucketBoundaries The boundaries of the histogram buckets
  */
final case class HistogramDefinition(name: String, bucketBoundaries: Seq[Double])
