// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

final case class HistogramDefinition(nameRegex: String, bucketBoundaries: Seq[Double])
