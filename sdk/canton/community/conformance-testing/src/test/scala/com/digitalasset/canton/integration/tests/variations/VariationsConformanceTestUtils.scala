// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.variations

private[variations] object VariationsConformanceTestUtils {
  // Use higher parallelism than the default of 4 since conformance tests in variations CI job
  // run without neighbors (numPermits = 2)
  val ConcurrentTestRuns: Int = 8
}
