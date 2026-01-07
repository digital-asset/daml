// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.continuity.latest

import com.digitalasset.canton.integration.tests.continuity.{
  ProtocolContinuityConformanceTest,
  ProtocolContinuityConformanceTestParticipant,
  ProtocolContinuityConformanceTestSynchronizer,
}
import com.digitalasset.canton.util.ReleaseUtils

/** The Protocol continuity tests test that we don't accidentally break protocol compatibility with
  * respect to the Ledger API. The tests are executed for the latest published release.
  */
trait LatestProtocolContinuityConformanceTest extends ProtocolContinuityConformanceTest {
  lazy val testedReleases: List[ReleaseUtils.TestedRelease] =
    ProtocolContinuityConformanceTest.latestSupportedRelease(logger)

  protected val numShards: Int = 1
  protected def shard: Int
}

class LatestProtocolContinuityShard0ConformanceTestSynchronizer
    extends ProtocolContinuityConformanceTestSynchronizer
    with LatestProtocolContinuityConformanceTest {
  override val shard: Int = 0
}

class LatestProtocolContinuityShard0ConformanceTestParticipant
    extends ProtocolContinuityConformanceTestParticipant
    with LatestProtocolContinuityConformanceTest {
  override val shard: Int = 0
}
