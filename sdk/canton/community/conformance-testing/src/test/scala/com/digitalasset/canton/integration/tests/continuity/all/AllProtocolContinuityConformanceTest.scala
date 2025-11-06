// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.continuity.all

import com.digitalasset.canton.integration.tests.continuity.{
  ProtocolContinuityConformanceTest,
  ProtocolContinuityConformanceTestParticipant,
  ProtocolContinuityConformanceTestSynchronizer,
}

/** The Protocol continuity tests test that we don't accidentally break protocol compatibility with
  * respect to the Ledger API. The tests are executed for all prior releases.
  */
trait AllProtocolContinuityConformanceTest extends ProtocolContinuityConformanceTest {
  lazy val testedReleases =
    ProtocolContinuityConformanceTest.previousSupportedReleases(logger)

  override lazy val ledgerApiTestToolVersions = testedReleases.map(_.releaseVersion.toString)

  protected val numShards: Int = 6
  protected def shard: Int
}

class ProtocolContinuityShard0ConformanceTestSynchronizer
    extends ProtocolContinuityConformanceTestSynchronizer
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 0
}

class ProtocolContinuityShard1ConformanceTestSynchronizer
    extends ProtocolContinuityConformanceTestSynchronizer
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 1
}

class ProtocolContinuityShard2ConformanceTestSynchronizer
    extends ProtocolContinuityConformanceTestSynchronizer
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 2
}

class ProtocolContinuityShard3ConformanceTestSynchronizer
    extends ProtocolContinuityConformanceTestSynchronizer
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 3
}

class ProtocolContinuityShard4ConformanceTestSynchronizer
    extends ProtocolContinuityConformanceTestSynchronizer
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 4
}

class ProtocolContinuityShard5ConformanceTestSynchronizer
    extends ProtocolContinuityConformanceTestSynchronizer
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 5
}

class ProtocolContinuityShard0ConformanceTestParticipant
    extends ProtocolContinuityConformanceTestParticipant
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 0
}

class ProtocolContinuityShard1ConformanceTestParticipant
    extends ProtocolContinuityConformanceTestParticipant
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 1
}

class ProtocolContinuityShard2ConformanceTestParticipant
    extends ProtocolContinuityConformanceTestParticipant
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 2
}

class ProtocolContinuityShard3ConformanceTestParticipant
    extends ProtocolContinuityConformanceTestParticipant
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 3
}

class ProtocolContinuityShard4ConformanceTestParticipant
    extends ProtocolContinuityConformanceTestParticipant
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 4
}

class ProtocolContinuityShard5ConformanceTestParticipant
    extends ProtocolContinuityConformanceTestParticipant
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 5
}
