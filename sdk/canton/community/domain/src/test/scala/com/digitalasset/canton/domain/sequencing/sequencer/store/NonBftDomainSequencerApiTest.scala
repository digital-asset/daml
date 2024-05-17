// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.store

import com.digitalasset.canton.domain.sequencing.sequencer.SequencerApiTest
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.sequencing.protocol.RecipientsTest.*
import com.digitalasset.canton.topology.TestingTopology

abstract class NonBftDomainSequencerApiTest extends SequencerApiTest {

  final class NonBftEnv extends Env {

    override protected val loggerFactory: NamedLoggerFactory =
      NonBftDomainSequencerApiTest.this.loggerFactory

    override lazy val topologyFactory =
      TestingTopology(domainParameters = List.empty)
        .withSimpleParticipants(
          p1,
          p2,
          p3,
          p4,
          p5,
          p6,
          p7,
          p8,
          p9,
          p10,
          p11,
          p12,
          p13,
          p14,
          p15,
          p17,
          p18,
          p19,
        )
        .build(loggerFactory)
  }

  override protected final type FixtureParam = NonBftEnv

  override protected final def createEnv(): FixtureParam = new NonBftEnv

  runSequencerApiTests()
}
