// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.sequencing.protocol.RecipientsTest.*
import com.digitalasset.canton.topology.{
  SequencerGroup,
  SequencerId,
  TestingIdentityFactory,
  TestingTopology,
}

abstract class BftDomainSequencerApiTest extends SequencerApiTest {

  final class BftEnv extends Env {

    override protected val loggerFactory: NamedLoggerFactory =
      BftDomainSequencerApiTest.this.loggerFactory

    override lazy val topologyFactory =
      new TestingIdentityFactory(
        topology = TestingTopology()
          .withSequencerGroup(
            SequencerGroup(
              active = NonEmpty.mk(Seq, SequencerId(domainId)),
              passive = Seq.empty,
              threshold = PositiveInt.one,
            )
          )
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
          ),
        loggerFactory,
        List.empty,
      )
  }

  override protected final type FixtureParam = BftEnv

  override protected final def createEnv(): FixtureParam = new BftEnv

  "BFT Domain" when runSequencerApiTests()
}
