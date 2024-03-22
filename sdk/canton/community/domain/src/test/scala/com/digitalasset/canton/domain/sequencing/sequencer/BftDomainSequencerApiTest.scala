// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.sequencing.protocol.RecipientsTest.*
import com.digitalasset.canton.topology.{
  SequencerGroup,
  SequencerId,
  TestingIdentityFactoryX,
  TestingTopologyX,
}
import com.digitalasset.canton.version.ProtocolVersion

abstract class BftDomainSequencerApiTest extends SequencerApiTest {

  final class BftEnv extends Env {

    override protected val loggerFactory: NamedLoggerFactory =
      BftDomainSequencerApiTest.this.loggerFactory

    override lazy val topologyFactory =
      new TestingIdentityFactoryX(
        topology = TestingTopologyX()
          .withSequencerGroup(
            SequencerGroup(
              active = Seq(SequencerId(domainId)),
              passive = Seq.empty,
              threshold = PositiveInt.one,
            )
          )
          .withSimpleParticipants(p11, p12, p13, p14, p15),
        loggerFactory,
        List.empty,
      )
  }

  override protected final type FixtureParam = BftEnv

  override protected final def createEnv(): FixtureParam = new BftEnv

  "BFT Domain" onlyRunWithOrGreaterThan ProtocolVersion.CNTestNet when runSequencerApiTests()
}
