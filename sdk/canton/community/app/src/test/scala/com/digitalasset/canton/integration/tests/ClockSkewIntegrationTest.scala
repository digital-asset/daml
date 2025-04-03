// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Reliability.{
  AdverseScenario,
  Component,
  ReliabilityTest,
  Remediation,
}
import com.digitalasset.canton.config.{ClockConfig, DbConfig}
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** Test the system when running participants and synchronizers with substantial clock skew
  */
abstract class ClockSkewIntegrationTest(skews: Map[String, FiniteDuration])
    extends CommunityIntegrationTest
    with SharedEnvironment {

  /*
    Why 10 seconds clock skew?
    This test uses default timeout settings, up to 10 seconds clock skew is already pretty
    significant!

    Increasing this clock skew to more than 10 seconds introduces flakes! A very large clock
    skew ultimately just breaks this test. – If you want to increase the clock skew you also
    have to adapt the default timeout configuration (`ledgerTimeRecordTimeTolerance` and maybe
    others), and do thorough testing.

    Also, currently (Oct 2024) the largest distributed system in operation (Canton Network)
    does not have any particular issue with clock skew, and uses the default timeout settings.
   */
  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransform(
        _.focus(_.parameters.clock)
          .replace(ClockConfig.WallClock(skews))
      )

  s"we can ping even with clock skews: $skews".taggedAs(
    ReliabilityTest(
      Component("Participant node", "two participants connected to each other via synchronizer"),
      AdverseScenario(
        dependency = "Clock",
        details = s"Clock between nodes is skewed: $skews",
      ),
      Remediation(
        remediator = "Configuration of the transaction protocol",
        action = "Lenient default timeout settings in the transaction processing protocol",
      ),
      outcome = "Transactions still possible between participants",
    )
  ) in { implicit env =>
    import env.*

    loggerFactory.assertLogsUnorderedOptional(
      {
        clue("participant1 connect") {
          participant1.synchronizers.connect_local(sequencer1, daName)
        }
        clue("maybe ping participant1") {
          participant1.health.maybe_ping(participant1) shouldBe defined
        }

        clue("participant2 connect") {
          participant2.synchronizers.connect_local(sequencer1, daName)
        }
        // We need to wait until participant2 has vetted the AdminWorkflow packages, but this is already
        // handled by `connect_local` through `utils.synchronize_topology`
        clue("maybe ping participant2") {
          participant2.health.maybe_ping(participant2) shouldBe defined
        }
        // We need to wait until package vetting is synchronized by all participants, but this is already
        // handled by `connect_local` through `utils.synchronize_topology`

        clue("couple of pings between p1 and p2") {
          forAll(1 to 3) { _ =>
            participant1.health.maybe_ping(participant2) shouldBe defined
          }
        }
      },
      // because of the clock skew, we get warnings that the package vetting transaction takes longer to become effective
      (
        LogEntryOptionality.OptionalMany,
        _.warningMessage should include("Waiting for transaction"),
      ),
      // because of the clock skew, we may get delay warnings
      (
        LogEntryOptionality.OptionalMany,
        _.warningMessage should include("Late processing (or clock skew)"),
      ),
    )
  }

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

// We test with each node either ahead or behind of all the other nodes
class ClockSkewIntegrationTestPostgresParticipant1Behind
    extends ClockSkewIntegrationTest(Map("participant1" -> -10.seconds)) {}

class ClockSkewIntegrationTestPostgresParticipant1Ahead
    extends ClockSkewIntegrationTest(Map("participant1" -> 10.seconds)) {}

class ClockSkewIntegrationTestPostgresParticipant2Behind
    extends ClockSkewIntegrationTest(Map("participant2" -> -10.seconds)) {}

class ClockSkewIntegrationTestPostgresParticipant2Ahead
    extends ClockSkewIntegrationTest(Map("participant2" -> 10.seconds)) {}

class ClockSkewIntegrationTestPostgresSequencerBehind
    extends ClockSkewIntegrationTest(Map("sequencer1" -> -10.seconds)) {}

class ClockSkewIntegrationTestPostgresSequencerAhead
    extends ClockSkewIntegrationTest(Map("sequencer1" -> 10.seconds)) {}

class ClockSkewIntegrationTestPostgresMediatorBehind
    extends ClockSkewIntegrationTest(Map("mediator1" -> -10.seconds)) {}

class ClockSkewIntegrationTestPostgresMediatorAhead
    extends ClockSkewIntegrationTest(Map("mediator1" -> 10.seconds)) {}
