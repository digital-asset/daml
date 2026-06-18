// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseSharedStorage}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.sequencing.SequencerConnection
import org.scalatest.Assertion

// TODO(#16089): Currently this test cannot work due to the issue
trait SequencerHighAvailabilityIntegrationTest
    extends CommunityIntegrationTest
    with IsolatedEnvironments {

  protected val expectedLogs: Seq[(LogEntryOptionality, LogEntry => Assertion)] = Seq(
    (
      LogEntryOptionality.OptionalMany,
      { entry =>
        entry.errorMessage should startWith("Failed to send commitment message")
        entry.loggerName should include("AcsCommitmentProcessor")
      },
    ),
    (
      LogEntryOptionality.OptionalMany,
      _.warningMessage shouldBe "Locked connection was lost, trying to rebuild",
    ),
    // we struggle to shutdown the sequencer grpc connection if both sequencers are
    // offline as we can't close it, so we get this grpc warning
    (
      LogEntryOptionality.Optional,
      _.warningMessage should include("ManagedChannelOrphanWrapper"),
    ),
  )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 2,
        numSequencers = 4,
        numMediators = 1,
      )
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(EnvironmentDefinition.S1M1)
      }
      // TODO(#16089): Don't use the manual start if not necessary
      .withManualStart

  "environment using 2 sequencers in HA setup" should {
    // TODO(#16089): Re-enable this test
    "still be able to ping when one of the sequencers is down" ignore { implicit env =>
      import env.*

      // TODO(#16089): Don't use the manual start and bootstrap if not really necessary
      sequencers.local.start()
      bootstrap.synchronizer(
        synchronizerName = daName.unwrap,
        staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
        synchronizerOwners = Seq(sequencer1),
        synchronizerThreshold = PositiveInt.one,
        sequencers = Seq(sequencer1),
        mediators = Seq(mediator1),
      )

      participant1.synchronizers.connect_multi(daName, Seq(sequencer1, sequencer2))
      participant2.synchronizers.connect_multi(daName, Seq(sequencer1, sequencer2))

      participant1.health.ping(participant2.id)

      loggerFactory.assertLogsUnorderedOptional(
        {
          sequencer1.stop()
          participant1.health.ping(participant2.id)

          sequencer1.start()
          sequencer2.stop()
          participant1.health.ping(participant2.id)

          env.stopAll()
        },
        expectedLogs *,
      )
    }
  }

  "environment using 4 sequencers in HA setup" should {
    // TODO(#16089): Re-enable this test
    "still be able to ping when three of them are down" ignore { implicit env =>
      import env.*

      // TODO(#16089): Don't use the manual start and bootstrap if not really necessary
      sequencers.local.start()
      bootstrap.synchronizer(
        synchronizerName = daName.unwrap,
        staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
        synchronizerOwners = Seq(sequencer1),
        synchronizerThreshold = PositiveInt.one,
        sequencers = Seq(sequencer1),
        mediators = Seq(mediator1),
      )

      val sequencerConns =
        Seq[SequencerConnection](sequencer1, sequencer2, sequencer3, sequencer4)
      participant1.synchronizers.connect_multi(daName, sequencerConns)
      participant2.synchronizers.connect_multi(daName, sequencerConns)

      participant1.health.ping(participant2.id)

      loggerFactory.assertLogsUnorderedOptional(
        {
          sequencer1.stop()
          sequencer2.stop()
          sequencer3.stop()
          participant1.health.ping(participant2.id)

          sequencer1.start()
          sequencer4.stop()
          participant1.health.ping(participant2.id)

          env.stopAll()
        },
        expectedLogs *,
      )
    }
  }

  "environment with a late starting sequencer in HA setup" should {
    // TODO(#16089): Re-enable this test
    "be able to be used after starting delayed" ignore { implicit env =>
      import env.*

      // TODO(#16089): Don't use the manual start and bootstrap if not really necessary
      sequencer1.start()
      sequencer2.start()
      bootstrap.synchronizer(
        synchronizerName = daName.unwrap,
        staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
        synchronizerOwners = Seq(sequencer1),
        synchronizerThreshold = PositiveInt.one,
        sequencers = Seq(sequencer1),
        mediators = Seq(mediator1),
      )

      clue("p1 connect to two sequencers") {
        participant1.synchronizers.connect_multi(
          daName,
          Seq[SequencerConnection](sequencer1, sequencer2),
        )
      }

      clue("p1 can ping itself") {
        participant1.health.ping(participant1.id)
      }

      sequencer3.start()

      clue("connect p1 to many sequencers") {
        participant1.synchronizers.modify(
          daName,
          _.addConnection(sequencer3.sequencerConnection).value,
        )
      }
      clue("connect mediator also to sequencer3") {
        mediator1.sequencer_connection.modify(_.addEndpoints(sequencer3).value)
      }

      clue("p1 can ping p1 after connecting mediator to sequencer 3") {
        participant1.health.ping(participant1.id)
      }

      clue("turn off s1 and s2") {
        sequencer1.stop()
        sequencer2.stop()
      }

      clue("can not ping via s3 as the participant hasn't picked up the sequencer connection") {
        participant1.health.maybe_ping(participant1.id) shouldBe None
      }

      clue("can ping after reconnect") {
        // TODO(#13058) modified sequencer connection should have an immediate effect
        loggerFactory.assertLogsUnorderedOptional(
          {
            participant1.synchronizers.disconnect(daName)
            participant1.synchronizers.reconnect(daName)
          },
          expectedLogs *,
        )
      }

      clue("can not ping via s3") {
        participant1.health.maybe_ping(participant1.id) shouldBe defined
      }

    }
  }

}

class PostgresSequencerHighAvailabilityIntegrationTest
    extends SequencerHighAvailabilityIntegrationTest {

  registerPlugin(
    new UsePostgres(
      loggerFactory
    )
  )
  registerPlugin(
    UseSharedStorage.forSequencers(
      "sequencer1",
      Seq("sequencer2", "sequencer3", "sequencer4"),
      loggerFactory,
    )
  )
}
