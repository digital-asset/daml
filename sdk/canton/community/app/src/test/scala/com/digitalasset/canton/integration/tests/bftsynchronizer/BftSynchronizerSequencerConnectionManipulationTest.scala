// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.bftsynchronizer

import com.daml.metrics.api.testing.MetricValues.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.LocalSequencerReference
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.sequencing.{
  SequencerConnectionPoolDelays,
  SequencerConnections,
  SubmissionRequestAmplification,
}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.{SequencerAlias, config}
import monocle.macros.syntax.lens.*

import scala.collection.immutable.Seq

sealed trait BftSynchronizerSequencerConnectionManipulationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S2M2.addConfigTransform(
      _.focus(_.parameters.timeouts.processing.sequencerInfo)
        .replace(config.NonNegativeDuration.ofSeconds(1))
    )

  "Basic synchronizer startup with 1 out of 2 sequencers threshold" in { implicit env =>
    import env.*

    // STEP 1: connect participants for the synchronizer via "their" sequencers
    clue("participant1 connects to sequencer1, sequencer2") {
      participant1.synchronizers.connect_local_bft(
        sequencers = Seq(sequencer1, sequencer2),
        synchronizerAlias = daName,
        sequencerTrustThreshold = PositiveInt.two,
      )
    }
    clue("participant2 connects to sequencer2, sequencer1") {
      participant2.synchronizers.connect_local_bft(
        sequencers = Seq(sequencer2, sequencer1),
        synchronizerAlias = daName,
        sequencerTrustThreshold = PositiveInt.two,
      )
    }

    // STEP 2: participants can now transact with each other
    participant1.health.ping(participant2.id)

    // STEP 3: participant uses the sequencer round-robin
    val sequencers = Seq(sequencer1, sequencer2)
    def getProcessedMessages(sequencer: LocalSequencerReference): Long = {
      val metric = sequencer.underlying.value.sequencer.metrics.publicApi.messagesProcessed
      // Explicitly mark the metric so that it exists even if the sequencer has not yet seen any submission requests.
      metric.mark()
      metric.value
    }

    val messagesProcessedBefore = sequencers.map(getProcessedMessages)
    val count = 20L
    for { _ <- 1 to count.toInt } {
      // We simply fetch a synchronizer time because this does not trigger additional messages
      // from other nodes (e.g., mediators) that could influence the distribution
      // We do this sequentially to ensure that the synchronizer time tracker really issues as many requests
      participant1.testing.fetch_synchronizer_time(
        daName,
        config.NonNegativeDuration.ofSeconds(60),
      )
    }
    val messagesProcessedAfter = sequencers.map(getProcessedMessages)
    val messagesProcessedBySequencer =
      messagesProcessedAfter.lazyZip(messagesProcessedBefore).map(_ - _)
    val messagesProcessed = messagesProcessedBySequencer.sum
    messagesProcessed should be >= count
    val expectedLoadPerSequencer = messagesProcessed / 2
    val tolerance = (0.3d * expectedLoadPerSequencer.toDouble).toLong
    sequencers.lazyZip(messagesProcessedBySequencer).foreach { (sequencerId, processed) =>
      clue(s"Sequencer $sequencerId processed") {
        processed should be >= (expectedLoadPerSequencer - tolerance)
        processed should be <= (expectedLoadPerSequencer + tolerance)
      }
    }

    // STEP 4: change sequencer connections in participant to rely only to a single sequencer connection
    // - This happens as the node will filter out the connections to the ones that are available
    //   when we connect.
    participant1.synchronizers.disconnect(daName)
    participant2.synchronizers.disconnect(daName)

    // TODO(#19911) Remove when we can configure the connection threshold independently from the trust threshold
    clue("lower the trust threshold to one") {
      Seq(participant1, participant2).foreach(
        _.synchronizers.modify(
          daName,
          _.focus(_.sequencerConnections).modify(old =>
            old.withSequencerTrustThreshold(sequencerTrustThreshold = PositiveInt.one).value
          ),
        )
      )
    }

    // stop both mediators to ensure that they don't attempt to reach the sequencer and emit warnings
    Seq(mediator1, mediator2).foreach(_.stop())
    sequencer2.stop()
    clue("restarting mediators after turning off sequencer2") {
      Seq(mediator1, mediator2).foreach(_.start())
    }

    clue("reconnecting participants while sequencer2 is offline") {
      participant1.synchronizers.reconnect(daName)
      participant2.synchronizers.reconnect(daName)
    }

    val pingTimeout = config.NonNegativeDuration.ofSeconds(40)

    clue("pinging works nicely again despite sequencer2 being offline") {
      participant1.health.maybe_ping(participant2.id, timeout = pingTimeout) shouldBe defined
    }

    // STEP 5: expect sequencer working after disconnect
    Seq(mediator1, mediator2).foreach(_.stop())
    participant1.synchronizers.disconnect(daName)
    participant2.synchronizers.disconnect(daName)

    sequencer1.stop()
    sequencer2.start()

    clue("reconnecting nodes after switching from p1 to p2") {
      Seq(mediator1, mediator2).foreach(_.start())
      participant1.synchronizers.reconnect(daName)
      participant2.synchronizers.reconnect(daName)
    }

    clue("pinging works again") {
      participant1.health.ping(participant2.id, timeout = pingTimeout)
    }

    sequencer1.start()
  }

  "Modification of sequencer connections triggers sequencers id fetching and storing" in {
    implicit env =>
      import env.*

      participant3.synchronizers.connect_local(sequencer1, daName)

      participant3.synchronizers
        .config(daName)
        .value
        .sequencerConnections
        .connections
        .forgetNE should have size 1

      participant3.synchronizers.modify(
        daName,
        _.copy(sequencerConnections =
          SequencerConnections.tryMany(
            Seq(sequencer1, sequencer2).map { sequencer =>
              // On purpose, we don't set the sequencer id. It should be grabbed automatically.
              sequencer.sequencerConnection
                .withAlias(SequencerAlias.tryCreate(sequencer.name))
            },
            sequencerTrustThreshold = PositiveInt.two,
            sequencerLivenessMargin = NonNegativeInt.zero,
            SubmissionRequestAmplification.NoAmplification,
            SequencerConnectionPoolDelays.default,
          )
        ),
      )

      // Check that ids are set
      val configs: Map[SequencerAlias, Option[SequencerId]] = participant3.synchronizers
        .config(daName)
        .value
        .sequencerConnections
        .aliasToConnection
        .view
        .mapValues(_.sequencerId)
        .toMap

      val expectedConfigs = Map(
        sequencer1.sequencerAlias -> Some(sequencer1.id),
        sequencer2.sequencerAlias -> Some(sequencer2.id),
      )

      configs shouldBe expectedConfigs
  }

}

class BftSynchronizerSequencerConnectionManipulationTestPostgres
    extends BftSynchronizerSequencerConnectionManipulationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory
    )
  )
}
