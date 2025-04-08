// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.SequencerReference
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.toxiproxy.UseToxiproxy.ToxiproxyConfig
import com.digitalasset.canton.integration.plugins.toxiproxy.{ProxyConfig, UseToxiproxy}
import com.digitalasset.canton.integration.tests.toxiproxy.ToxiproxyHelpers
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  EnvironmentSetupPlugin,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.sequencing.SubmissionRequestAmplification
import com.digitalasset.canton.{SequencerAlias, config}
import eu.rekawek.toxiproxy.model.{Toxic, ToxicDirection, ToxicList}
import monocle.macros.syntax.lens.*
import org.scalatest.BeforeAndAfter

import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters.*

/** Template for Toxiproxy tests for various components, see implementation(s). While the plain old
  * HA is not supported, it leverages submission request amplification for overcoming faulty
  * sequencer nodes.
  */
abstract class ToxiproxyIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with BeforeAndAfter {

  private lazy val BongLevels: Int = 3
  private lazy val BongTimeout = 3.minutes
  private lazy val PingTimeout = config.NonNegativeDuration.ofMinutes(1)
  private lazy val SequencersCount = 3
  private lazy val MediatorsCount = 1

  private lazy val amplification = SubmissionRequestAmplification(
    factor = PositiveInt.tryCreate(SequencersCount),
    patience = config.NonNegativeFiniteDuration.Zero,
  )

  // need to be overwritten as `def` or `lazy val`, else null pointer exceptions are thrown due to scala initialization order
  protected def proxyConfs: Seq[ProxyConfig]
  protected def pluginsToRegister: Seq[
    EnvironmentSetupPlugin
  ]

  /** component to which the connection is disrupted */
  protected def component: String

  private val toxiproxyPlugin = UseToxiproxy(ToxiproxyConfig(proxyConfs))

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 2,
        numSequencers = SequencersCount,
        numMediators = MediatorsCount,
      )
      .withNetworkBootstrap { implicit env =>
        import env.*
        val testSequencers = sequencers.local.take(SequencersCount)
        val testMediators = mediators.local.take(MediatorsCount)
        val description = NetworkTopologyDescription(
          daName,
          synchronizerOwners = testSequencers,
          synchronizerThreshold = PositiveInt.one,
          sequencers = testSequencers,
          mediators = testMediators,
          mediatorRequestAmplification = amplification,
          overrideMediatorToSequencers =
            // Use a threshold of two to ensure that the mediator connects to all sequencers.
            // TODO(#19911) Reduce to one again once this can be configured independently.
            Some(testMediators.map(_ -> (testSequencers, PositiveInt.two)).toMap),
        )
        NetworkBootstrapper(Seq(description))
      }
      .addConfigTransforms(
        _.focus(_.parameters.timeouts.console.ping).replace(PingTimeout)
      )
      .withSetup { implicit env =>
        import env.*
        val connections = NonEmpty
          .from(sequencers.local.take(SequencersCount))
          .getOrElse(throw new IllegalArgumentException("Should not be empty"))
          .map(s => (SequencerAlias.tryCreate(s.name), s))
          .toMap
        participant1.synchronizers.connect_local_bft(
          connections,
          alias = daName,
          submissionRequestAmplification = amplification,
          // Use a threshold of two to ensure that the participant connects to all sequencers.
          // TODO(#19911) Reduce to one again once this can be configured independently.
          sequencerTrustThreshold = PositiveInt.two,
        )
        participant2.synchronizers.connect_local_bft(
          connections,
          alias = daName,
          submissionRequestAmplification = amplification,
          // Use a threshold of two to ensure that the participant connects to all sequencers.
          // TODO(#19911) Reduce to one again once this can be configured independently.
          sequencerTrustThreshold = PositiveInt.two,
        )

        // In the tests we have numerous requests that will be dropped while being sequenced.
        // So the participant needs to await the maxSequencingTimestamp before it can reject the request.
        // The application can only retry when the participant has rejected the request.
        //
        // The following setting will choose a short timeout for sequencing, so that the application does not have to wait too much.
        // However, the value needs to be sufficiently higher than the amplification patience so that amplified requests
        // do not fail on exceeding the max sequencing time.
        runOnAllInitializedSynchronizersForAllOwners((owner, synchronizer) =>
          owner.topology.synchronizer_parameters
            .set_ledger_time_record_time_tolerance(
              synchronizer.synchronizerId,
              PingTimeout.duration,
            )
        )
      }
      .withTeardown { _ =>
        ToxiproxyHelpers.removeAllProxies(
          toxiproxyPlugin.runningToxiproxy.controllingToxiproxyClient
        )
      }

  after {
    for (proxy <- toxiproxyPlugin.runningToxiproxy.controllingToxiproxyClient.getProxies.asScala) {
      for (toxic <- proxy.toxics().getAll.asScala) {
        toxic.remove()
      }
    }
  }

  // toxiproxy needs to be registered last
  pluginsToRegister.foreach(registerPlugin)
  registerPlugin(toxiproxyPlugin)

  "The sequencer should recover" when {
    def enableTimeout(
        client: ToxicList,
        toxicity: Double = 1.0,
        downstreamOnly: Boolean = false,
    ): Seq[Toxic] =
      enableToxics(
        client.timeout("timeout-upstream", ToxicDirection.UPSTREAM, 1),
        client.timeout("timeout-downstream", ToxicDirection.DOWNSTREAM, 1),
        toxicity,
        downstreamOnly,
      )

    def enableToxics(
        upstream: => Toxic,
        downstream: => Toxic,
        toxicity: Double,
        downstreamOnly: Boolean,
    ): Seq[Toxic] = {
      val toxics = if (downstreamOnly) Seq(downstream) else Seq(upstream, downstream)
      toxics.foreach(_.setToxicity(toxicity.toFloat))
      toxics
    }

    def enableZeroBandwidth(
        toxics: ToxicList,
        toxicity: Double = 1.0,
        downstreamOnly: Boolean = false,
    ): Seq[Toxic] = enableToxics(
      toxics.bandwidth("zero-bandwidth-upstream", ToxicDirection.UPSTREAM, 0),
      toxics.bandwidth("zero-bandwidth-downstream", ToxicDirection.DOWNSTREAM, 0),
      toxicity,
      downstreamOnly,
    )

    def testBongWithToxic(enableToxics: ToxicList => Seq[Toxic], recoverAfter: FiniteDuration)(
        implicit env: TestConsoleEnvironment
    ): Unit = {
      import env.*

      val time = participant1.health.ping(participant2)
      logger.debug(s"pinging with healthy connection takes time: $time")

      logger.debug("breaking the connection to the sequencers")
      val toxics = proxyConfs.flatMap { conf =>
        val name = conf.name
        val proxy = toxiproxyPlugin.runningToxiproxy.getProxy(name).value
        enableToxics(proxy.underlying.toxics())
      }

      logger.debug(s"starting a bong when $component is unusable")
      val bongF = Future {
        participant2.testing.bong(
          targets = Set(participant1.id, participant2.id),
          validators = Set(participant1.id),
          levels = BongLevels,
          timeout = BongTimeout + recoverAfter,
        )
      }

      logger.debug(
        s"waiting $recoverAfter before healing the connection to the targets"
      )
      Threading.sleep(recoverAfter.toMillis)

      logger.debug("healing the connection to the sequencers")
      toxics.foreach(_.remove())

      logger.debug("now the previously started bong should succeed")
      Await.result(bongF, BongTimeout)

      logger.debug("ping once more to make sure everything is alright")
      participant1.health.ping(participant2)
    }

    s"the connection to $component has timeouts" in { implicit env =>
      testBongWithToxic(enableTimeout(_), 10.seconds)
    }

    s"the connection to $component has zero bandwidth" in { implicit env =>
      testBongWithToxic(enableZeroBandwidth(_), 10.seconds)
    }

    // Choose a toxicity of less than 1.0. As a result, toxiproxy will let some messages go through.
    // Consequently, a retry at the sequencer will succeed after a few iterations, but then a different attempt to reach the backend will fail.
    // Overall, the failures will hit more code paths than in a test with toxicity of 1.0.
    s"the connection to $component has intermittent timeouts" in { implicit env =>
      testBongWithToxic(enableTimeout(_, 0.8), 60.seconds)
    }

    s"the connection to $component has intermittent zero bandwidth" in { implicit env =>
      testBongWithToxic(enableZeroBandwidth(_, 0.8), 60.seconds)
    }

    // Downstream failures are a bit tricky, because requests do get executed, but the client does not learn about it.
    // The client will normally retry, but data corruption will result if a retried request is not idempotent.
    s"the connection to $component has intermittent timeouts (downstream only)" in { implicit env =>
      testBongWithToxic(
        enableTimeout(_, 0.8, downstreamOnly = true),
        30.seconds,
      )
    }

    s"the connection to $component has intermittent zero bandwidth (downstream only)" in {
      implicit env =>
        testBongWithToxic(
          enableZeroBandwidth(_, 0.8, downstreamOnly = true),
          30.seconds,
        )
    }

    "failover to other sequencers" in { implicit env =>
      import env.*

      def toxicsForSequencer(sequencer: SequencerReference) =
        proxyConfs.filter(_.name.contains(sequencer.name)).map { conf =>
          val name = conf.name
          val proxy = toxiproxyPlugin.runningToxiproxy.getProxy(name).value
          proxy.underlying.disable()
          proxy
        }

      logger.debug("breaking the connection to the first sequencer")
      val toxicsForFirstSequencer = toxicsForSequencer(sequencer1)

      logger.debug(
        s"starting a ping when the first sequencer is unhealthy, but the second sequencer is healthy"
      )

      val time1 = participant1.health.ping(participant2)
      logger.debug(s"pinging takes time: $time1")

      logger.debug("healing the connection to the first sequencer")
      toxicsForFirstSequencer.foreach(_.underlying.enable())

      participant1.health.ping(participant2)

      logger.debug("breaking the connection to the second sequencer")
      val toxicsForSecondSequencer = toxicsForSequencer(sequencer2)
      logger.debug(
        s"starting a ping when the first sequencer is healthy, but the second sequencer is unhealthy"
      )

      val time2 = participant1.health.ping(participant2)
      logger.debug(s"pinging takes time: $time2")

      logger.debug("healing the connection to the second sequencer")
      toxicsForSecondSequencer.foreach(_.underlying.enable())

      logger.debug("stopping first sequencer so we use second sequencer for the last ping")
      sequencer1.stop()

      participant1.health.ping(participant2)

      nodes.local.stop()
    }
  }
}
