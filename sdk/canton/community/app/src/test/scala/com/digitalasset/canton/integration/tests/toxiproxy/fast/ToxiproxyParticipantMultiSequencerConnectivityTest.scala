// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy.fast

import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.admin.api.client.data.{
  SequencerConnectionPoolDelays,
  SequencerConnectionValidation,
  SequencerConnections,
  SubmissionRequestAmplification,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{PositiveDurationSeconds, SynchronizerTimeTrackerConfig}
import com.digitalasset.canton.integration.ConfigTransforms.heavyTestDefaults
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.toxiproxy.{
  ParticipantToSequencerPublicApi,
  UseToxiproxy,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.toxiproxy.{
  ToxiproxyHelpers,
  ToxiproxyParticipantSynchronizerBase,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  IsolatedEnvironments,
}
import eu.rekawek.toxiproxy.model.ToxicDirection

/** This test verifies that a participant can tolerate a sequencer being down or unreachable and can
  * still connect to a synchronizer as long as SequencerConnectionValidation.Active is specified
  * rather than All.
  */
class ToxiproxyParticipantMultiSequencerConnectivityTest
    extends CommunityIntegrationTest
    with IsolatedEnvironments
    with ToxiproxyParticipantSynchronizerBase {

  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(toxiProxy)

  override def proxyConf: () => ParticipantToSequencerPublicApi =
    () =>
      ParticipantToSequencerPublicApi(
        sequencer = "sequencer1",
        name = "participant1-to-sequencer1-cannot-recover",
      )

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 2,
        numSequencers = 4,
        numMediators = 1,
      )
      .withNetworkBootstrap { implicit env =>
        import env.*
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            // single synchronizer owner avoids synchronizing topology when updating synchronizer parameters
            synchronizerOwners = Seq(sequencer1),
            synchronizerThreshold = PositiveInt.one,
            // TODO(#19911): Make the threshold configurable, until then:
            //  deploy 4 sequencers as we need 2*threshold+1 sequencers to connect (for threshold=1)
            sequencers = Seq(sequencer1, sequencer2, sequencer3, sequencer4),
            mediators = Seq(mediator1),
          )
        )
      }
      .clearConfigTransforms() // Clear the config transforms as the default config transforms reduce timeouts and therefore cause flakes
      .addConfigTransforms(heavyTestDefaults*)
      .withSetup { implicit env =>
        env.runOnAllInitializedSynchronizersForAllOwners(
          (owner, synchronizer) =>
            owner.topology.synchronizer_parameters
              .propose_update(
                synchronizer.synchronizerId,
                _.update(reconciliationInterval = PositiveDurationSeconds.ofSeconds(1)),
              ),
          topologyAwaitIdle = false,
        )
      }
      .withTeardown(_ =>
        ToxiproxyHelpers.removeAllProxies(toxiProxy.runningToxiproxy.controllingToxiproxyClient)
      )

  "The participant synchronizer connection" should {

    "succeed connecting to a synchronizer" when {
      "one of the sequencers is unreachable" in { implicit env =>
        import env.*
        val toxiproxy = toxiProxy.runningToxiproxy
        val proxy = getProxy
        val toxic = proxy.underlying
          .toxics()
          .timeout("upstream-pause-indefinite", ToxicDirection.UPSTREAM, 0)

        val sequencer1ToxiproxyConnection = UseToxiproxy
          .generateSynchronizerConnectionConfig(
            SynchronizerConnectionConfig
              .tryGrpcSingleConnection(daName, sequencer1.name, "http://dummy.url"),
            proxyConf(),
            toxiproxy,
          )
          .sequencerConnections
          .connections
          .head
        val sequencerConnections = SequencerConnections.tryMany(
          connections = Seq(
            sequencer1ToxiproxyConnection.withAlias(SequencerAlias.tryCreate("seq1")),
            sequencer2.sequencerConnection.withAlias(SequencerAlias.tryCreate("seq2")),
            sequencer3.sequencerConnection.withAlias(SequencerAlias.tryCreate("seq3")),
            sequencer4.sequencerConnection.withAlias(SequencerAlias.tryCreate("seq4")),
          ),
          sequencerTrustThreshold = PositiveInt.one,
          sequencerLivenessMargin = NonNegativeInt.zero,
          submissionRequestAmplification = SubmissionRequestAmplification.NoAmplification,
          sequencerConnectionPoolDelays = SequencerConnectionPoolDelays.default,
        )

        participant1.synchronizers.connect_by_config(
          SynchronizerConnectionConfig(
            synchronizerAlias = daName,
            sequencerConnections = sequencerConnections,
            manualConnect = false,
            synchronizerId = None,
            priority = 0,
            initialRetryDelay = None,
            maxRetryDelay = None,
            SynchronizerTimeTrackerConfig(),
          ),
          validation = SequencerConnectionValidation.Active,
          synchronize = Some(env.commandTimeouts.bounded),
        )

        logger.info("Managed to connect participant1 in spite of sequencer1 not being reachable")

        // By this point the main purpose of the test has succeeded, but let's start
        // participant2 and do a ping.
        participant2.synchronizers.connect_local_bft(
          sequencers = Seq(sequencer1, sequencer2, sequencer3, sequencer4),
          synchronizerAlias = daName,
        )
        participant1.health.ping(participant2)

        logger.info("Clear toxicity")
        toxic.setToxicity(0f)

        // Restore access to sequencer1.
        logger.info("disconnect participant1 from the synchronizer")
        participant1.synchronizers.disconnect_all()

        logger.info("Add sequencer1 bypassing removed toxic")
        participant1.synchronizers.modify(
          daName,
          // This time around demand that all sequencers be up
          // In addition to specifying SequencerConnectionValidation.All, increase the
          // threshold to 3 so that the reconnect also waits to connect to all sequencers.
          synchronizerConnectionConfig => {
            val sc = synchronizerConnectionConfig.sequencerConnections
            synchronizerConnectionConfig.copy(sequencerConnections =
              SequencerConnections
                .many(
                  sc.connections,
                  // TODO(#19911) Reduce this to one once the desired number of connections can be configured separately
                  sequencerTrustThreshold =
                    if (participant1.config.sequencerClient.useNewConnectionPool) {
                      // The connection pool obtains (trustThreshold + livenessMargin) subscriptions, so we need 4 to ensure sequencer1 is among them
                      PositiveInt.tryCreate(4)
                    } else PositiveInt.three,
                  sequencerLivenessMargin = NonNegativeInt.zero,
                  submissionRequestAmplification = sc.submissionRequestAmplification,
                  sequencerConnectionPoolDelays = sc.sequencerConnectionPoolDelays,
                )
                .getOrElse(fail("Must succeed increasing threshold to two with 4 sequencers"))
            )
          },
          SequencerConnectionValidation.All,
        )

        logger.info("reconnect participant1 to the synchronizer and run a ping")
        participant1.synchronizers.reconnect_all()
        participant1.health.ping(participant2)

        logger.info("Check that now participant1 is connected to sequencer1")
        eventually() {
          val participantsConnectedToSequencer1 =
            sequencer1.health.status.trySuccess.connectedParticipants
          participantsConnectedToSequencer1 should contain(participant1.id)
        }

        logger.info("Remove toxic to avoid log noise due to slow shutdown")
        toxic.remove()
        logger.info("Removed toxic")
      }
    }
  }
}
