// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy.fast

import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.admin.api.client.data.SynchronizerConnectionConfig
import com.digitalasset.canton.integration.EnvironmentDefinition.P2_S1M1
import com.digitalasset.canton.integration.plugins.toxiproxy.UseToxiproxy.ToxiproxyConfig
import com.digitalasset.canton.integration.plugins.toxiproxy.{
  ParticipantToSequencerPublicApi,
  UseToxiproxy,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import eu.rekawek.toxiproxy.model.ToxicDirection

class ToxiproxyPluginTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasExecutionContext
    with EntitySyntax {
  override lazy val environmentDefinition: EnvironmentDefinition =
    P2_S1M1

  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  private val p1ProxyConf =
    ParticipantToSequencerPublicApi("sequencer1", name = "participant1-to-sequencer1")
  private val toxiProxy = new UseToxiproxy(ToxiproxyConfig(List(p1ProxyConf)))
  registerPlugin(toxiProxy)

  override val defaultParticipant: String = "participant1"

  "The Toxiproxy plugin should" should {
    "be able to add latency to a connection" in { implicit env =>
      import env.*

      val toxiproxy = toxiProxy.runningToxiproxy
      val proxy = toxiproxy.getProxy(p1ProxyConf.name).value

      participant2.start()
      participant1.start()
      sequencer1.start()
      mediator1.start()

      val synchronizerViaProxy =
        UseToxiproxy.generateSynchronizerConnectionConfig(
          SynchronizerConnectionConfig
            .tryGrpcSingleConnection(daName, sequencer1.name, "http://dummy.url"),
          p1ProxyConf,
          toxiproxy,
        )

      participant1.synchronizers.connect_by_config(synchronizerViaProxy)
      participant2.synchronizers.connect_local(sequencer1, daName)

      // Warm up
      participant1.health.ping(participant2.id)

      val initialTime = participant1.health.ping(participant2.id)
      logger.info(s"Initially, p1 pings p2 in $initialTime.")

      val latencyMillis = 2000L

      proxy.underlying
        .toxics()
        .latency("upstream-latency", ToxicDirection.UPSTREAM, latencyMillis)
      try {

        val finalTime = participant1.health.ping(participant2.id)
        logger.info(s"With ${latencyMillis / 1000} seconds of latency, p1 pings p2 in $finalTime.")

        val delta = (finalTime - initialTime).toMillis

        // delta should be about 3 * latencyMillis
        assert(delta > 2 * latencyMillis, "added latency should not be too low")
        assert(delta < 6 * latencyMillis, "added latency should not be too high")
      } finally {
        proxy.underlying.delete()

        import scala.jdk.CollectionConverters.*
        val client = toxiProxy.runningToxiproxy.controllingToxiproxyClient
        eventually() {
          val proxies = client.getProxies.asScala.toList.map(p => p.getName)
          proxies shouldBe List.empty
        }

      }
    }
  }
}
