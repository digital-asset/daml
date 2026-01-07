// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy

import com.digitalasset.canton.admin.api.client.data.SynchronizerConnectionConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.toxiproxy.UseToxiproxy.ToxiproxyConfig
import com.digitalasset.canton.integration.plugins.toxiproxy.{
  ParticipantToSequencerPublicApi,
  RunningProxy,
  UseToxiproxy,
}
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.{BaseTest, SequencerAlias}

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*
import scala.util.Try

/** Currently tests the participant-synchronizer connection under various network conditions. The
  * test-setup used is:
  *   - Two participants, participant 1 and participant2 connected to a single synchronizer da
  *   - Participant1 connects to da via a proxy controlled by toxiproxy
  *   - Participant2 connects to da directly
  */
trait ToxiproxyParticipantSynchronizerBase extends BaseTest {

  val defaultParticipant: String = ToxiproxyHelpers.defaultParticipant

  def proxyConf: () => ParticipantToSequencerPublicApi

  val toxiProxy = new UseToxiproxy(ToxiproxyConfig(List(proxyConf())))

  def getProxy: RunningProxy = {
    val toxiproxy = toxiProxy.runningToxiproxy
    toxiproxy.getProxy(proxyConf().name).value
  }

}

object ToxiproxyParticipantSynchronizerHelpers extends BaseTest with EntitySyntax {

  /** Connect participant1 to the synchronizer da via a (toxi)proxy
    *
    * Connect participant2 to the synchronizer da directly
    */
  def connectParticipantsToDa(conf: ParticipantToSequencerPublicApi, toxiProxy: UseToxiproxy)(
      implicit env: TestConsoleEnvironment
  ): RunningProxy = {
    import env.*

    val toxiproxy = toxiProxy.runningToxiproxy
    val proxy = toxiproxy.getProxy(conf.name).value

    val synchronizerViaProxy =
      UseToxiproxy.generateSynchronizerConnectionConfig(
        SynchronizerConnectionConfig
          .tryGrpcSingleConnection(
            daName,
            SequencerAlias.tryCreate(conf.sequencer),
            "http://dummy.url",
          ),
        conf,
        toxiproxy,
      )

    val count = new AtomicInteger()
    eventually(20.seconds) {
      logger.info(
        s"Attempt ${count.getAndIncrement()}: Connecting p1 to synchronizer $daName via $synchronizerViaProxy"
      )
      val connection = Try(participant1.synchronizers.connect_by_config(synchronizerViaProxy))
      logger.info(s"Result of attempted connection : $connection")
      participant1.synchronizers.list_connected().map(d => d.synchronizerAlias) should contain(
        daName
      )
    }

    logger.info(s"Connecting p2 to synchronizer $daName")
    participant2.synchronizers.connect_local(sequencer1, alias = daName)

    participant1.health.ping(participant2.id)

    proxy
  }

  override val defaultParticipant: String = ToxiproxyHelpers.defaultParticipant
}
