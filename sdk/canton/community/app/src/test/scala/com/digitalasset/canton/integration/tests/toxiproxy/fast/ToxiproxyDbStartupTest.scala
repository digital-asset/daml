// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy.fast

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.StorageConfig.Memory
import com.digitalasset.canton.config.{ModifiableDbConfig, StorageConfig}
import com.digitalasset.canton.console.{CommandFailure, LocalInstanceReference}
import com.digitalasset.canton.integration.ConfigTransforms.modifyAllStorageConfigs
import com.digitalasset.canton.integration.plugins.toxiproxy.*
import com.digitalasset.canton.integration.plugins.toxiproxy.UseToxiproxy.ToxiproxyConfig
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.toxiproxy.ToxiproxyHelpers
import com.digitalasset.canton.integration.tests.toxiproxy.fast.ToxiproxyDbStartupTest.noFailFastForNode
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransform,
  EnvironmentDefinition,
  EnvironmentSetupPlugin,
  SharedEnvironment,
}
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

/** Tests for starting up Canton nodes in the presence of network failures between the node and the
  * database.
  */
abstract class ToxiproxyDbStartupTest extends CommunityIntegrationTest with SharedEnvironment {

  def proxyConf: ProxyConfig
  def storagePlugin: EnvironmentSetupPlugin

  def runOnStart(toxiproxy: RunningToxiproxy): Unit = {
    val proxy = toxiproxy.getProxy(proxyConf.name).value
    proxy.underlying.disable()
  }

  val toxiproxyPlugin = new UseToxiproxy(ToxiproxyConfig(List(proxyConf), runOnStart))

  registerPlugin(storagePlugin)
  registerPlugin(toxiproxyPlugin)

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S1M1_Manual.withManualStart.withTeardown { _ =>
      ToxiproxyHelpers.removeAllProxies(
        toxiproxyPlugin.runningToxiproxy.controllingToxiproxyClient
      )
    }

  def fastStartupTimeLimit: FiniteDuration = 15.seconds

  def sequencerStartupFailsFast(): Unit =
    "the sequencer-db connection" when {
      "down" should {
        "cause fast failure on startup" in { implicit env =>
          import env.*

          val start = Deadline.now

          // synchronizer should fail to come up because the database is down
          a[CommandFailure] should be thrownBy sequencer1.start()

          val time = Deadline.now - start
          time should be < fastStartupTimeLimit

        }
      }
    }

  def sequencerStartupSurviveDatabaseOutage(): Unit =
    "the sequencer-db connection" when {
      "down" should {
        "wait for the database to come up" in { implicit env =>
          import env.*

          participant1.start()
          participant2.start()

          val startF = Future {
            sequencer1.start()
            mediator1.start()
            bootstrap.synchronizer(
              daName.unwrap,
              sequencers = Seq(sequencer1),
              mediators = Seq(mediator1),
              synchronizerOwners = Seq(sequencer1),
              synchronizerThreshold = PositiveInt.one,
              staticSynchronizerParameters =
                EnvironmentDefinition.defaultStaticSynchronizerParameters,
            )
            Seq[LocalInstanceReference](sequencer1, mediator1).foreach(
              _.topology.synchronisation.await_idle()
            )
          }

          always(fastStartupTimeLimit)(assert(!startF.isCompleted))

          val proxy = toxiproxyPlugin.runningToxiproxy.getProxy(proxyConf.name).value
          proxy.underlying.enable()

          val () = Await.result(startF, fastStartupTimeLimit)

          participant1.synchronizers.connect_local(sequencer1, alias = daName)
          participant2.synchronizers.connect_local(sequencer1, alias = daName)

          eventually() {
            participant1.health.maybe_ping(participant2) shouldBe defined
          }
        }
      }
    }

  def mediatorStartupFailsFast(): Unit =
    "the mediator-db connection" when {
      "down" should {
        "cause fast failure on startup" in { implicit env =>
          import env.*

          val start = Deadline.now

          // synchronizer should fail to come up because the database is down
          a[CommandFailure] should be thrownBy mediator1.start()

          val time = Deadline.now - start
          time should be < fastStartupTimeLimit

        }
      }
    }

  def mediatorStartupSurviveDatabaseOutage(): Unit =
    "the mediator-db connection" when {
      "down" should {
        "wait for the database to come up" in { implicit env =>
          import env.*

          participant1.start()
          participant2.start()

          val startF = Future {
            sequencer1.start()
            mediator1.start()
            bootstrap.synchronizer(
              daName.unwrap,
              sequencers = Seq(sequencer1),
              mediators = Seq(mediator1),
              synchronizerOwners = Seq(sequencer1),
              synchronizerThreshold = PositiveInt.one,
              staticSynchronizerParameters =
                EnvironmentDefinition.defaultStaticSynchronizerParameters,
            )
            Seq[LocalInstanceReference](sequencer1, mediator1).foreach(
              _.topology.synchronisation.await_idle()
            )
          }

          always(fastStartupTimeLimit) {
            assert(!startF.isCompleted)
          }

          val proxy = toxiproxyPlugin.runningToxiproxy.getProxy(proxyConf.name).value
          proxy.underlying.enable()

          val () = Await.result(startF, fastStartupTimeLimit)

          participant1.synchronizers.connect_local(sequencer1, alias = daName)
          participant2.synchronizers.connect_local(sequencer1, alias = daName)

          eventually() {
            participant1.health.maybe_ping(participant2) shouldBe defined
          }
        }
      }
    }

  def participantStartupFailsFast(): Unit =
    "the participant-db connection" when {
      "down" should {
        "cause fast failure on startup" in { implicit env =>
          import env.*

          participant2.start()

          val start = Deadline.now

          // synchronizer should fail to come up because the database is down
          a[CommandFailure] should be thrownBy {
            participant1.start()
          }

          val time = Deadline.now - start
          time should be < fastStartupTimeLimit

        }
      }
    }

  def participantStartupSurviveDatabaseOutage(): Unit =
    // The system should fail fast when starting whilst the db connection is down -- it shouldn't hang forever.
    "the participant-db connection" when {
      "down" should {
        "wait for the database to come up" in { implicit env =>
          import env.*

          participant2.start()

          val startF = Future {
            sequencer1.start()
            mediator1.start()
            bootstrap.synchronizer(
              daName.unwrap,
              sequencers = Seq(sequencer1),
              mediators = Seq(mediator1),
              synchronizerOwners = Seq(sequencer1),
              synchronizerThreshold = PositiveInt.one,
              staticSynchronizerParameters =
                EnvironmentDefinition.defaultStaticSynchronizerParameters,
            )
            participant1.start()
          }

          always(fastStartupTimeLimit)(assert(!startF.isCompleted))

          val proxy = toxiproxyPlugin.runningToxiproxy.getProxy(proxyConf.name).value
          proxy.underlying.enable()

          val () = Await.result(startF, fastStartupTimeLimit)

          participant1.synchronizers.connect_local(sequencer1, alias = daName)
          participant2.synchronizers.connect_local(sequencer1, alias = daName)

          eventually() {
            participant1.health.maybe_ping(participant2) shouldBe defined
          }
        }
      }
    }

}
abstract class ToxiproxySequencerStartupTestPostgres extends ToxiproxyDbStartupTest {
  override def storagePlugin: EnvironmentSetupPlugin =
    new UsePostgres(loggerFactory)

  registerPlugin(new UseBftSequencer(loggerFactory))

  def proxyConf: ProxyConfig =
    SequencerToPostgres("sequencer-to-postgres-startup", "sequencer1", dbTimeout = 1000L)
}

class ToxiproxySequencerStartupFailsFastPostgres extends ToxiproxySequencerStartupTestPostgres {
  sequencerStartupFailsFast()
}

class ToxiproxySequencerStartupWaitsPostgres extends ToxiproxySequencerStartupTestPostgres {
  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.addConfigTransform(noFailFastForNode("sequencer1"))

  sequencerStartupSurviveDatabaseOutage()
}

abstract class ToxiproxyMediatorStartupTestPostgres extends ToxiproxyDbStartupTest {
  override def storagePlugin: EnvironmentSetupPlugin =
    new UsePostgres(loggerFactory)

  registerPlugin(new UseBftSequencer(loggerFactory))

  def proxyConf: ProxyConfig =
    MediatorToPostgres("mediator-to-postgres-startup", "mediator1", dbTimeout = 1000L)
}

class ToxiproxyMediatorStartupFailsFastPostgres extends ToxiproxyMediatorStartupTestPostgres {
  mediatorStartupFailsFast()
}

class ToxiproxyMediatorStartupWaitsPostgres extends ToxiproxyMediatorStartupTestPostgres {
  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.addConfigTransform(noFailFastForNode("mediator1"))

  mediatorStartupSurviveDatabaseOutage()
}

abstract class ToxiproxyParticipantStartupTestPostgres extends ToxiproxyDbStartupTest {
  override def storagePlugin: EnvironmentSetupPlugin =
    new UsePostgres(loggerFactory)

  registerPlugin(new UseBftSequencer(loggerFactory))

  def proxyConf: ProxyConfig =
    ParticipantToPostgres("Participant-to-postgres-startup", "participant1")
}

class ToxiproxyParticipantStartupFailsFastPostgres extends ToxiproxyParticipantStartupTestPostgres {
  participantStartupFailsFast()
}

class ToxiproxyParticipantStartupWaitsPostgres extends ToxiproxyParticipantStartupTestPostgres {

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.addConfigTransform(noFailFastForNode("participant1"))

  participantStartupSurviveDatabaseOutage()
}

object ToxiproxyDbStartupTest {
  private val noFailFast: StorageConfig => StorageConfig = {
    case dbConfig: ModifiableDbConfig[?] =>
      dbConfig.modify(parameters = dbConfig.parameters.focus(_.failFastOnStartup).replace(false))
    case memory: Memory =>
      memory.copy(parameters = memory.parameters.focus(_.failFastOnStartup).replace(false))
  }

  def noFailFastForNode(nodeName: String): ConfigTransform = modifyAllStorageConfigs {
    case (_, name, config) if name == nodeName =>
      noFailFast(config)
    case (_, _, config) =>
      config
  }
}
