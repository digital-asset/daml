// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.crashrecovery

import cats.syntax.option.*
import com.daml.metrics.api.testing.MetricValues
import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.admin.api.client.data.SynchronizerConnectionConfig
import com.digitalasset.canton.config.RequireTypes.{ExistingFile, PositiveInt}
import com.digitalasset.canton.config.{
  NonNegativeFiniteDuration as NonNegativeFiniteDurationConfig,
  PemFile,
  TlsBaseServerConfig,
}
import com.digitalasset.canton.console.LocalSequencerReference
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseSharedStorage}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransform,
  ConfigTransforms,
  EnvironmentDefinition,
  EnvironmentSetupPlugin,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality.Optional
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeConfig
import com.digitalasset.canton.util.BinaryFileUtil
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.*

// TODO(#16089): Currently this test cannot work due to the issue
class SequencerFailoverTestPostgres extends BaseDatabaseSequencerFailoverTest {
  override def createStoragePlugin(): EnvironmentSetupPlugin = new UsePostgres(loggerFactory)
}

abstract class BaseDatabaseSequencerFailoverTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with MetricValues {

  private val sequencer1Name = "sequencer1"
  private val sequencer2Name = "sequencer2"
  private val sequencer3Name = "sequencer3"

  private val certsPath = "community/app/src/test/resources/tls"

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 2,
        numSequencers = 3,
        numMediators = 1,
      )
      // add the new sequencer nodes before the default config transforms
      .clearConfigTransforms()
      .addConfigTransform(updateSequencerNodes)
      .addConfigTransform(ConfigTransforms.uniqueH2DatabaseNames)
      .addConfigTransforms(ConfigTransforms.defaults*)
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(EnvironmentDefinition.S1M1)
      }
      // TODO(#16089): Don't use the manual start if not necessary
      .withManualStart

  // must occur before shared storage plugin is registered
  protected def createStoragePlugin(): EnvironmentSetupPlugin
  registerPlugin(createStoragePlugin())

  registerPlugin(
    UseSharedStorage.forSequencers(
      sequencer1Name,
      Seq(sequencer2Name, sequencer3Name),
      loggerFactory,
    )
  )

  val updateSequencerNodes: ConfigTransform = config => {
    // most properties should be set in the underlying config file or updated by config transforms.
    // this will just update values specifically related to sequencer ha that we couldn't put in the examples
    // or where the defaults are suitable.
    def enableTls(sequencerName: String, commonName: String): Option[TlsBaseServerConfig] =
      TlsBaseServerConfig(
        certChainFile =
          PemFile(ExistingFile.tryCreate(s"$certsPath/$sequencerName-$commonName.crt")),
        privateKeyFile =
          PemFile(ExistingFile.tryCreate(s"$certsPath/$sequencerName-$commonName.pem")),
      ).some

    def updateSequencerConfig(
        name: String,
        config: SequencerNodeConfig,
    ): SequencerNodeConfig =
      config
        .focus(_.sequencer)
        .modify {
          case dbConfig @ SequencerConfig.Database(_, _, haConfig, _, _) =>
            dbConfig.copy(
              highAvailability = haConfig.map(
                _.copy(
                  keepAliveInterval = NonNegativeFiniteDurationConfig.ofMillis(100)
                )
              )
            )
          case other => fail(s"Unexpected sequencer configuration: $other")
        }
        .focus(_.publicApi)
        .modify(
          _.copy(
            address = "localhost",
            // we're going to setup TLS for each sequencer slightly differently
            tls = name match {
              // as the synchronizer alone uses this one don't enable TLS as that's not interesting there
              case "sequencer1" => None
              // the participants will load balance between sequencers 2&3.
              // use different hostnames/common-names for the certs to ensure that the GRPC channel is using the appropriate
              // service authority for each resolved address.
              case "sequencer2" => enableTls(name, "localhost")
              case "sequencer3" => enableTls(name, "127.0.0.1")
            },
          )
        )
        .focus(_.adminApi)
        .modify(_.copy("localhost"))

    ConfigTransforms.updateAllSequencerConfigs(updateSequencerConfig)(config)
  }

  // TODO(#16089): Re-enable this test
  "should be able to start and use many sequencer nodes sharing the same database" ignore {
    implicit env =>
      import env.*

      // TODO(#16089): Don't use the manual start if not necessary

      loggerFactory.assertLogsUnorderedOptional(
        {
          // start the mediator
          mediator1.start()
          // start all the sequencers at once
          // when we initialize sequencer1 the others will notice and fully start their sequencer node instance
          sequencer1.start()
          sequencer2.start()
          sequencer3.start()

          val synchronizerName = daName.unwrap
          // architecture-handbook-entry-begin: SequencerHAOnboarding
          // bootstrap the synchronizer with just one of the sequencers as they're just replicas sharing a database
          bootstrap.synchronizer(
            synchronizerName,
            sequencers = Seq(sequencer1),
            synchronizerOwners = Seq(sequencer1),
            synchronizerThreshold = PositiveInt.one,
            mediators = Seq(mediator1),
            staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
          )
          // architecture-handbook-entry-end: SequencerHAOnboarding

          // wait for the synchronizer to think it's initialized first
          Seq[LocalSequencerReference](sequencer2, sequencer3).foreach(
            _.health.wait_for_initialized()
          )
        },
        // this is pretty clunky: if the mediator hits a sequencer during/while it's initializing itself it may not yet
        // have visibility of the registered mediator key. The auth logic will wait ~100ms and try again which will very
        // likely then be fine, but the first attempt is currently logged at warning level by the signature verification
        // in the sequencer.
        (
          Optional,
          _.warningMessage should (include("provided invalid signature") and include(
            "Valid keys are List()"
          )),
        ),
      )

      // start participants and throw them at the load balanced sequencers (which should round-robin requests between them)
      participants.local.start()

      // use slightly different addresses to ensure that the cert checks are working appropriately
      val sequencer2Url = s"https://localhost:${sequencer2.config.publicApi.port.unwrap}"
      val sequencer3Url = s"https://127.0.0.1:${sequencer3.config.publicApi.port.unwrap}"

      // connect the participants to sequencer2&3 (we'll later take down sequencer2)
      val sAlias = SequencerAlias.Default
      val synchronizerConnectionConfig = SynchronizerConnectionConfig
        .tryGrpcSingleConnection(
          synchronizerAlias = daName,
          sequencerAlias = sequencer2.name,
          connection = sequencer2Url, // we'll add sequencer3 later
          // use custom trusted ca certificates as the servers certs are self signed
          certificates = BinaryFileUtil
            .readByteStringFromFile(s"$certsPath/root-ca.crt")
            .valueOrFail("root-ca.crt")
            .some,
        )
        .addEndpoints(sAlias, sequencer3Url)
        .value

      participant1.synchronizers.connect_by_config(synchronizerConnectionConfig)
      participant2.synchronizers.connect_by_config(synchronizerConnectionConfig)

      // do a bunch of stuff
      participant1.testing.bong(Set(participant1), levels = 5, timeout = 1.minute)

      val metrics =
        Seq(sequencer1, sequencer2, sequencer3).map(_.underlying.value.sequencer.metrics)

      // currently just log the metrics so they're visible on logs
      def logMetrics(name: String, getter: SequencerMetrics => Any): Unit = {
        val valuesText = metrics.zipWithIndex
          .map { case (metrics, index) =>
            s"sequencer${index + 1} = ${getter(metrics)}"
          }
          .mkString(",")
        logger.info(s"$name: $valuesText")
      }

      logMetrics("messages processed", _.publicApi.messagesProcessed.value)
      logMetrics("current subscriptions", _.publicApi.subscriptionsGauge.getValue)
  }

  // TODO(#16089): Re-enable this test
  "synchronizer should continue working after one sequencer falls over" ignore { implicit env =>
    import env.*

    logger.info("Stopping sequencer2")
    sequencer2.stop()

    // do another bong - should now run against sequencer3
    participant1.testing.bong(Set(participant1), levels = 5, timeout = 1.minute)
  }
}
