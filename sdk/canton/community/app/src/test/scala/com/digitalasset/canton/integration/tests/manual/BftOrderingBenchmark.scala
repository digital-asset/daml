// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual

import com.digitalasset.canton.config.RequireTypes.{Port, PositiveInt}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.toxiproxy.UseToxiproxy.ToxiproxyConfig
import com.digitalasset.canton.integration.plugins.toxiproxy.{
  BftSequencerPeerToPeer,
  ProxyConfig,
  SequencerToPostgres,
  UseToxiproxy,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.bftsequencer.AwaitsBftSequencerAuthenticationDisseminationQuorum
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.{
  DefaultConsensusEmptyBlockCreationTimeout,
  DefaultMaxBatchCreationInterval,
  DefaultMaxBatchesPerProposal,
  DefaultMaxRequestsInBatch,
  DefaultMinRequestsInBatch,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochLength
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance.dabft.DaBftBindingFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance.{
  BftBenchmarkConfig,
  BftBenchmarkTool,
}
import com.digitalasset.canton.tracing.TracingConfig
import eu.rekawek.toxiproxy
import eu.rekawek.toxiproxy.model.ToxicDirection
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}

/** An example script to use this on canton-testing machines follows:
  *
  * #!/usr/bin/env bash
  *
  * export LOG_LEVEL_CANTON=WARN
  *
  * export SBT_OPTS="\
  * -Xmx60G -Xms60G \
  * -Dbft-ordering-benchmark.num-nodes=16 \
  * -Dbft-ordering-benchmark.enable-prometheus-metrics=true \
  * -Dscala.concurrent.context.numThreads=30 \
  * -Dbft-ordering-benchmark.num-db-connections-per-node=5 \
  * -Dbft-ordering-benchmark.request-bytes=20000 \
  * -Dbft-ordering-benchmark.benchmark-duration=1minute \ "
  *
  * export CI=1 # When this defined, it ensures no dockerized Postgres is being used
  *
  * export POSTGRES_HOST=db-testing.da-int.net export POSTGRES_USER=*
  *
  * export POSTGRES_PASSWORD=*
  *
  * export POSTGRES_DB=postgres
  *
  * sbt "dumpClassPath; testOnly
  * com.digitalasset.canton.integration.tests.manual.BftOrderingBenchmark"
  */
@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
class BftOrderingBenchmark
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AwaitsBftSequencerAuthenticationDisseminationQuorum {

  private val BFTOrderingBenchmarkPrefix = "bft-ordering-benchmark"
  private val PostgresProxyNameSuffix = "postgres"

  private val numberOfSequencers: Int =
    Option(System.getProperty(s"$BFTOrderingBenchmarkPrefix.num-nodes"))
      .map(_.toInt)
      .getOrElse(4)

  // Use a bigger epoch length by default for better performance
  private val epochLength: EpochLength =
    Option(System.getProperty(s"$BFTOrderingBenchmarkPrefix.epoch-length"))
      .map(len => EpochLength(len.toLong))
      .getOrElse(EpochLength(512L))

  private val consensusEmptyBlockCreationTimeout: FiniteDuration =
    Option(System.getProperty(s"$BFTOrderingBenchmarkPrefix.empty-block-timeout"))
      .map(Duration(_).asInstanceOf[FiniteDuration])
      .getOrElse(DefaultConsensusEmptyBlockCreationTimeout)

  private val maxRequestsInBatch: Short =
    Option(System.getProperty(s"$BFTOrderingBenchmarkPrefix.max-requests-in-batch"))
      .map(_.toShort)
      .getOrElse(DefaultMaxRequestsInBatch)

  private val minRequestsInBatch: Short =
    Option(System.getProperty(s"$BFTOrderingBenchmarkPrefix.min-requests-in-batch"))
      .map(_.toShort)
      .getOrElse(DefaultMinRequestsInBatch)

  private val maxBatchCreationInterval: FiniteDuration =
    Option(System.getProperty(s"$BFTOrderingBenchmarkPrefix.max-batch-creation-interval"))
      .map(Duration(_).asInstanceOf[FiniteDuration])
      .getOrElse(DefaultMaxBatchCreationInterval)

  private val maxBatchesPerBlockProposal: Short =
    Option(System.getProperty(s"$BFTOrderingBenchmarkPrefix.max-batches-per-block-proposal"))
      .map(_.toShort)
      .getOrElse(DefaultMaxBatchesPerProposal)

  private val dedicatedExecutionContextDivisor: Option[Int] =
    Option(
      System.getProperty(s"$BFTOrderingBenchmarkPrefix.dedicated-execution-context-divisor")
    )
      .map(_.toInt)

  private val useInMemoryStorageForBftOrderer: Boolean =
    Option(System.getProperty(s"$BFTOrderingBenchmarkPrefix.use-in-memory-storage"))
      .exists(_.toBoolean)

  private val numberOfDbConnectionsPerNode: PositiveInt =
    PositiveInt.tryCreate(
      Option(System.getProperty(s"$BFTOrderingBenchmarkPrefix.num-db-connections-per-node"))
        .map(_.toInt)
        .getOrElse(5)
    )

  /** Tracing options. Disabled by default. */
  private val (tracingEnabled, tracingReportingPort, tracingSamplerRatio) =
    (
      Option(System.getProperty(s"$BFTOrderingBenchmarkPrefix.enable-tracing"))
        .exists(_.toBoolean),
      Option(System.getProperty(s"$BFTOrderingBenchmarkPrefix.tracing-reporting-port"))
        .map(_.toInt)
        .getOrElse(4317),
      Option(System.getProperty(s"$BFTOrderingBenchmarkPrefix.tracing-sampler-ratio"))
        .map(_.toDouble)
        .getOrElse(0.5),
    )

  private val requestBytes =
    Option(System.getProperty(s"$BFTOrderingBenchmarkPrefix.request-bytes"))
      .map(_.toInt)
      .getOrElse(256)

  private val runDuration =
    Option(
      System.getProperty(s"$BFTOrderingBenchmarkPrefix.benchmark-duration")
    )
      .map(Duration(_))
      .getOrElse(30 seconds)

  private val perNodeWritePeriod =
    Option(
      System.getProperty(s"$BFTOrderingBenchmarkPrefix.per-node-write-period")
    )
      .map(Duration(_))
      .getOrElse(100 milliseconds)

  private val reportingIntervalOpt =
    Option(
      System.getProperty(
        s"$BFTOrderingBenchmarkPrefix.reporting-interval"
      )
    )
      .map(Duration(_))

  /** A simulated network latency between sequencers. Disabled if [[None]]. */
  private val sequencerToSequencerLatencyMillis: Option[Long] =
    Option(
      System.getProperty(s"$BFTOrderingBenchmarkPrefix.sequencer-to-sequencer-latency-millis")
    ).map(_.toLong)

  /** A simulated network latency between sequencers and their databases. Disabled if [[None]]. */
  private val sequencerDbLatencyMillis: Option[Long] =
    Option(System.getProperty(s"$BFTOrderingBenchmarkPrefix.sequencer-db-latency-millis"))
      .map(_.toLong)

  registerPlugin(
    new UsePostgres(
      loggerFactory,
      customDbNames = Some((identity, "_bft_ordering_benchmark")),
      customMaxConnectionsByNode = Some(_ => Some(numberOfDbConnectionsPerNode)),
    )
  )

  private val bftSequencerPlugin =
    new UseBftSequencer(
      loggerFactory,
      shouldOverwriteStoredEndpoints = true,
      shouldUseMemoryStorageForBftOrderer = useInMemoryStorageForBftOrderer,
      shouldBenchmarkBftSequencer = true,
      standaloneOrderingNodes = true,
      epochLength = epochLength,
      consensusEmptyBlockCreationTimeout = consensusEmptyBlockCreationTimeout,
      maxRequestsInBatch = maxRequestsInBatch,
      minRequestsInBatch = minRequestsInBatch,
      maxBatchCreationInterval = maxBatchCreationInterval,
      maxBatchesPerBlockProposal = maxBatchesPerBlockProposal,
      dedicatedExecutionContextDivisor = dedicatedExecutionContextDivisor,
    )
  registerPlugin(bftSequencerPlugin)

  // We'll bootstrap one synchronizer with only one mediator (which is the minimum). We do need an environment where
  // the BFT orderer gets initialized, but we don't need it to handle submissions (the standalone mode will take care
  // of submissions).
  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 0,
        numSequencers = numberOfSequencers,
        numMediators = 1,
      )
      .clearConfigTransforms() // to disable globally unique ports
      .addConfigTransforms(
        ReplayTestCommon.configTransforms(
          prometheusHttpServerPort = Port.tryCreate(19091),
          prefix = BFTOrderingBenchmarkPrefix,
        )*
      )
      .addConfigTransforms(
        _.focus(_.monitoring.tracing.tracer).replace(
          TracingConfig.Tracer(
            exporter =
              if (tracingEnabled) TracingConfig.Exporter.Otlp(port = tracingReportingPort)
              else TracingConfig.Exporter.Disabled,
            sampler = TracingConfig.Sampler.TraceIdRatio(ratio = tracingSamplerRatio),
          )
        )
      )
      .withNetworkBootstrap { implicit env =>
        import env.*

        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = sequencers.all,
            synchronizerThreshold = PositiveInt.one,
            sequencers = sequencers.all,
            mediators = mediators.all,
            mediatorThreshold = PositiveInt.one,
          )
        )
      }

  val toxiProxyPlugin: Option[UseToxiproxy] =
    if (sequencerToSequencerLatencyMillis.isDefined || sequencerDbLatencyMillis.isDefined) {
      Some({
        val sequencerToPostgresProxyConfigs: Seq[ProxyConfig] =
          (1 to numberOfSequencers).map { sequencerIndex =>
            SequencerToPostgres(
              s"sequencer$sequencerIndex-to-$PostgresProxyNameSuffix",
              s"sequencer$sequencerIndex",
            )
          }
        val sequencerPeerToPeerProxyConfigs: Seq[ProxyConfig] =
          for {
            toSequencerIndex <- (1 to numberOfSequencers)
          } yield {
            BftSequencerPeerToPeer(
              s"to-peer$toSequencerIndex",
              s"sequencer$toSequencerIndex",
            )
          }

        new UseToxiproxy(
          ToxiproxyConfig(proxies =
            sequencerToPostgresProxyConfigs ++ sequencerPeerToPeerProxyConfigs
          )
        )
      })
    } else None

  toxiProxyPlugin.foreach(registerPlugin)

  private def addToxics(proxy: toxiproxy.Proxy): Unit = {
    val proxyName = proxy.getName
    val isDbProxy = proxyName.endsWith(PostgresProxyNameSuffix)
    val maybeLatency =
      if (isDbProxy) {
        sequencerDbLatencyMillis
      } else {
        sequencerToSequencerLatencyMillis
      }

    maybeLatency.foreach { latency =>
      // Use only upstream (client -> server) latencies to try to avoid issues related to directions in which connections
      //  are established, i.e., we want connections to use Toxiproxy addresses before non-Toxiproxy addresses are used.
      // TODO(#28117) support two-way latencies
      proxy
        .toxics()
        .latency(s"upstream-latency-$proxyName", ToxicDirection.UPSTREAM, latency)
    }
  }

  "Run a BFT orderer benchmark" in { implicit env =>
    import env.*

    toxiProxyPlugin.foreach(
      _.runningToxiproxy.controllingToxiproxyClient.getProxies.forEach(addToxics)
    )
    mediators.local.foreach(_.stop())

    // Use a high timeout to allow many nodes in performance testing environments

    waitUntilAllBftSequencersAuthenticateDisseminationQuorum(5.minutes)

    val benchmarkTool = new BftBenchmarkTool(DaBftBindingFactory, loggerFactory)
    val benchmarkToolConfig =
      BftBenchmarkConfig(
        transactionBytes = requestBytes,
        runDuration = runDuration,
        perNodeWritePeriod = perNodeWritePeriod,
        reportingInterval = reportingIntervalOpt,
        nodes = bftSequencerPlugin.p2pEndpoints
          .getOrElse(fail("No P2P endpoints found"))
          .values
          .zipWithIndex
          .map { case (p2pConfig, idx) =>
            val host = p2pConfig.address
            val port = p2pConfig.port.unwrap
            val node: BftBenchmarkConfig.Node =
              if (idx == 0) {
                BftBenchmarkConfig.NetworkedReadWriteNode(
                  host = host,
                  writePort = port,
                  readPort = port,
                )
              } else {
                BftBenchmarkConfig.NetworkedWriteOnlyNode(
                  host = host,
                  writePort = port,
                )
              }
            node
          }
          .toSeq,
      )
    benchmarkTool.run(benchmarkToolConfig).discard
  }
}
