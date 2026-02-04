// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.benchmarks

import cats.data.EitherT
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest.{
  RichSynchronizerIdO,
  defaultStaticSynchronizerParameters,
  testedProtocolVersion,
  testedReleaseProtocolVersion,
}
import com.digitalasset.canton.FutureHelpers
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.DbConfig.Postgres
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt, PositiveLong}
import com.digitalasset.canton.console.LocalSequencerReference
import com.digitalasset.canton.crypto.{Crypto, SynchronizerCrypto, SynchronizerCryptoClient}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.replica.ReplicaManager
import com.digitalasset.canton.resource.{DbStorageSingle, Storage}
import com.digitalasset.canton.sequencing.GrpcSequencerConnection
import com.digitalasset.canton.sequencing.authentication.AuthenticationTokenManagerConfig
import com.digitalasset.canton.sequencing.client.ReplayAction.SequencerSends
import com.digitalasset.canton.sequencing.client.grpc.GrpcSequencerChannelBuilder
import com.digitalasset.canton.sequencing.client.transports.replay.ReplayingSendsSequencerClientTransportPekko
import com.digitalasset.canton.sequencing.client.transports.{
  GrpcSequencerClientAuth,
  GrpcSequencerClientTransportPekko,
  SequencerClientTransport,
  SequencerClientTransportPekko,
}
import com.digitalasset.canton.sequencing.client.{ReplayConfig, RequestSigner, SequencerClient}
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.synchronizer.metrics.SequencerTestMetrics
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.cache.{CacheTestMetrics, TopologyStateWriteThroughCache}
import com.digitalasset.canton.topology.client.WriteThroughCacheSynchronizerTopologyClient
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.{
  DbInitializationStore,
  NoPackageDependencies,
  TopologyStore,
}
import com.digitalasset.canton.topology.{Member, ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, TraceContext, TracingConfig}
import io.grpc.{CallOptions, ManagedChannel}
import org.apache.pekko.stream.Materializer
import org.scalatest.{EitherValues, OptionValues}

import java.nio.file.Path
import scala.concurrent.ExecutionContextExecutor

/** A lightweight implementation of the Participant for replay test purposes. It's a thin wrapper
  * around the [[ReplayingSendsSequencerClientTransportPekko]] that avoids heavy read-side
  * processing.
  */
final class ReplayingParticipant private (
    val sendsConfig: SequencerSends,
    synchronizerCryptoClient: SynchronizerCryptoClient,
    replayingTransport: ReplayingSendsSequencerClientTransportPekko,
    storage: DbStorageSingle,
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
) extends NamedLogging
    with FlagCloseableAsync {

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] =
    Seq[AsyncOrSyncCloseable](
      SyncCloseable("replayingTransport.close()", replayingTransport.close()),
      SyncCloseable("synchronizerCryptoClient.close()", synchronizerCryptoClient.close()),
      SyncCloseable("storage.close()", storage.close()),
    )
}

object ReplayingParticipant extends FutureHelpers with EitherValues with OptionValues {

  def tryCreate(
      connectedToSequencer: LocalSequencerReference,
      replayDirectory: Path,
      recordingFileName: String,
      futureSupervisor: FutureSupervisor,
      postgresPlugin: UsePostgres,
      clock: Clock,
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
  )(implicit
      env: TestConsoleEnvironment,
      closeContext: CloseContext,
      traceContext: TraceContext,
  ): ReplayingParticipant = {
    import env.*

    val nodeName = recordingFileName.split("::")(1)

    val extendedLoggerFactory = loggerFactory.append("replaying-participant", nodeName)

    val dbConfig =
      postgresPlugin.generateDbConfig(
        nodeName,
        // Make it as lightweight as possible
        DbParametersConfig(maxConnections = Some(PositiveInt.one)),
        Postgres.defaultConfig,
      )
    val storage = DbStorageSingle
      .create(
        dbConfig,
        // `true` results in a smaller number of reserved connections, but we already define the number above
        connectionPoolForParticipant = false,
        logQueryCost = None,
        clock = clock,
        scheduler = None, // Used for query cost tracking (disabled above)
        metrics = ParticipantTestMetrics.storageMetrics,
        timeouts = timeouts,
        loggerFactory = extendedLoggerFactory,
      )
      .value
      .onShutdown(throw new IllegalStateException("Unexpected shutdown"))
      .value

    val initStore = new DbInitializationStore(storage, timeouts, extendedLoggerFactory)
    val member = ParticipantId(initStore.uid.futureValueUS.value)
    val psid = connectedToSequencer.synchronizer_id.toPhysical

    val crypto =
      mkCrypto(
        clock,
        futureSupervisor,
        storage,
        extendedLoggerFactory,
        timeouts,
      ).valueOrFail("create crypto").futureValueUS
    val synchronizerCrypto = SynchronizerCrypto(crypto, defaultStaticSynchronizerParameters)

    val underlyingTransport =
      mkUnderlyingTransport(
        connectedToSequencer.sequencerConnection.toInternal,
        member,
        psid,
        synchronizerCrypto,
        clock,
        extendedLoggerFactory,
        timeouts,
      )

    val indexedStringStore =
      IndexedStringStore.create(
        storage,
        CacheConfig(maximumSize = PositiveLong.tryCreate(10)),
        timeouts,
        loggerFactory,
      )

    val topologyStore =
      TopologyStore
        .create(
          SynchronizerStore(psid),
          storage,
          indexedStringStore,
          testedProtocolVersion,
          timeouts,
          BatchingConfig(),
          extendedLoggerFactory,
        )
        .futureValueUS
    val synchronizerTopologyClient =
      WriteThroughCacheSynchronizerTopologyClient
        .create(
          clock,
          defaultStaticSynchronizerParameters,
          topologyStore,
          new TopologyStateWriteThroughCache(
            topologyStore,
            BatchAggregatorConfig.defaultsForTesting,
            TopologyConfig.forTesting.topologyStateCacheEvictionThreshold,
            TopologyConfig.forTesting.maxTopologyStateCacheItems,
            TopologyConfig.forTesting.enableTopologyStateCacheConsistencyChecks,
            CacheTestMetrics.metrics,
            futureSupervisor,
            timeouts,
            loggerFactory,
          ),
          synchronizerUpgradeTime = None,
          NoPackageDependencies,
          CachingConfigs(),
          TopologyConfig(),
          timeouts,
          futureSupervisor,
          extendedLoggerFactory,
        )()
        .futureValueUS
    val synchronizerCryptoClient =
      SynchronizerCryptoClient.create(
        member,
        psid.logical,
        synchronizerTopologyClient,
        defaultStaticSynchronizerParameters,
        synchronizerCrypto,
        verificationParallelismLimit = PositiveInt.one,
        CachingConfigs.defaultPublicKeyConversionCache,
        timeouts,
        futureSupervisor,
        extendedLoggerFactory,
      )

    val sendsConfig = SequencerSends(extendedLoggerFactory, usePekko = true)

    val recordingConfig =
      ReplayConfig(replayDirectory, sendsConfig).recordingConfig.setFilename(recordingFileName)

    // Registers itself in the `sendsConfig`
    val replayingTransport =
      new ReplayingSendsSequencerClientTransportPekko(
        testedProtocolVersion,
        recordingConfig.fullFilePath,
        sendsConfig,
        member,
        underlyingTransport,
        RequestSigner(synchronizerCryptoClient, testedProtocolVersion, extendedLoggerFactory),
        synchronizerCryptoClient.currentSnapshotApproximation.futureValueUS,
        clock,
        SequencerTestMetrics.sequencerClient,
        timeouts,
        extendedLoggerFactory,
      )

    new ReplayingParticipant(
      sendsConfig,
      synchronizerCryptoClient,
      replayingTransport,
      storage,
      extendedLoggerFactory,
      timeouts,
    )
  }

  private def mkCrypto(
      clock: Clock,
      futureSupervisor: FutureSupervisor,
      storage: Storage,
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
  )(implicit
      executionContext: ExecutionContextExecutor,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Crypto] = {
    val cryptoConfig = CryptoConfig()
    Crypto
      .create(
        cryptoConfig,
        CachingConfigs.defaultKmsMetadataCache,
        SessionEncryptionKeyCacheConfig(),
        CachingConfigs.defaultPublicKeyConversionCache,
        storage,
        Option.empty[ReplicaManager],
        testedReleaseProtocolVersion,
        futureSupervisor,
        clock,
        executionContext,
        timeouts,
        BatchingConfig(),
        loggerFactory,
        NoReportingTracerProvider,
      )
  }

  private def mkUnderlyingTransport(
      sequencerConnection: GrpcSequencerConnection,
      member: Member,
      synchronizerId: PhysicalSynchronizerId,
      synchronizerCrypto: SynchronizerCrypto,
      clock: Clock,
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
  )(implicit
      executionContext: ExecutionContextExecutor,
      executionSequencerFactory: ExecutionSequencerFactory,
      materializer: Materializer,
  ): SequencerClientTransport & SequencerClientTransportPekko = {

    val channel = createChannel(sequencerConnection, loggerFactory)

    val auth =
      new GrpcSequencerClientAuth(
        synchronizerId,
        member,
        synchronizerCrypto,
        NonEmpty(Map, sequencerConnection.endpoints.head -> channel),
        Seq(testedProtocolVersion),
        AuthenticationTokenManagerConfig(),
        clock,
        metricsO = None,
        metricsContext = MetricsContext.Empty,
        timeouts,
        loggerFactory,
      )

    new GrpcSequencerClientTransportPekko(
      channel,
      CallOptions.DEFAULT,
      auth,
      timeouts,
      SequencerClient
        .loggerFactoryWithSequencerAlias(loggerFactory, sequencerConnection.sequencerAlias),
      testedProtocolVersion,
    )
  }

  private def createChannel(conn: GrpcSequencerConnection, loggerFactory: NamedLoggerFactory)(
      implicit executionContext: ExecutionContextExecutor
  ): ManagedChannel = {
    val channelBuilder =
      ClientChannelBuilder(
        SequencerClient.loggerFactoryWithSequencerAlias(loggerFactory, conn.sequencerAlias)
      )
    GrpcSequencerChannelBuilder(
      channelBuilder,
      conn,
      maxRequestSize = NonNegativeInt.maxValue,
      TracingConfig.Propagation.Disabled,
      Some(KeepAliveClientConfig()),
    )
  }
}
