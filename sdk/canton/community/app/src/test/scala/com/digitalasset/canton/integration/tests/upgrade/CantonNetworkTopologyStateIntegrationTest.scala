// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade

import better.files.File
import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.{BatchAggregatorConfig, BatchingConfig, TopologyConfig}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.plugins.toxiproxy.UseToxiproxy.ToxiproxyConfig
import com.digitalasset.canton.integration.plugins.toxiproxy.{ParticipantToPostgres, UseToxiproxy}
import com.digitalasset.canton.integration.tests.toxiproxy.ToxiproxyHelpers
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.NodeLoggingUtil
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage}
import com.digitalasset.canton.store.IndexedTopologyStoreId
import com.digitalasset.canton.topology.processing.{InitialTopologySnapshotValidator, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.db.DbTopologyStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransactions,
  TimeQuery,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.SynchronizerParametersState
import com.digitalasset.canton.util.JarResourceUtils
import com.google.protobuf.ByteString
import eu.rekawek.toxiproxy.model.ToxicDirection
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.concurrent.PatienceConfiguration.Timeout

import scala.concurrent.duration.*

/** Try to load CN genesis state and ensure that the history state can still be loaded with the
  * stricter checks in Canton's topology management.
  *
  * Specifically, we're testing that we can import OTKs with missing signing key signatures:
  *   - into a temporary topology store
  *   - as part of the genesis state, i.e. sequencing timestamp is
  *     [[SignedTopologyTransaction.InitialTopologySequencingTime]]
  */
final class CantonNetworkTopologyStateIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  private val enablePostgres = true
  private val toxiProxyLatency = 0 // in ms, 0 to disable
  private val timeout = 30.seconds

  private val participant1Proxy = "participant1-to-postgres"
  // using toxi-proxy to simulate database latency
  private val toxiproxyPlugin = new UseToxiproxy(
    ToxiproxyConfig(proxies = Seq(ParticipantToPostgres(participant1Proxy, "participant1")))
  )
  if (enablePostgres) {
    registerPlugin(new UsePostgres(loggerFactory))
  }
  if (toxiProxyLatency > 0) {
    if (!enablePostgres)
      throw new IllegalStateException("ToxiProxy for Postgres enabled, but Postgres is disabled")
    registerPlugin(toxiproxyPlugin)
  }

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S1M1_Manual
      .withSetup { env =>
        import env.*
        participant1.start()
      }
      .withTeardown { _ =>
        if (toxiProxyLatency > 0) {
          ToxiproxyHelpers.removeAllProxies(
            toxiproxyPlugin.runningToxiproxy.controllingToxiproxyClient
          )
        }
      }

  private lazy val genesisFileName: String = "CN-genesis-state-20250410.gz"

  private lazy val genesisBytes = {
    val genesisStateZip = File(JarResourceUtils.resourceFile(genesisFileName).toURI)
    genesisStateZip.gzipInputStream()(ByteString.readFrom)
  }
  private lazy val genesisSnapshot =
    StoredTopologyTransactions.fromTrustedByteString(genesisBytes).value
  private val disableDebugLogging = false

  private var topologyStoreAfterImport: Option[TopologyStore[?]] = None

  private def runValidation(
      topoStoreIdx: Int,
      txs: GenericStoredTopologyTransactions,
      cleanupTopologyState: Boolean,
  )(implicit env: TestConsoleEnvironment) = {
    import env.*

    val synchronizerId = txs
      .collectOfMapping[SynchronizerParametersState]
      .result
      .headOption
      .value
      .mapping
      .synchronizerId
    val static = StaticSynchronizerParameters.defaultsWithoutKMS(testedProtocolVersion).toInternal
    val storeId = SynchronizerStore(synchronizerId.toPhysical)
    val store = participant1.underlying.valueOrFail("is there").storage match {
      case _: MemoryStorage =>
        new InMemoryTopologyStore[TopologyStoreId](
          SynchronizerStore(synchronizerId.toPhysical),
          testedProtocolVersion,
          loggerFactory,
          timeouts,
        )
      case storage: DbStorage =>
        new DbTopologyStore(
          storage,
          storeId,
          IndexedTopologyStoreId.tryCreate(storeId, topoStoreIdx),
          testedProtocolVersion,
          timeouts,
          BatchingConfig(),
          loggerFactory,
        )
    }
    val validator = new InitialTopologySnapshotValidator(
      participant1.underlying.value.cryptoPureApi,
      store,
      BatchAggregatorConfig.defaultsForTesting,
      TopologyConfig.forTesting.copy(validateInitialTopologySnapshot = true),
      Some(static),
      timeouts,
      loggerFactory,
      cleanupTopologySnapshot = cleanupTopologyState,
    )
    validator
      .validateAndApplyInitialTopologySnapshot(txs)
      .futureValueUS(Timeout(timeout)) // See https://github.com/DACH-NY/canton/issues/26651
      .value
    (store, validator)
  }

  "Canton node " can {
    "load topology transactions from Canton Network genesis state to a temporary store" in {
      implicit env =>
        import env.*

        val testTempStoreId =
          participant1.topology.stores
            .create_temporary_topology_store("test", testedProtocolVersion)

        participant1.topology.transactions
          .import_topology_snapshot(genesisBytes, testTempStoreId)

        val importedState = participant1.topology.transactions
          .list(testTempStoreId, timeQuery = TimeQuery.Range(None, None), operation = None)
          .result

        importedState should have length genesisSnapshot.result.length.toLong

        // comparing individual elements instead of `importedState shouldBe genesisSnapshot.result`,
        // because the way scala test does comparison is highly inefficient
        importedState.zip(genesisSnapshot.result).foreach { case (actual, expected) =>
          actual.rejectionReason shouldBe expected.rejectionReason

          // we only compare the TopologyTransaction, because the importing
          // and loading transactions in bulk (as opposed to validating the
          // initial topology store bootstrap snapshot) will assign different local "sequenced time"
          // and also might change the signatures (eg previously sequencers had to sign SequencerSynchronizerState,
          // but such signatures will now be automatically removed and therefore the signatures would mismatch
          actual.transaction.transaction shouldBe expected.transaction.transaction
        }
    }

    "successfully validate the initial topology snapshot" in { implicit env =>
      if (disableDebugLogging)
        NodeLoggingUtil.setLevel(level = "INFO")
      if (toxiProxyLatency > 0) {
        // enable db toxiproxy
        val proxy = toxiproxyPlugin.runningToxiproxy.getProxy(participant1Proxy)
        val client = proxy.valueOrFail("must be here").underlying
        client.toxics().latency("participant-db", ToxicDirection.UPSTREAM, toxiProxyLatency.toLong)
      }

      val (store1, _) = runValidation(11, genesisSnapshot, cleanupTopologyState = true)
      topologyStoreAfterImport = Some(store1)

    }

    "validate reimport after cleanup" in { implicit env =>
      import env.*
      // now fetch the state again
      topologyStoreAfterImport.map { store =>
        val stateAfterImport = StoredTopologyTransactions(
          store
            .findEssentialStateAtSequencedTime(
              SequencedTime(CantonTimestamp.MaxValue),
              includeRejected = true,
            )
            .runWith(Sink.seq)
            .futureValue
        )

        runValidation(12, stateAfterImport, cleanupTopologyState = false)
      }
    }

  }
}
