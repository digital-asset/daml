// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import com.digitalasset.canton
import com.digitalasset.canton.UniquePortGenerator
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.StorageConfig.Memory
import com.digitalasset.canton.config.{CantonConfig, QueryCostMonitoringConfig, TlsClientConfig}
import com.digitalasset.canton.crypto.provider.jce.JcePrivateCrypto
import com.digitalasset.canton.crypto.{Fingerprint, SigningKeySpec, SigningKeyUsage}
import com.digitalasset.canton.integration.EnvironmentSetupPlugin
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.{
  MultiSynchronizer,
  SequencerSynchronizerGroups,
  SingleSynchronizer,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig.BftSequencer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.{
  BftBlockOrderingStandalonePeerConfig,
  DefaultConsensusBlockCompletionTimeout,
  DefaultDedicatedExecutionContextDivisor,
  DefaultEpochLength,
  DefaultMaxBatchCreationInterval,
  DefaultMaxBatchesPerProposal,
  DefaultMaxRequestsInBatch,
  DefaultMinRequestsInBatch,
  P2PNetworkConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochLength
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeConfig
import com.digitalasset.canton.synchronizer.sequencer.{
  BlockSequencerConfig,
  BlockSequencerStreamInstrumentationConfig,
  SequencerConfig,
}
import com.digitalasset.canton.topology.{Namespace, SequencerId}
import com.digitalasset.canton.util.SingleUseCell
import monocle.macros.GenLens
import monocle.macros.syntax.lens.*

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** @param dynamicallyOnboardedSequencerNames
  *   Names of sequencers that are not part of the initial network config, and can be added later as
  *   part of a test.
  * @param shouldGenerateEndpointsOnly
  *   If true, replaces addresses and ports only (instead of building a full config) to avoid their
  *   clashes. Useful for config file integration tests.
  * @param shouldOverwriteStoredEndpoints
  *   Set to true to overwrite peer endpoints in the database with config, e.g., when using a
  *   database dump.
  * @param shouldUseMemoryStorageForBftOrderer
  *   Overwrites the dedicated BFT Orderer's storage to in-memory.
  * @param standaloneOrderingNodes
  *   Enable standalone BFT ordering nodes mode.
  */
final class UseBftSequencer(
    override protected val loggerFactory: NamedLoggerFactory,
    val sequencerGroups: SequencerSynchronizerGroups = SingleSynchronizer,
    dynamicallyOnboardedSequencerNames: Seq[InstanceName] = Seq.empty,
    shouldGenerateEndpointsOnly: Boolean = false,
    shouldOverwriteStoredEndpoints: Boolean = false,
    shouldUseMemoryStorageForBftOrderer: Boolean = false,
    shouldBenchmarkBftSequencer: Boolean = false,
    standaloneOrderingNodes: Boolean = false,
    epochLength: EpochLength = DefaultEpochLength,
    // Use a shorter empty block creation timeout by default to speed up tests that stop sequencing
    //  and use `GetTime` to await an effective time to be reached on the synchronizer.
    consensusEmptyBlockCreationTimeout: FiniteDuration = 250.millis,
    consensusBlockCompletionTimeout: FiniteDuration = DefaultConsensusBlockCompletionTimeout,
    maxRequestsInBatch: Short = DefaultMaxRequestsInBatch,
    minRequestsInBatch: Short = DefaultMinRequestsInBatch,
    maxBatchCreationInterval: FiniteDuration = DefaultMaxBatchCreationInterval,
    maxBatchesPerBlockProposal: Short = DefaultMaxBatchesPerProposal,
    dedicatedExecutionContextDivisor: Option[Int] = DefaultDedicatedExecutionContextDivisor,
) extends EnvironmentSetupPlugin {

  private val tmpDir = better.files.File(System.getProperty("java.io.tmpdir"))

  val p2pEndpoints: SingleUseCell[Map[InstanceName, BftBlockOrdererConfig.P2PEndpointConfig]] =
    new SingleUseCell()

  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig =
    if (shouldGenerateEndpointsOnly) generateEndpoints(config)
    else createFullConfig(config)

  private def generateEndpoints(config: CantonConfig) = {
    val instanceNameToPort =
      config.sequencers.keys.map(_ -> UniquePortGenerator.next).toMap

    val sequencers =
      config.sequencers.map { case (instanceName, sequencerNodeConfig) =>
        val sequencer =
          sequencerNodeConfig.sequencer match {
            case BftSequencer(blockSequencerConfig, bftOrdererConfig) =>
              BftSequencer(
                blockSequencerConfig,
                bftOrdererConfig
                  .copy(
                    epochLength = epochLength,
                    consensusEmptyBlockCreationTimeout = consensusEmptyBlockCreationTimeout,
                    consensusBlockCompletionTimeout = consensusBlockCompletionTimeout,
                    maxRequestsInBatch = maxRequestsInBatch,
                    minRequestsInBatch = minRequestsInBatch,
                    maxBatchCreationInterval = maxBatchCreationInterval,
                    maxBatchesPerBlockProposal = maxBatchesPerBlockProposal,
                    dedicatedExecutionContextDivisor = dedicatedExecutionContextDivisor,
                  )
                  // server endpoint's lens
                  .focus(_.initialNetwork)
                  .some
                  .andThen(GenLens[P2PNetworkConfig](_.serverEndpoint))
                  .modify(
                    _.focus(_.address)
                      .replace("localhost")
                      .focus(_.internalPort)
                      .replace(Some(instanceNameToPort(instanceName)))
                      .focus(_.externalAddress)
                      .replace("localhost")
                      .focus(_.externalPort)
                      .replace(instanceNameToPort(instanceName))
                  )
                  // peer endpoints' lens
                  .focus(_.initialNetwork)
                  .some
                  .andThen(GenLens[P2PNetworkConfig](_.peerEndpoints))
                  .modify { peerEndpoints =>
                    val otherPeerPorts =
                      instanceNameToPort.filterNot { case (name, _) => name == instanceName }
                    peerEndpoints
                      .zip(otherPeerPorts.values)
                      .map { case (p2pEndpointConfig, port) =>
                        p2pEndpointConfig
                          .focus(_.address)
                          .replace("localhost")
                          .focus(_.port)
                          .replace(port)
                      }
                  },
              )

            case otherSequencerConfig => otherSequencerConfig
          }

        instanceName -> sequencerNodeConfig.focus(_.sequencer).replace(sequencer)
      }

    config.focus(_.sequencers).replace(sequencers)
  }

  private def createFullConfig(config: CantonConfig): CantonConfig = {
    // Contains all sequencers from the environment definition. Typically, the environment definition also contains
    //  sequencers that are onboarded dynamically by tests (i.e, not initialized from the very beginning).
    val groups = sequencerGroups match {
      case MultiSynchronizer(groups) => groups
      case SingleSynchronizer => Seq(config.sequencers.keys)
    }
    val sequencersToEndpoints: mutable.Map[InstanceName, BftBlockOrdererConfig.P2PEndpointConfig] =
      mutable.Map()
    val sequencersToConfig: Map[InstanceName, SequencerConfig] =
      groups.flatMap { group =>
        val endpoints = group.map { sequencerName =>
          sequencerName -> BftBlockOrdererConfig.P2PEndpointConfig(
            "localhost",
            UniquePortGenerator.next,
            Some(TlsClientConfig(trustCollectionFile = None, clientCert = None, enabled = false)),
          )
        }.toMap
        endpoints.map { case (selfInstanceName, selfEndpoint) =>
          sequencersToEndpoints.addOne(selfInstanceName -> selfEndpoint)
          val otherInitialNamesAndEndpoints =
            if (dynamicallyOnboardedSequencerNames.contains(selfInstanceName))
              // Dynamically onboarded peers' endpoints are not part of the initial network but are added later
              //  by the concrete test case.
              Seq.empty
            else
              endpoints.view.filterNot { case (name, _) =>
                name == selfInstanceName || dynamicallyOnboardedSequencerNames.contains(name)
              }.toSeq
          val (otherInitialNames, otherInitialEndpoints) = otherInitialNamesAndEndpoints.unzip
          val network = BftBlockOrdererConfig.P2PNetworkConfig(
            BftBlockOrdererConfig.P2PServerConfig(
              selfEndpoint.address,
              internalPort = Some(selfEndpoint.port),
              externalAddress = selfEndpoint.address,
              externalPort = selfEndpoint.port,
              externalTlsConfig = Some(
                TlsClientConfig(trustCollectionFile = None, clientCert = None, enabled = false)
              ),
            ),
            peerEndpoints = otherInitialEndpoints,
            overwriteStoredEndpoints = shouldOverwriteStoredEndpoints,
          )
          val standaloneOpt = Option.when(standaloneOrderingNodes) {
            val keyPair = JcePrivateCrypto
              .generateSigningKeypair(SigningKeySpec.EcCurve25519, SigningKeyUsage.ProtocolOnly)
              .getOrElse(throw new RuntimeException("Failed to generate keypair"))
            val privKey = keyPair.privateKey
            val pubKey = keyPair.publicKey
            val privKeyFile = tmpDir / s"node-${selfInstanceName}_signing_private_key.bin"
            val pubKeyFile = tmpDir / s"node-${selfInstanceName}_signing_public_key.bin"
            privKeyFile.writeByteArray(privKey.toProtoV30.toByteArray)
            pubKeyFile.writeByteArray(pubKey.toProtoV30.toByteArray)
            BftBlockOrdererConfig.BftBlockOrderingStandaloneNetworkConfig(
              thisSequencerId = sequencerId(selfInstanceName),
              signingPrivateKeyProtoFile = privKeyFile.toJava,
              signingPublicKeyProtoFile = pubKeyFile.toJava,
              peers = otherInitialNames
                .map { otherInitialInstanceName =>
                  BftBlockOrderingStandalonePeerConfig(
                    sequencerId = sequencerId(otherInitialInstanceName),
                    signingPublicKeyProtoFile =
                      tmpDir / s"node-${otherInitialInstanceName}_signing_public_key.bin" toJava,
                  )
                },
            )
          }
          val blockSequencerConfig =
            if (shouldBenchmarkBftSequencer)
              BlockSequencerConfig(
                circuitBreaker = BlockSequencerConfig.CircuitBreakerConfig(enabled = false),
                streamInstrumentation = BlockSequencerStreamInstrumentationConfig(isEnabled = true),
              )
            else BlockSequencerConfig()
          selfInstanceName -> SequencerConfig.BftSequencer(
            block = blockSequencerConfig,
            config = BftBlockOrdererConfig(
              epochLength = epochLength,
              consensusEmptyBlockCreationTimeout = consensusEmptyBlockCreationTimeout,
              consensusBlockCompletionTimeout = consensusBlockCompletionTimeout,
              maxRequestsInBatch = maxRequestsInBatch,
              minRequestsInBatch = minRequestsInBatch,
              maxBatchCreationInterval = maxBatchCreationInterval,
              maxBatchesPerBlockProposal = maxBatchesPerBlockProposal,
              dedicatedExecutionContextDivisor = dedicatedExecutionContextDivisor,
              initialNetwork = Some(network),
              standalone = standaloneOpt,
              storage = Option.when(shouldUseMemoryStorageForBftOrderer)(Memory()),
            ),
          )
        }
      }.toMap

    def mapSequencerConfigs(
        kv: (InstanceName, SequencerNodeConfig)
    ): (InstanceName, SequencerNodeConfig) = kv match {
      case (name, cfg) =>
        (
          name,
          cfg.focus(_.sequencer).replace(sequencersToConfig(name)),
        )
    }

    p2pEndpoints.putIfAbsent(sequencersToEndpoints.toMap)
    config
      .focus(_.monitoring.logging.queryCost)
      .modify { _ =>
        if (shouldBenchmarkBftSequencer)
          Some(
            QueryCostMonitoringConfig(every = canton.config.NonNegativeFiniteDuration.ofSeconds(30))
          )
        else
          None
      }
      .focus(_.sequencers)
      .modify(_.map(mapSequencerConfigs))
  }

  private def sequencerId(instanceName: InstanceName): String =
    SequencerId
      .tryCreate(instanceName.unwrap, Namespace(Fingerprint.tryFromString("default")))
      .toProtoPrimitive
}
