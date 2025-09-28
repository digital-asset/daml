// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms

import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{
  CryptoConfig,
  CryptoProvider,
  KmsConfig,
  NonNegativeFiniteDuration,
  PrivateKeyStoreConfig,
}
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.bootstrap.InitializedSynchronizer
import com.digitalasset.canton.integration.tests.topology.TopologyManagementHelper
import com.digitalasset.canton.protocol.StaticSynchronizerParameters as StaticSynchronizerParametersInternal
import com.digitalasset.canton.time.{RemoteClock, SimClock}
import monocle.macros.syntax.lens.*

/** Defines the necessary environment and setup for running a set of nodes with KMS as their
  * provider, either with or without pre-generated keys.
  *
  * Check contributing/kms.md on how to run the tests
  */
trait KmsCryptoIntegrationTestBase extends TopologyManagementHelper {
  self: CommunityIntegrationTest with EnvironmentSetup =>

  // Defines which nodes will run an external KMS.
  protected lazy val protectedNodes: Set[String] = Set("participant1")

  // Defines which nodes will not use session signing keys.
  protected lazy val nodesWithSessionSigningKeysDisabled: Set[String] = Set.empty

  protected def otherConfigTransforms: Seq[ConfigTransform] = Seq.empty

  protected val kmsConfigTransform: ConfigTransform = {
    def modifyCryptoSchemeConfig(conf: CryptoConfig): CryptoConfig =
      conf.copy(provider = CryptoProvider.Jce)

    val sequencerConfigTransform = ConfigTransforms.updateSequencerConfig("sequencer1")(
      _.focus(_.crypto).modify(modifyCryptoSchemeConfig)
    )
    val mediatorConfigTransform = ConfigTransforms.updateMediatorConfig("mediator1")(
      _.focus(_.crypto).modify(modifyCryptoSchemeConfig)
    )
    val participant1ConfigTransform = ConfigTransforms.updateParticipantConfig("participant1")(
      _.focus(_.crypto).modify(modifyCryptoSchemeConfig)
    )
    val participant2ConfigTransform = ConfigTransforms.updateParticipantConfig("participant2")(
      _.focus(_.crypto).modify(modifyCryptoSchemeConfig)
    )

    sequencerConfigTransform
      .compose(mediatorConfigTransform)
      .compose(participant1ConfigTransform)
      .compose(participant2ConfigTransform)
  }

  protected def kmsConfig: KmsConfig

  protected def topologyPreDefinedKeys: TopologyKmsKeys

  // we use a distributed topology for the KMS tests
  protected val environmentBaseConfig: EnvironmentDefinition =
    EnvironmentDefinition.P2S1M1_Manual

  protected def getTopologyKeysForNode(name: String): TopologyKmsKeys =
    if (name.contains("sequencer"))
      topologyPreDefinedKeys.copy(
        namespaceKeyId = topologyPreDefinedKeys.namespaceKeyId.map(_.concat("-sequencer")),
        sequencerAuthKeyId = topologyPreDefinedKeys.sequencerAuthKeyId.map(_.concat("-sequencer")),
        signingKeyId = topologyPreDefinedKeys.signingKeyId.map(_.concat("-sequencer")),
        encryptionKeyId = topologyPreDefinedKeys.encryptionKeyId,
      )
    else if (name.contains("mediator"))
      topologyPreDefinedKeys.copy(
        namespaceKeyId = topologyPreDefinedKeys.namespaceKeyId.map(_.concat("-mediator")),
        sequencerAuthKeyId = topologyPreDefinedKeys.sequencerAuthKeyId.map(_.concat("-mediator")),
        signingKeyId = topologyPreDefinedKeys.signingKeyId.map(_.concat("-mediator")),
        encryptionKeyId = topologyPreDefinedKeys.encryptionKeyId,
      )
    else // for the participants
      topologyPreDefinedKeys.copy(
        namespaceKeyId = topologyPreDefinedKeys.namespaceKeyId.map(_.concat(s"-$name")),
        sequencerAuthKeyId = topologyPreDefinedKeys.sequencerAuthKeyId.map(_.concat(s"-$name")),
        signingKeyId = topologyPreDefinedKeys.signingKeyId.map(_.concat(s"-$name")),
        encryptionKeyId = topologyPreDefinedKeys.encryptionKeyId.map(_.concat(s"-$name")),
      )

  protected def teardown(): Unit = {}

  override lazy val environmentDefinition: EnvironmentDefinition =
    environmentBaseConfig
      .addConfigTransforms(otherConfigTransforms*)
      .addConfigTransform(
        ConfigTransforms.setCrypto(
          CryptoConfig(
            provider = CryptoProvider.Kms,
            kms = Some(kmsConfig),
            privateKeyStore = PrivateKeyStoreConfig(None),
          ),
          (name: String) => protectedNodes.contains(name),
        )
      )
      .withSetup { implicit env =>
        import env.*

        sequencer1.start()
        mediator1.start()

        if (
          sequencer1.config.init.identity.isManual && !sequencer1.config.init.generateTopologyTransactionsAndKeys
        ) {
          manuallyInitNode(
            sequencer1,
            if (sequencer1.config.crypto.provider == CryptoProvider.Kms)
              Some(getTopologyKeysForNode(sequencer1.name))
            else None,
          )
        }

        if (
          mediator1.config.init.identity.isManual && !mediator1.config.init.generateTopologyTransactionsAndKeys
        )
          manuallyInitNode(
            mediator1,
            if (mediator1.config.crypto.provider == CryptoProvider.Kms)
              Some(getTopologyKeysForNode(mediator1.name))
            else None,
          )

        val topologyChangeDelay = environment.clock match {
          case _: RemoteClock | _: SimClock => NonNegativeFiniteDuration.Zero
          case _ => StaticSynchronizerParametersInternal.defaultTopologyChangeDelay.toConfig
        }

        val staticParameters =
          StaticSynchronizerParameters.defaults(
            sequencer1.config.crypto,
            testedProtocolVersion,
            topologyChangeDelay = topologyChangeDelay,
          )
        val synchronizerId = bootstrap.synchronizer(
          daName.unwrap,
          sequencers = Seq(sequencer1),
          mediators = Seq(mediator1),
          synchronizerOwners = Seq(sequencer1),
          synchronizerThreshold = PositiveInt.one,
          staticParameters,
        )

        env.initializedSynchronizers.put(
          daName,
          InitializedSynchronizer(
            synchronizerId,
            staticParameters.toInternal,
            synchronizerOwners = Set(sequencer1),
          ),
        )

        // make sure synchronizer nodes are initialized
        mediator1.health.wait_for_initialized()
        sequencer1.health.wait_for_initialized()

        participant1.start()
        participant2.start()

        Seq(participant1, participant2) foreach {
          case p: LocalParticipantReference if p.config.init.identity.isManual =>
            manuallyInitNode(
              p,
              if (p.config.crypto.provider == CryptoProvider.Kms)
                Some(getTopologyKeysForNode(p.name))
              else None,
            )
          case _ =>
        }

        // make sure participant nodes are initialized
        participant1.health.wait_for_initialized()
        participant2.health.wait_for_initialized()

        Seq(participant1, participant2).foreach(
          _.synchronizers.connect_local(sequencer1, alias = daName)
        )
      }
      .withTeardown(_ => teardown())

  // by default auto-init is set to true, and we run without persistence
  protected def setupPlugins(
      withAutoInit: Boolean,
      storagePlugin: Option[EnvironmentSetupPlugin],
      sequencerPlugin: EnvironmentSetupPlugin,
  ): Unit
}
