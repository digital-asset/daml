// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.daml.nonempty.NonEmpty
import com.daml.test.evidence.tag.Security.SecurityTestSuite
import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{
  CryptoConfig,
  CryptoProvider,
  CryptoSchemeConfig,
  SigningSchemeConfig,
}
import com.digitalasset.canton.console.{InstanceReference, LocalInstanceReference}
import com.digitalasset.canton.crypto.{SigningAlgorithmSpec, SigningKeySpec}
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.synchronizer.config.SynchronizerParametersConfig
import com.digitalasset.canton.topology.transaction.TopologyMapping.Code
import com.digitalasset.canton.util.{BinaryFileUtil, JarResourceUtils}
import monocle.macros.syntax.lens.*

/*
This integration was created as part of the fix of https://github.com/DACH-NY/canton-network-internal/issues/1063

Bug:
The code that checked whether a new authorization brought additional signatures was using signatures and not fingerprints.
As a result, we were storing several signatures for the same key.

Test setup:
Uses non-default schemes that are non-deterministic.
 */
sealed trait DuplicateSignaturesIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SecurityTestSuite {

  // Default JCE
  private val jce: CryptoConfig = CryptoConfig(provider = CryptoProvider.Jce)

  // JCE with default ECDSA P-256 for non-deterministic signatures
  private val jceWithOnlySigEcDsaP256: CryptoConfig = jce.copy(
    signing = SigningSchemeConfig(
      algorithms = CryptoSchemeConfig(
        default = Some(SigningAlgorithmSpec.EcDsaSha256),
        allowed = Some(NonEmpty.mk(Set, SigningAlgorithmSpec.EcDsaSha256)),
      ),
      keys = CryptoSchemeConfig(
        default = Some(SigningKeySpec.EcP256),
        allowed = Some(NonEmpty.mk(Set, SigningKeySpec.EcP256)),
      ),
    )
  )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S1M1_Config
      .withNetworkBootstrap { implicit env =>
        import env.*

        val staticSynchronizerParameters = StaticSynchronizerParameters.fromConfig(
          SynchronizerParametersConfig()
            .copy(
              requiredSigningAlgorithmSpecs =
                Some(NonEmpty.mk(Set, SigningAlgorithmSpec.EcDsaSha256)),
              requiredSigningKeySpecs = Some(NonEmpty.mk(Set, SigningKeySpec.EcP256)),
            ),
          jce,
          testedProtocolVersion,
        )

        val network = NetworkTopologyDescription.createWithStaticSynchronizerParameters(
          daName,
          synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1),
          synchronizerThreshold = PositiveInt.two,
          sequencers = Seq(sequencer1),
          mediators = Seq(mediator1),
          staticSynchronizerParameters = staticSynchronizerParameters,
        )

        new NetworkBootstrapper(network)
      }
      .addConfigTransforms(
        ConfigTransforms.updateAllSequencerConfigs { case (_, config) =>
          config.focus(_.crypto).replace(jceWithOnlySigEcDsaP256)
        }
      )
      .addConfigTransforms(
        ConfigTransforms.updateAllMediatorConfigs { case (_, config) =>
          config.focus(_.crypto).replace(jceWithOnlySigEcDsaP256)
        }
      )
      .addConfigTransforms(
        ConfigTransforms.updateAllParticipantConfigs { case (_, config) =>
          config.focus(_.crypto).replace(jceWithOnlySigEcDsaP256)
        }
      )

  "Not store duplicate signatures" in { implicit env =>
    import env.*

    val currentParameters = sequencer1.topology.synchronizer_parameters.latest(daId)
    val newParameters = currentParameters.update(confirmationResponseTimeout =
      currentParameters.confirmationResponseTimeout.plusSeconds(1)
    )

    currentParameters should not be newParameters

    // Call twice with sequencer1
    Seq[LocalInstanceReference](sequencer1, sequencer1, mediator1).foreach { node =>
      node.topology.synchronizer_parameters.propose(
        daId,
        newParameters,
        store = Some(daId),
        serial = Some(PositiveInt.two),
      )
    }

    val newTx = eventually() {
      val tx = sequencer1.topology.transactions
        .list(store = daId, filterMappings = Seq(Code.SynchronizerParametersState))
        .result
        .loneElement

      tx.transaction.serial shouldBe PositiveInt.two

      tx
    }

    /*
    Expect two signatures by mediator1 and signature1, but not the second signature
    for sequencer1 from the double propose call
     */
    newTx.transaction.signatures.toSeq.size shouldBe 2
  }

  /*
  This test reads data with too many signatures and ensures they don't end up in deserialized data
   */
  "Duplicated signatures should be discarded upon deserialization" in { implicit env =>
    import env.*

    /*
    The snapshot contains two signatures by the same key of sequencer1 (generated using test case above when it was failing).
     */
    val readSnapshot = BinaryFileUtil
      .readByteStringFromFile(
        JarResourceUtils.resourceFile("topology-snapshot-duplicate-signatures").toString
      )
      .value

    val testTempStoreId =
      participant1.topology.stores
        .create_temporary_topology_store("test", testedProtocolVersion)

    participant1.topology.transactions
      .import_topology_snapshot(readSnapshot, testTempStoreId)

    val importedTx = participant1.topology.transactions
      .list(testTempStoreId, filterMappings = Seq(Code.SynchronizerParametersState))
      .result
      .loneElement

    // Two means that the additional signature for sequencer1 is discarded
    importedTx.transaction.signatures.toSeq.size shouldBe 2
  }
}

class DuplicateSignaturesIntegrationTestPostgres extends DuplicateSignaturesIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
}
