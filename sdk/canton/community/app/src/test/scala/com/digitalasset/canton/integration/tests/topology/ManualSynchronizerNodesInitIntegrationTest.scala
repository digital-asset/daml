// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.LocalInstanceReference
import com.digitalasset.canton.crypto.SigningKeyUsage
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllMappings
import com.digitalasset.canton.topology.{Namespace, UniqueIdentifier}

/** Test to fully manually initialize synchronizer nodes with identity and topology keys. */
trait ManualSynchronizerNodesInitIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S1M1_Manual
      .addConfigTransform(ConfigTransforms.disableAutoInit(Set("sequencer1", "mediator1")))

  private def nodeInit(node: LocalInstanceReference): Unit = {
    // create namespace key for the node
    val namespaceKey = node.keys.secret
      .generate_signing_key(
        s"${node.name}-${SigningKeyUsage.Namespace.identifier}",
        usage = SigningKeyUsage.NamespaceOnly,
      )

    node.health.wait_for_ready_for_id()

    // initialize the node id
    node.topology.init_id_from_uid(
      UniqueIdentifier.tryCreate(node.name, namespaceKey.fingerprint)
    )

    node.health.wait_for_ready_for_node_topology()

    node.topology.namespace_delegations.propose_delegation(
      Namespace(namespaceKey.fingerprint),
      namespaceKey,
      CanSignAllMappings,
    )

    // every node needs to create a signing key
    val protocolSigningKey = node.keys.secret
      .generate_signing_key(
        s"${node.name}-${SigningKeyUsage.Protocol.identifier}",
        usage = SigningKeyUsage.ProtocolOnly,
      )

    // create a sequencer authentication signing key for the mediator
    val sequencerAuthKey = node.keys.secret
      .generate_signing_key(
        s"${node.name}-${SigningKeyUsage.SequencerAuthentication.identifier}",
        usage = SigningKeyUsage.SequencerAuthenticationOnly,
      )

    val keys = NonEmpty(Seq, protocolSigningKey, sequencerAuthKey)

    node.topology.owner_to_key_mappings.propose(
      member = node.id.member,
      keys = keys,
      signedBy = (namespaceKey +: keys).map(_.fingerprint),
    )

    node.health.wait_for_ready_for_initialization()

  }

  "manually initialize the mediator node" in { implicit env =>
    import env.*

    mediator1.start()
    nodeInit(mediator1)
  }

  "manually initialize the sequencer node" in { implicit env =>
    import env.*

    sequencer1.start()
    nodeInit(sequencer1)
  }

  "bootstrap the synchronizer and run a ping" in { implicit env =>
    import env.*

    bootstrap.synchronizer(
      "synchronizer1",
      sequencers = Seq(sequencer1),
      mediators = Seq(mediator1),
      synchronizerOwners = Seq(sequencer1),
      synchronizerThreshold = PositiveInt.one,
      staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
    )

    participant1.start()
    participant1.synchronizers.connect_local(sequencer1, "synchronizer1")
    participant1.health.ping(participant1)
  }

}

class ManualSynchronizerNodesInitIntegrationTestPostgres
    extends ManualSynchronizerNodesInitIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
