// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DbConfig, StorageConfig}
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UseH2,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.{
  IdentifierDelegation,
  NamespaceDelegation,
  OwnerToKeyMapping,
}
import monocle.macros.syntax.lens.*

trait ManualParticipantSetupIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S1M1_Manual
      .addConfigTransforms(
        ConfigTransforms.updateParticipantConfig("participant2")(
          _.focus(_.init.identity).replace(None)
        ),
        ConfigTransforms.updateParticipantConfig("participant3")(
          _.focus(_.init.identity).replace(None)
        ),
      )

  "create a participant based on the identity of another participant" in { implicit env =>
    import env.*
    val staticParameters =
      StaticSynchronizerParameters.defaults(sequencer1.config.crypto, testedProtocolVersion)
    sequencer1.start()
    mediator1.start()

    bootstrap.synchronizer(
      "test-synchronizer",
      sequencers = Seq(sequencer1),
      mediators = Seq(mediator1),
      synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1),
      synchronizerThreshold = PositiveInt.two,
      staticParameters,
    )
    participant1.start()
    participant1.synchronizers.connect_local(sequencer1, daName)

    val id = participant1.id

    val keys = participant1.keys.secret.list().map { k =>
      k.name -> participant1.keys.secret.download(k.publicKey.fingerprint)
    }

    val codes = Set(NamespaceDelegation.code, OwnerToKeyMapping.code, IdentifierDelegation.code)
    val bootstrapTxs = participant1.topology.transactions
      .list(filterAuthorizedKey = Some(id.fingerprint))
      .result
      .map(_.transaction)
      .filter(x => codes.contains(x.mapping.code))

    participant1.synchronizers.disconnect_all()
    participant2.start()

    keys.foreach { case (name, key) =>
      participant2.keys.secret.upload(key, name.map(_.unwrap))
    }
    participant2.topology.init_id(id.uid)

    participant2.health.wait_for_ready_for_node_topology()
    participant2.topology.transactions.load(bootstrapTxs, TopologyStoreId.Authorized)

    participant2.health.wait_for_initialized()

    participant2.synchronizers.connect_local(sequencer1, daName)
  }
}

class ManualParticipantSetupIntegrationTestInMemory extends ManualParticipantSetupIntegrationTest {
  registerPlugin(new UseCommunityReferenceBlockSequencer[StorageConfig.Memory](loggerFactory))
}

class ManualParticipantSetupIntegrationTestPostgres extends ManualParticipantSetupIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

class ManualParticipantSetupIntegrationTestH2 extends ManualParticipantSetupIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))
}
