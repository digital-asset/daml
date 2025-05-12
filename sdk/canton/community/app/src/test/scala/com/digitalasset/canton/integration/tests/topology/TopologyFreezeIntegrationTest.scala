// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.crypto.SigningKeyUsage
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllMappings
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, TopologyManagerError}

sealed trait TopologyFreezeIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P0_S1M1

  "freezing the topology does not permit further topology transactions" in { implicit env =>
    import env.*

    synchronizerOwners1.foreach { owner =>
      owner.topology.synchronizer_migration.topology_freezes.propose_freeze(
        PhysicalSynchronizerId(daId, testedProtocolVersion)
      )
    }

    val owner1 = synchronizerOwners1.headOption.value
    val targetKey = owner1.keys.secret.generate_signing_key(usage = SigningKeyUsage.NamespaceOnly)

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      owner1.topology.namespace_delegations
        .propose_delegation(owner1.namespace, targetKey, CanSignAllMappings, daId),
      _ shouldBeCantonErrorCode (TopologyManagerError.TopologyFreezeActive),
    )
  }

  "the topology state can be unfrozen again" in { implicit env =>
    import env.*
    synchronizerOwners1.foreach(
      _.topology.synchronizer_migration.topology_freezes
        .propose_unfreeze(PhysicalSynchronizerId(daId, testedProtocolVersion))
    )

    val owner1 = synchronizerOwners1.headOption.value
    val targetKey = owner1.keys.secret.generate_signing_key(usage = SigningKeyUsage.NamespaceOnly)
    owner1.topology.namespace_delegations
      .propose_delegation(owner1.namespace, targetKey, CanSignAllMappings, daId)

    eventually() {
      owner1.topology.namespace_delegations
        .list(daId, filterTargetKey = Some(targetKey.fingerprint)) should have size 1
    }
  }
}

class TopologyFreezeIntegrationTestPostgres extends TopologyFreezeIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
