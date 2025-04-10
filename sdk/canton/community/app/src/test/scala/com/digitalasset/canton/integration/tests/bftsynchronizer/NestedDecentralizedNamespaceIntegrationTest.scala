// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.bftsynchronizer

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.ConsoleEnvironment.Implicits.*
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.topology.transaction.DecentralizedNamespaceDefinition
import com.digitalasset.canton.topology.{Namespace, TopologyManagerError}

class NestedDecentralizedNamespaceIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S2M2.withSetup { implicit env =>
      import env.*

      participants.all.synchronizers.connect_local(sequencer1, daName)
    }

  def createDecentralizedNamespace(
      owner: Namespace,
      authorizer: InstanceReference,
  )(implicit env: TestConsoleEnvironment): DecentralizedNamespaceDefinition = {
    logger.debug(
      s"Creating decentralized namespace for owner $owner with authorizer ${authorizer.id}"
    )
    val decentralizedNamespaceDef = authorizer.topology.decentralized_namespaces
      .propose_new(
        owners = Set(owner),
        threshold = PositiveInt.one,
        store = env.daId,
        mustFullyAuthorize = true,
      )
      .transaction
      .mapping
    assert(decentralizedNamespaceDef.namespace != owner)
    assert(decentralizedNamespaceDef.namespace != authorizer.namespace)
    decentralizedNamespaceDef
  }

  def assertDecentralizedNamespaceWithSerial(
      instance: InstanceReference,
      decentralizedNamespace: Namespace,
      serial: PositiveInt,
  )(implicit env: TestConsoleEnvironment): Unit =
    eventually() {
      val decentralizedNamespaces = instance.topology.decentralized_namespaces
        .list(
          store = env.daId,
          filterNamespace = decentralizedNamespace.filterString,
        )
      decentralizedNamespaces.exists(_.context.serial == serial) shouldBe true
    }

  def joinDecentralizedNamespace(
      decentralizedNamespace: DecentralizedNamespaceDefinition,
      owner: InstanceReference,
      joiningNamespace: Namespace,
      joiner: InstanceReference,
      serial: PositiveInt,
  )(implicit env: TestConsoleEnvironment): DecentralizedNamespaceDefinition = {
    val updated = DecentralizedNamespaceDefinition
      .create(
        decentralizedNamespace.namespace,
        PositiveInt.one,
        decentralizedNamespace.owners.incl(joiningNamespace),
      )
      .value
    Seq(owner, joiner).distinct.foreach { instance =>
      instance.topology.decentralized_namespaces.propose(
        decentralizedNamespace = updated,
        store = env.daId,
        signedBy = Seq(instance.fingerprint),
        serial = Some(serial),
      )
    }
    assertDecentralizedNamespaceWithSerial(owner, decentralizedNamespace.namespace, serial)
    assertDecentralizedNamespaceWithSerial(joiner, decentralizedNamespace.namespace, serial)
    updated
  }

  "cannot create a nested decentralized namespace" in { implicit env =>
    import env.*

    val decentralizedNamespace1Def =
      createDecentralizedNamespace(participant1.namespace, participant1)
    val decentralizedNamespace2Def =
      createDecentralizedNamespace(participant2.namespace, participant2)

    assertThrowsAndLogsCommandFailures(
      joinDecentralizedNamespace(
        decentralizedNamespace1Def,
        participant1,
        decentralizedNamespace2Def.namespace,
        participant2,
        PositiveInt.tryCreate(2),
      ),
      _.commandFailureMessage should (include(
        TopologyManagerError.InvalidTopologyMapping.id
      ) and include(s"No root certificate found for ${decentralizedNamespace2Def.namespace}")),
    )
  }

  "cannot create a cycles when creating a new or joining an existing decentralized namespace" in {
    implicit env =>
      import env.*

      val decentralizedNamespace3Def =
        createDecentralizedNamespace(participant3.namespace, participant3)

      clue("creating a decentralized namespace with a decentralized namespace as owner") {
        assertThrowsAndLogsCommandFailures(
          createDecentralizedNamespace(decentralizedNamespace3Def.namespace, participant3),
          _.commandFailureMessage should (include(
            TopologyManagerError.InvalidTopologyMapping.id
          ) and include(s"No root certificate found for ${decentralizedNamespace3Def.namespace}")),
        )
      }

      clue("a decentralized namespace joins as its own owner") {
        assertThrowsAndLogsCommandFailures(
          joinDecentralizedNamespace(
            decentralizedNamespace3Def,
            participant3,
            decentralizedNamespace3Def.namespace,
            participant3,
            PositiveInt.tryCreate(2),
          ),
          _.commandFailureMessage should (include(
            TopologyManagerError.InvalidTopologyMapping.id
          ) and include(s"No root certificate found for ${decentralizedNamespace3Def.namespace}")),
        )
      }
  }

}
