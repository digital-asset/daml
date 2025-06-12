// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.crypto.SigningKeyUsage
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore
import com.digitalasset.canton.sequencing.GrpcSequencerConnection
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllMappings
import com.digitalasset.canton.topology.{
  KnownPhysicalSynchronizerId,
  PhysicalSynchronizerId,
  SequencerId,
  TopologyManagerError,
  UnknownPhysicalSynchronizerId,
}
import com.google.protobuf.ByteString

import java.net.URI

sealed trait LogicalSynchronizerUpgradeTopologyIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S2M2

  private def successorSynchronizerId(implicit env: TestConsoleEnvironment) =
    env.daId.copy(serial = NonNegativeInt.one)

  "migration announcement does not permit further topology transactions" in { implicit env =>
    import env.*

    synchronizerOwners1.foreach { owner =>
      owner.topology.synchronizer_upgrade.announcement.propose(
        daId,
        PhysicalSynchronizerId(daId, testedProtocolVersion, NonNegativeInt.one),
      )
    }

    val owner1 = synchronizerOwners1.headOption.value
    val targetKey = owner1.keys.secret.generate_signing_key(usage = SigningKeyUsage.NamespaceOnly)

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      owner1.topology.namespace_delegations
        .propose_delegation(owner1.namespace, targetKey, CanSignAllMappings, daId),
      _ shouldBeCantonErrorCode (TopologyManagerError.OngoingSynchronizerUpgrade),
    )
  }

  "the topology state can be unfrozen again" in { implicit env =>
    import env.*
    synchronizerOwners1.foreach(
      _.topology.synchronizer_upgrade.announcement.revoke(
        daId,
        PhysicalSynchronizerId(daId, testedProtocolVersion, NonNegativeInt.one),
      )
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

  "sequencers announce their success endpoints" in { implicit env =>
    import env.*
    val sequencer1Alias = SequencerAlias.create("sequencer1").value
    val sequencer2Alias = SequencerAlias.create("sequencer2").value

    // participant1 has a single sequencer connection
    participant1.synchronizers.connect_local(sequencer1, daName)

    // participant2 is connected to two sequencers
    participant2.synchronizers.connect_local_bft(
      synchronizer = NonEmpty(
        Map,
        sequencer1Alias -> sequencer1,
        sequencer2Alias -> sequencer2,
      ),
      alias = daName,
      sequencerTrustThreshold = PositiveInt.two,
    )

    // announce the migration to prepare for the sequencer connection announcements
    synchronizerOwners1.foreach(
      _.topology.synchronizer_upgrade.announcement.propose(
        daId,
        successorSynchronizerId,
      )
    )

    // sequencer1 announces its connection details for the successor synchronizer
    sequencer1.topology.synchronizer_upgrade.sequencer_successors.propose_successor(
      sequencer1.id,
      endpoints = NonEmpty(Seq, new URI("https://localhost:5000")),
      daId,
      customTrustCertificates = Some(ByteString.copyFromUtf8("test")),
    )
    // check that participant1 automatically created a synchronizer config for the successor synchronizer
    // with a connection to the same sequencer alias/id
    checkMigratedSequencerConfig(participant1, sequencer1.id -> 5000)

    // check that participant2 has not created any configs yet
    connectionConfigStore(participant2)
      .get(daName, UnknownPhysicalSynchronizerId)
      .toOption shouldBe None
    connectionConfigStore(participant2)
      .get(daName, KnownPhysicalSynchronizerId(successorSynchronizerId))
      .toOption shouldBe None

    // sequencer2 announces its connection details for the successor synchronizer
    sequencer2.topology.synchronizer_upgrade.sequencer_successors.propose_successor(
      sequencer2.id,
      endpoints = NonEmpty(Seq, new URI("http://localhost:6000"), new URI("http://localhost:7000")),
      daId,
    )
    // check that participant2 automatically created a synchronizer config for the successor synchronizer
    // with a connection to the same sequencer alias/id
    checkMigratedSequencerConfig(
      participant2,
      sequencer1.id -> 5000,
      sequencer2.id -> 6000,
      sequencer2.id -> 7000,
    )

    // sequencer2 changes its connection details for the successor synchronizer
    sequencer2.topology.synchronizer_upgrade.sequencer_successors.propose_successor(
      sequencer2.id,
      endpoints = NonEmpty(Seq, new URI("http://localhost:6000")),
      daId,
    )
    // check that participant2 updated the synchronizer config for the successor synchronizer
    // for sequencer2
    checkMigratedSequencerConfig(participant2, sequencer1.id -> 5000, sequencer2.id -> 6000)

    // sequencer1 changes its connection details for the successor synchronizer
    sequencer1.topology.synchronizer_upgrade.sequencer_successors.propose_successor(
      sequencer1.id,
      endpoints = NonEmpty(Seq, new URI("https://localhost:5005")),
      daId,
      customTrustCertificates = Some(ByteString.copyFromUtf8("test")),
    )
    // check that the participants automatically modified their synchronizer configs for the successor synchronizer
    // according to the latest sequencer connection updates
    checkMigratedSequencerConfig(participant1, sequencer1.id -> 5005)
    checkMigratedSequencerConfig(participant2, sequencer1.id -> 5005, sequencer2.id -> 6000)

  }

  private def connectionConfigStore(participant: LocalParticipantReference) =
    participant.underlying.value.sync.synchronizerConnectionConfigStore

  private def checkMigratedSequencerConfig(
      participant: LocalParticipantReference,
      expectedSequencerPorts: (SequencerId, Int)*
  )(implicit env: TestConsoleEnvironment) = {
    import env.*
    val portMap = expectedSequencerPorts.groupBy(_._1).view.mapValues(_.map(_._2)).toMap
    eventually() {
      val configStore = connectionConfigStore(participant)
      val currentConfig =
        configStore.get(daName, KnownPhysicalSynchronizerId(daId)).value
      currentConfig.status shouldBe SynchronizerConnectionConfigStore.Active
      val successorConfig =
        configStore.get(daName, KnownPhysicalSynchronizerId(successorSynchronizerId)).value
      successorConfig.status shouldBe SynchronizerConnectionConfigStore.MigratingTo

      val currentSequencers = currentConfig.config.sequencerConnections.aliasToConnection.map {
        case (seqAlias, conn) =>
          seqAlias -> conn.sequencerId.value
      }
      forAll(currentSequencers.forgetNE) { case (seqAlias, seqId) =>
        val successorSequencerConfig = successorConfig.config.sequencerConnections.aliasToConnection
          .get(seqAlias)
          .value
          .asInstanceOf[GrpcSequencerConnection]
        successorSequencerConfig.sequencerId.value shouldBe seqId
        successorSequencerConfig.endpoints.forgetNE
          .map(_.port.unwrap) should contain theSameElementsAs (portMap(seqId))
      }
    }
  }

}

class LogicalSynchronizerUpgradeTopologyIntegrationTestPostgres
    extends LogicalSynchronizerUpgradeTopologyIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
