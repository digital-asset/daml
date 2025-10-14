// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.crypto.SigningKeyUsage
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.sequencing.{GrpcSequencerConnection, SequencerConnections}
import com.digitalasset.canton.topology.TopologyManagerError.InvalidSynchronizerSuccessor
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllMappings
import com.digitalasset.canton.topology.{
  KnownPhysicalSynchronizerId,
  PhysicalSynchronizerId,
  SequencerId,
  TopologyManagerError,
  UnknownPhysicalSynchronizerId,
}
import com.google.protobuf.ByteString
import monocle.syntax.all.*
import org.scalatest.Assertion

import java.net.URI
import java.util.concurrent.atomic.AtomicReference

sealed trait LogicalSynchronizerUpgradeTopologyIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S2M2
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.automaticallyPerformLogicalSynchronizerUpgrade).replace(false)
        )
      )
      .withSetup { env =>
        latestSuccessorPSId.set(Some(env.daId))
      }

  /*
  PSId of the successor needs to be strictly increasing with different announcements.
  This allows to track the latest used.
   */
  private val latestSuccessorPSId = new AtomicReference[Option[PhysicalSynchronizerId]](None)

  private def allocateSuccessorPSId(): PhysicalSynchronizerId =
    latestSuccessorPSId.updateAndGet { existing =>
      Some(existing.value.copy(serial = existing.value.serial.increment.toNonNegative))
    }.value

  private lazy val upgradeTime = CantonTimestamp.now().plusSeconds(3600)

  "migration announcement does not permit further topology transactions" in { implicit env =>
    import env.*

    val successorPSId = allocateSuccessorPSId()
    synchronizerOwners1.foreach { owner =>
      owner.topology.synchronizer_upgrade.announcement.propose(
        successorPhysicalSynchronizerId = successorPSId,
        upgradeTime = upgradeTime,
      )
    }

    val owner1 = synchronizerOwners1.headOption.value
    val targetKey = owner1.keys.secret.generate_signing_key(usage = SigningKeyUsage.NamespaceOnly)

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      owner1.topology.namespace_delegations
        .propose_delegation(owner1.namespace, targetKey, CanSignAllMappings, daId),
      _.shouldBeCantonErrorCode(TopologyManagerError.OngoingSynchronizerUpgrade),
    )
  }

  "the topology state can be unfrozen again" in { implicit env =>
    import env.*
    synchronizerOwners1.foreach(
      _.topology.synchronizer_upgrade.announcement.revoke(
        successorPhysicalSynchronizerId = latestSuccessorPSId.get().value,
        upgradeTime = upgradeTime,
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

    // participant1 has a single sequencer connection
    participant1.synchronizers.connect_local(sequencer1, daName)

    // participant2 is connected to two sequencers
    participant2.synchronizers.connect_local_bft(
      sequencers = Seq(sequencer1, sequencer2),
      synchronizerAlias = daName,
      sequencerTrustThreshold = PositiveInt.two,
    )

    // announce the migration to prepare for the sequencer connection announcements
    val successorPSId = allocateSuccessorPSId()
    synchronizerOwners1.foreach(
      _.topology.synchronizer_upgrade.announcement.propose(
        successorPhysicalSynchronizerId = successorPSId,
        upgradeTime = upgradeTime,
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
    checkUpgradedSequencerConfig(participant1, sequencer1.id -> 5000)

    // check that participant2 has not created any configs yet
    connectionConfigStore(participant2)
      .get(daName, UnknownPhysicalSynchronizerId)
      .toOption shouldBe None
    connectionConfigStore(participant2)
      .get(daName, KnownPhysicalSynchronizerId(successorPSId))
      .toOption shouldBe None

    // sequencer2 announces its connection details for the successor synchronizer
    sequencer2.topology.synchronizer_upgrade.sequencer_successors.propose_successor(
      sequencer2.id,
      endpoints = NonEmpty(Seq, new URI("http://localhost:6000"), new URI("http://localhost:7000")),
      daId,
    )
    // check that participant2 automatically created a synchronizer config for the successor synchronizer
    // with a connection to the same sequencer alias/id
    checkUpgradedSequencerConfig(
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
    checkUpgradedSequencerConfig(participant2, sequencer1.id -> 5000, sequencer2.id -> 6000)

    // sequencer1 changes its connection details for the successor synchronizer
    sequencer1.topology.synchronizer_upgrade.sequencer_successors.propose_successor(
      sequencer1.id,
      endpoints = NonEmpty(Seq, new URI("https://localhost:5005")),
      daId,
      customTrustCertificates = Some(ByteString.copyFromUtf8("test")),
    )
    // check that the participants automatically modified their synchronizer configs for the successor synchronizer
    // according to the latest sequencer connection updates
    checkUpgradedSequencerConfig(participant1, sequencer1.id -> 5005)
    checkUpgradedSequencerConfig(participant2, sequencer1.id -> 5005, sequencer2.id -> 6000)

  }

  // this test simulates an automation that is part of a logical synchronizer upgrade
  "participants should be able to connect to a physical synchronizer and just perform a handshake" in {
    implicit env =>
      import env.*

      // disable the topology freeze, otherwise the participant cannot onboard.
      // During an actual LSU, this wouldn't be needed, because the participant would perform the handshake with the
      // unfrozen successor synchronizer
      synchronizerOwners1.foreach(
        _.topology.synchronizer_upgrade.announcement.revoke(
          latestSuccessorPSId.get().value,
          upgradeTime = upgradeTime,
        )
      )

      // register the synchronizer config.
      // During an actual LSU, this wouldn't be needed, because the successor listener would automatically create the
      // config.
      participant3.synchronizers.register_by_config(
        SynchronizerConnectionConfig(
          daName,
          SequencerConnections.single(sequencer1.sequencerConnection),
          synchronizerId = Some(daId),
        ),
        performHandshake = false,
      )
      // manually set the physical synchronizer id.
      // During an actual LSU, this would be set by the participant when automatically
      // setting up the successor synchronizer connection configuration.
      participant3.underlying.value.sync.synchronizerConnectionConfigStore
        .setPhysicalSynchronizerId(daName, daId)
        .futureValueUS
        .discard
      // Perform a manual handshake that just downloads the topology state.
      // During an actual LSU, the participant would make this call after every announcement. Here, we just want to test
      // whether the call works as expected.
      participant3.underlying.value.sync
        .connectToPSIdWithHandshake(daId)
        .futureValueUS

      eventually() {
        participant3.topology.sequencers.list(daId) should not be empty
      }
  }

  "successor PSId should increase between announcements" in { implicit env =>
    import env.*

    val successor1 = allocateSuccessorPSId()
    val successor2 = allocateSuccessorPSId()
    val successor3 = allocateSuccessorPSId()

    Seq(successor1, successor2).foreach { successor =>
      synchronizerOwners1.foreach { owner =>
        owner.topology.synchronizer_upgrade.announcement.propose(
          successorPhysicalSynchronizerId = successor,
          upgradeTime = upgradeTime,
        )
      }

      synchronizerOwners1.foreach { owner =>
        owner.topology.synchronizer_upgrade.announcement.revoke(
          successorPhysicalSynchronizerId = successor,
          upgradeTime = upgradeTime,
        )
      }
    }

    // Re-using successor1 or successor2 should fail
    Seq(successor1, successor2).foreach { successor =>
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        sequencer1.topology.synchronizer_upgrade.announcement.propose(
          successorPhysicalSynchronizerId = successor,
          upgradeTime = upgradeTime,
        ),
        entry => {
          entry shouldBeCantonErrorCode (InvalidSynchronizerSuccessor)
          entry.errorMessage should include(
            InvalidSynchronizerSuccessor.Reject
              .conflictWithPreviousAnnouncement(successor, successor2)
              .cause
          )
        },
      )
    }

    // But successor3 should be fine
    sequencer1.topology.synchronizer_upgrade.announcement.propose(
      successorPhysicalSynchronizerId = successor3,
      upgradeTime = upgradeTime,
    )
  }

  private def connectionConfigStore(participant: LocalParticipantReference) =
    participant.underlying.value.sync.synchronizerConnectionConfigStore

  private def checkUpgradedSequencerConfig(
      participant: LocalParticipantReference,
      expectedSequencerPorts: (SequencerId, Int)*
  )(implicit env: TestConsoleEnvironment): Assertion = {
    import env.*
    val portMap = expectedSequencerPorts.groupBy(_._1).view.mapValues(_.map(_._2)).toMap
    eventually() {
      val configStore = connectionConfigStore(participant)
      val currentConfig =
        configStore.get(daName, KnownPhysicalSynchronizerId(daId)).value
      currentConfig.status shouldBe SynchronizerConnectionConfigStore.Active
      val successorConfig =
        configStore.get(daName, KnownPhysicalSynchronizerId(latestSuccessorPSId.get().value)).value
      successorConfig.status shouldBe SynchronizerConnectionConfigStore.UpgradingTarget

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
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
