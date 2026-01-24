// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms

import better.files.File
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{KeyPurpose, SigningPublicKeyWithName}
import com.digitalasset.canton.integration.bootstrap.InitializedSynchronizer
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.security.kms.aws.AwsKmsCryptoIntegrationTestBase
import com.digitalasset.canton.integration.tests.security.kms.gcp.GcpKmsCryptoIntegrationTestBase
import com.digitalasset.canton.integration.util.AcsInspection
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.ForceFlag
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllButNamespaceDelegations
import com.digitalasset.canton.topology.transaction.{NamespaceDelegation, ParticipantPermission}

import scala.jdk.CollectionConverters.*

trait KmsMigrationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with KmsCryptoIntegrationTestBase
    with AcsInspection {

  protected val sequencerGroups: MultiSynchronizer =
    MultiSynchronizer(Seq(Set("sequencer1"), Set("sequencer2")).map(_.map(InstanceName.tryCreate)))

  /** Environment:
    *   - p1: with KMS
    *   - p2: with JCE and KMS aligned schemes
    *   - p3: default crypto config (JCE, default schemes)
    *   - da: with JCE and KMS aligned schemes
    *   - acme: default crypto config (JCE, default schemes)
    */
  override protected val environmentBaseConfig: EnvironmentDefinition =
    EnvironmentDefinition.P3S2M2_Manual

  "setup synchronizer to migrate from" in { implicit env =>
    import env.*

    participant3.start()
    sequencer2.start()
    mediator2.start()

    // check that participant1 has KMS keys
    participant1.keys.secret.list().map(_.kmsKeyId) should contain theSameElementsAs
      topologyPreDefinedKeys.forNode(participant1).productIterator.toSeq

    // check that participant3 has no KMS Keys
    participant3.keys.secret.list().map(_.kmsKeyId).forall(_.isEmpty) shouldBe true

    // initialize synchronizer2 (synchronizer1 is initialized in KmsCryptoIntegrationTestBase)
    val synchronizerId = bootstrap.synchronizer(
      acmeName.unwrap,
      synchronizerOwners = Seq(sequencer2),
      synchronizerThreshold = PositiveInt.one,
      sequencers = Seq(sequencer2),
      mediators = Seq(mediator2),
      staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
    )

    env.initializedSynchronizers.put(
      acmeName,
      InitializedSynchronizer(
        synchronizerId,
        EnvironmentDefinition.defaultStaticSynchronizerParameters.toInternal,
        synchronizerOwners = Set(sequencer2),
      ),
    )

    sequencer2.health.wait_for_initialized()

    participant3.synchronizers.connect_local(sequencer2, alias = acmeName)

    Set(participant1, participant3).foreach { p =>
      p.dars.upload(CantonExamplesPath)
    }

    val alice = participant3.parties.enable("Alice", synchronizer = acmeName)
    val bob = participant3.parties.enable("Bob", synchronizer = acmeName)

    // Create some contract for alice and bob on p3
    val (obligor, owner, participant) = (alice, bob, participant3)
    IouSyntax.createIou(participant)(obligor, owner)
  }

  "setup namespace delegation" in { implicit env =>
    import env.*

    val participantOld = participant3
    val participantNew = participant1

    // user-manual-entry-begin: KmsSetupNamespaceDelegation
    val namespaceOld = participantOld.namespace
    val namespaceNew = participantNew.namespace

    val rootNamespaceDelegationOld = participantOld.topology.transactions
      .list(filterAuthorizedKey = Some(namespaceOld.fingerprint))
      .result
      .map(_.transaction)
      .filter(_.mapping.code == NamespaceDelegation.code)
      .head

    val namespaceKeyNew = participantNew.keys.public.download(namespaceNew.fingerprint)
    participantOld.keys.public.upload(namespaceKeyNew, Some("pNew-namespace-key"))

    val namespaceNewSigningKey = participantNew.keys.public
      .list(
        filterFingerprint = participantNew.fingerprint.unwrap,
        filterPurpose = Set(KeyPurpose.Signing),
      )
      .map(_.asInstanceOf[SigningPublicKeyWithName].publicKey)
      .loneElement

    // Delegate namespace of old participant to new participant
    val delegation = participantOld.topology.namespace_delegations.propose_delegation(
      namespace = namespaceOld,
      targetKey = namespaceNewSigningKey,
      CanSignAllButNamespaceDelegations,
    )

    participantNew.topology.transactions
      .load(
        Seq(rootNamespaceDelegationOld, delegation),
        TopologyStoreId.Authorized,
        ForceFlag.AlienMember,
      )
  // user-manual-entry-end: KmsSetupNamespaceDelegation
  }

  "re-create Alice/Bob on new participant" in { implicit env =>
    import env.*

    val participantOld = participant3
    val participantNew = participant1
    val newKmsSynchronizerAlias = daName

    // user-manual-entry-begin: KmsRecreatePartiesInNewParticipant
    val parties = participantOld.parties.list().map(_.party)

    parties.foreach { party =>
      participantNew.topology.party_to_participant_mappings
        .propose(
          party = party,
          newParticipants = Seq(participantNew.id -> ParticipantPermission.Submission),
          store = daId,
        )
    }

    // Disconnect from new KMS-compatible synchronizer to prepare migration of parties and contracts
    participantNew.synchronizers.disconnect(newKmsSynchronizerAlias)
  // user-manual-entry-end: KmsRecreatePartiesInNewParticipant
  }

  "export and import ACS for Alice/Bob" in { implicit env =>
    import env.*

    val participantOld = participant3
    val participantNew = participant1

    val aliceOld = participantOld.parties.find("Alice")
    val bobOld = participantOld.parties.find("Bob")

    val oldSynchronizerSequencer = sequencer2
    val oldSynchronizerAlias = acmeName
    val newSynchronizerAlias = daName

    val oldSynchronizerMediator = mediator2

    val oldSynchronizerId = acmeId
    val newKmsSynchronizerId = daId

    // user-manual-entry-begin: KmsMigrateACSofParties
    val parties = participantOld.parties.list().map(_.party)

    // Make sure synchronizer and the old participant are quiet before exporting ACS
    participantOld.synchronizers.disconnect(oldSynchronizerAlias)
    oldSynchronizerMediator.stop()
    oldSynchronizerSequencer.stop()

    File.usingTemporaryFile("participantOld-acs", suffix = ".txt") { acsFile =>
      val acsFileName = acsFile.toString

      val ledgerEnd = participantOld.ledger_api.state.end()

      // Export from old participant
      participantOld.repair.export_acs(
        parties = parties.toSet,
        exportFilePath = acsFileName,
        ledgerOffset = ledgerEnd,
        contractSynchronizerRenames = Map(oldSynchronizerId.logical -> newKmsSynchronizerId.logical),
      )

      // Import to new participant
      participantNew.repair.import_acsV2(acsFileName, newKmsSynchronizerId.logical)
    }

    // Kill/stop the old participant
    participantOld.stop()

    // Connect the new participant to the new synchronizer
    participantNew.synchronizers.reconnect(newSynchronizerAlias)
    // user-manual-entry-end: KmsMigrateACSofParties

    // wait for participantNew to full onboard and process all topology transactions from the synchronizer
    val (aliceP1, bobP1) = eventually(retryOnTestFailuresOnly = false) {
      (participantNew.parties.find("Alice"), participantNew.parties.find("Bob"))
    }

    aliceP1 shouldEqual aliceOld
    bobP1 shouldEqual bobOld

    eventually() {
      participantNew.ledger_api.state.acs.of_party(aliceP1) should not be empty
      participantNew.ledger_api.state.acs.of_party(bobP1) should not be empty
    }

    participantNew.health.ping(participant2)
  }

  "test that ACS of Alice/Bob works on new participant" in { implicit env =>
    import env.*

    val alice = participant1.parties.find("Alice")
    val bob = participant1.parties.find("Bob")

    val (obligor, owner, participant) = (alice, bob, participant1)

    val iou = findIOU(
      participant,
      owner,
      contract =>
        contract.data.owner == owner.toProtoPrimitive && contract.data.payer == obligor.toProtoPrimitive,
    )

    // Transfer contract originally created on p3 from bob to alice
    participant.ledger_api.javaapi.commands
      .submit(
        Seq(owner),
        iou.id.exerciseTransfer(obligor.toProtoPrimitive).commands.asScala.toSeq,
      )

    // Alice creates a new contract directly with bob
    IouSyntax.createIou(participant)(obligor, owner)
  }
}

class AwsKmsMigrationBftOrderingIntegrationTestPostgres
    extends KmsMigrationIntegrationTest
    with AwsKmsCryptoIntegrationTestBase {

  setupPlugins(
    withAutoInit = false,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseBftSequencer(loggerFactory, sequencerGroups),
  )

}

class GcpKmsMigrationBftOrderingBlockIntegrationTestPostgres
    extends KmsMigrationIntegrationTest
    with GcpKmsCryptoIntegrationTestBase {

  setupPlugins(
    withAutoInit = false,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseBftSequencer(loggerFactory, sequencerGroups),
  )

}
