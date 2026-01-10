// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.admin.api.client.data.ParticipantStatus.SubmissionReady
import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{CryptoConfig, CryptoProvider}
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.crypto.SigningKeyUsage
import com.digitalasset.canton.integration.bootstrap.InitializedSynchronizer
import com.digitalasset.canton.integration.plugins.UseBftSequencer
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.admin.PingService
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllButNamespaceDelegations
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import org.slf4j.event.Level

class JceCryptoBftOrderingIntegrationTest
    extends CryptoIntegrationTest(CryptoConfig(provider = CryptoProvider.Jce))
    with ReconnectSynchronizerAutoInitIntegrationTest {
  registerPlugin(
    new UseBftSequencer(loggerFactory, sequencerGroups)
  )
}

abstract class CryptoIntegrationTest(cryptoConfig: CryptoConfig)
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S2M2_Manual
      .addConfigTransforms(
        ConfigTransforms.setCrypto(cryptoConfig)
      )
      // force sequencer2 to use the same publicApi port as sequencer1
      .addConfigTransform { config =>
        val seq1 = InstanceName.tryCreate("sequencer1")
        val seq2 = InstanceName.tryCreate("sequencer2")
        import monocle.macros.syntax.lens.*
        config
          .focus(_.sequencers)
          .index(seq2)
          .modify(
            _.focus(_.publicApi.internalPort)
              .replace(config.sequencers(seq1).publicApi.internalPort)
          )
      }
      .withSetup { implicit env =>
        import env.*

        participants.local.start()
        sequencer1.start()
        mediator1.start()

        val staticSynchronizerParameters = StaticSynchronizerParameters.defaults(
          sequencer1.config.crypto,
          testedProtocolVersion,
        )

        // initialize the first synchronizer where the 'regular' tests will be run
        val synchronizerId = bootstrap.synchronizer(
          daName.unwrap,
          sequencers = Seq(sequencer1),
          mediators = Seq(mediator1),
          synchronizerOwners = Seq(sequencer1),
          synchronizerThreshold = PositiveInt.one,
          staticSynchronizerParameters,
        )

        env.initializedSynchronizers.put(
          daName,
          InitializedSynchronizer(
            synchronizerId,
            staticSynchronizerParameters.toInternal,
            synchronizerOwners = Set(sequencer1),
          ),
        )

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
      }

  s"With crypto provider ${cryptoConfig.provider}" should {

    "make a ping" in { implicit env =>
      import env.*

      eventually() {
        assert(
          participant1.synchronizers
            .list_connected()
            .map(_.synchronizerAlias.unwrap)
            .contains(daName.unwrap)
        )
        assert(
          participant2.synchronizers
            .list_connected()
            .map(_.synchronizerAlias.unwrap)
            .contains(daName.unwrap)
        )
      }

      loggerFactory.suppress(
        SuppressionRule.forLogger[PingService] && SuppressionRule.Level(Level.WARN)
      ) {
        eventually() {
          assertPingSucceeds(participant1, participant2)
        }
      }

    }

    // exporting private keys is not allowed when using a KMS provider
    if (cryptoConfig.provider != CryptoProvider.Kms) {
      "export and import private keys pair offline" in { implicit env =>
        import env.*
        // architecture-handbook-entry-begin: ExportKeyForOfflineStorage
        // fingerprint of namespace giving key
        val participantId = participant1.id
        val namespace = participantId.fingerprint

        // create new key
        val name = "new-identity-key"
        val targetKey =
          participant1.keys.secret
            .generate_signing_key(name = name, usage = SigningKeyUsage.NamespaceOnly)

        // create an intermediate certificate authority through a namespace delegation
        // we do this by adding a new namespace delegation for the newly generated key
        // and we sign this using the root namespace key
        participant1.topology.namespace_delegations.propose_delegation(
          participantId.namespace,
          targetKey,
          CanSignAllButNamespaceDelegations,
        )

        // export namespace key to file for offline storage, in this example, it's a temporary file
        better.files.File.usingTemporaryFile("namespace", ".key") { privateKeyFile =>
          participant1.keys.secret.download_to(namespace, privateKeyFile.toString)

          // delete namespace key (very dangerous ...)
          participant1.keys.secret.delete(namespace, force = true)

          // architecture-handbook-entry-end: ExportKeyForOfflineStorage

          // key should not be present anymore on p1
          participant1.keys.secret.list(namespace.unwrap) shouldBe empty

          // show that we can still add new parties using the new delegate key
          participant1.topology.party_to_participant_mappings.propose(
            PartyId(participantId.uid.tryChangeId("NewParty")),
            newParticipants = List(participantId -> ParticipantPermission.Submission),
            signedBy = Seq(targetKey.fingerprint),
          )

          // ensure party pops up on the synchronizers
          eventually() {
            participant1.parties.list(filterParty = "NewParty") should not be empty
          }

          val other = participant1 // using other for the manual ...
          // architecture-handbook-entry-begin: ImportFromOfflineStorage
          // import it back wherever needed
          other.keys.secret
            .upload_from(privateKeyFile.toString, Some("newly-imported-identity-key"))
          // architecture-handbook-entry-end: ImportFromOfflineStorage

          participant1.keys.secret.list(namespace.unwrap) should not be empty
        }
      }

      "export and import private key pairs offline to a different participant" in { implicit env =>
        import env.*

        val fingerprint = participant1.fingerprint

        better.files.File.usingTemporaryFile("namespace", ".key") { keyPairFile =>
          participant1.keys.secret.download_to(fingerprint, keyPairFile.toString)

          participant2.keys.secret.list(fingerprint.unwrap) shouldBe empty
          participant2.keys.secret
            .upload_from(keyPairFile.toString, Some("newly-imported-identity-key"))
          participant2.keys.secret.list(fingerprint.unwrap) should not be empty
        }
      }

      "export and import private key pairs with a password" in { implicit env =>
        import env.*

        // Generate a new key for export and export with a password for encryption
        val key = participant1.keys.secret
          .generate_signing_key("encrypted-export-test", SigningKeyUsage.ProtocolOnly)

        val keypairPlain = participant1.keys.secret.download(key.id)
        val keypairEnc = participant1.keys.secret.download(key.id, password = Some("hello world"))

        keypairPlain shouldNot equal(keypairEnc)

        participant2.keys.secret.upload(
          keypairEnc,
          name = Some("encrypted-import-test"),
          password = Some("hello world"),
        )

        participant2.keys.secret.list(key.id.unwrap) should not be empty
      }

      "export and fail import of private key pairs with invalid passwords" in { implicit env =>
        import env.*

        // Generate a new key for export and export with a password for encryption
        val key = participant1.keys.secret
          .generate_signing_key("encrypted-export-test-2", SigningKeyUsage.ProtocolOnly)
        val keypair = participant1.keys.secret.download(key.id, password = Some("hello world"))

        // Invalid password
        assertThrowsAndLogsCommandFailures(
          participant2.keys.secret.upload(
            keypair,
            name = Some("encrypted-import-test-2"),
            password = Some("wrong password"),
          ),
          _.errorMessage should include("Failed to decrypt"),
        )

        // no password
        assertThrowsAndLogsCommandFailures(
          participant2.keys.secret.upload(
            keypair,
            name = Some("encrypted-import-test-2"),
          ),
          _.errorMessage should include("Failed to parse crypto key pair"),
        )
      }
    }
  }
}

/** Test that participants are able to automatically reconnect to a synchronizer after it has
  * restarted with a new ID/Alias. To simulate this behavior we stop the original synchronizer and
  * start a new one that uses the same endpoints as the first synchronizer.
  */
sealed trait ReconnectSynchronizerAutoInitIntegrationTest {
  self: CommunityIntegrationTest =>

  protected val sequencerGroups: MultiSynchronizer =
    MultiSynchronizer(Seq(Set("sequencer1"), Set("sequencer2")).map(_.map(InstanceName.tryCreate)))

  "be able to connect to a reset synchronizer with a new alias" in { implicit env =>
    import env.*
    eventually() {
      val healthStatus = health.status()
      val (sequencerStatus, participantStatus) =
        (healthStatus.sequencerStatus, healthStatus.participantStatus)
      sequencerStatus(sequencer1.name).connectedParticipants should contain.allOf(
        participantStatus(participant1.name).id,
        participantStatus(participant2.name).id,
      )
      participantStatus(participant1.name).connectedSynchronizers should contain(
        sequencerStatus(sequencer1.name).synchronizerId -> SubmissionReady(true)
      )
      participantStatus(participant2.name).connectedSynchronizers should contain(
        sequencerStatus(sequencer1.name).synchronizerId -> SubmissionReady(true)
      )
    }

    val anotherSynchronizerAlias = SynchronizerAlias.tryCreate("anotherSynchronizer")
    loggerFactory.suppressWarningsAndErrors {
      logger.info(s"stopping synchronizer $daName")
      mediator1.stop()
      sequencer1.stop()

      // we start a new synchronizer with a different alias
      logger.info(s"starting synchronizer ${anotherSynchronizerAlias.unwrap}")
      sequencer2.start()
      mediator2.start()

      val staticSynchronizerParameters = StaticSynchronizerParameters.defaults(
        sequencer1.config.crypto,
        testedProtocolVersion,
      )

      val anotherSynchronizerId = bootstrap.synchronizer(
        anotherSynchronizerAlias.unwrap,
        sequencers = Seq(sequencer2),
        mediators = Seq(mediator2),
        synchronizerOwners = Seq(sequencer2),
        synchronizerThreshold = PositiveInt.one,
        staticSynchronizerParameters,
      )

      env.initializedSynchronizers.put(
        anotherSynchronizerAlias,
        InitializedSynchronizer(
          anotherSynchronizerId,
          staticSynchronizerParameters.toInternal,
          synchronizerOwners = Set(sequencer2),
        ),
      )

      // after synchronizer reset, participants get automatically disconnected
      eventually() {
        val healthStatus = health.status()
        val (sequencerStatus, participantStatus) =
          (healthStatus.sequencerStatus, healthStatus.participantStatus)
        sequencerStatus(sequencer2.name).connectedParticipants shouldBe empty
        participantStatus(participant1.name).connectedSynchronizers shouldBe empty
        participantStatus(participant2.name).connectedSynchronizers shouldBe empty
      }
    }
    // connecting to the reset synchronizer with the same alias is not possible, since the synchronizer now has a new identity
    loggerFactory.suppressWarningsErrorsExceptions[CommandFailure](
      participant1.synchronizers.reconnect(daName)
    )
    loggerFactory.suppressWarningsErrorsExceptions[CommandFailure](
      participant2.synchronizers.reconnect(daName)
    )

    // connect with a new alias and it all should work
    participant1.synchronizers.connect_local(
      sequencer2,
      anotherSynchronizerAlias,
      manualConnect = false,
    )
    participant2.synchronizers.connect_local(
      sequencer2,
      anotherSynchronizerAlias,
      manualConnect = false,
    )

    eventually() {
      val healthStatus = health.status()
      val (sequencerStatus, participantStatus) =
        (healthStatus.sequencerStatus, healthStatus.participantStatus)
      sequencerStatus(sequencer2.name).connectedParticipants should contain.allOf(
        participantStatus(participant1.name).id,
        participantStatus(participant2.name).id,
      )
      participantStatus(participant1.name).connectedSynchronizers should contain(
        sequencerStatus(sequencer2.name).synchronizerId -> SubmissionReady(true)
      )
      participantStatus(participant2.name).connectedSynchronizers should contain(
        sequencerStatus(sequencer2.name).synchronizerId -> SubmissionReady(true)
      )

      // we need to wait until participant1 observed both participants, as otherwise below ping will fail
      participant1.parties
        .list(
          filterParticipant = participantStatus(participant2.name).id.filterString,
          synchronizerIds = sequencerStatus(sequencer2.name).synchronizerId.logical,
        ) should have length 1
    }

    logger.info(s"participant1 pings participant2")
    assertPingSucceeds(participant1, participant2)
  }

}
