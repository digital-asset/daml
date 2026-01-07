// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.crashrecovery

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Reliability.{
  AdverseScenario,
  Component,
  ReliabilityTest,
  Remediation,
}
import com.digitalasset.canton.config.EncryptedPrivateStoreConfig
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.crypto.SigningKeyUsage.matchesRelevantUsages
import com.digitalasset.canton.crypto.admin.grpc.PrivateKeyMetadata
import com.digitalasset.canton.crypto.kms.KmsKeyId
import com.digitalasset.canton.crypto.{SigningKeyUsage, SigningPublicKey}
import com.digitalasset.canton.integration.plugins.{UseAwsKms, UseGcpKms, UsePostgres}
import com.digitalasset.canton.integration.{
  ConfigTransform,
  ConfigTransforms,
  EnvironmentSetupPlugin,
}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import monocle.macros.syntax.lens.*

/** Test that in case the active replica is crashed a passive one can take over and keep using an
  * encrypted crypto private store seamlessly.
  */
trait ParticipantFailoverWithEncryptedPrivateKeyStoreIntegrationTest
    extends ParticipantFailoverIntegrationTestBase {

  protected def kmsWrapperKey: KmsKeyId
  protected def kmsRotationWrapperKey: KmsKeyId

  private def checkEncryptionStatus(keys: Seq[PrivateKeyMetadata], encrypted: Boolean) =
    forAll(keys) { pk =>
      pk.encrypted shouldBe encrypted
    }

  private def updateConfigForExternalNode(
      configTransform: ConfigTransform,
      name: String,
  ): Unit = {
    val newConfig = configTransform(
      externalPlugin.configs
        .get(name)
        .valueOrFail(s"could not retrieve $name config")
    )
    externalPlugin.configureAndReplace(newConfig, newConfig.parameters)(
      name,
      newConfig.participantsByString(name),
    )
  }

  "A replicated participant with an encrypted private key store" must {
    val reliabilityTest =
      ReliabilityTest(
        Component(
          name = "application",
          setting =
            "application connected to replicated participant with an encrypted private key store",
        ),
        AdverseScenario(
          dependency = "participant node",
          details = "forceful stop (and then restart) of the active participant's process",
        ),
        Remediation(
          remediator = "application",
          action =
            "fail-over to a new active participant replica while stopped participant restarts",
        ),
        outcome = "transaction processing continuously possible given proper retries " +
          "and private key store state remains correct",
      )
    "fail-over when the active replica crashes".taggedAs(
      reliabilityTest
    ) in { implicit env =>
      import env.*

      val p1 = rp("participant1")
      val p2 = rp("participant2")

      val (activeParticipant, passiveParticipant) = if (isActive(p1)) (p1, p2) else (p2, p1)

      eventually(timeout) {
        assert(isHealthy(activeParticipant.name))
        assert(!isHealthy(passiveParticipant.name))
      }

      // check that keys in both participants can be read and are encrypted
      checkEncryptionStatus(activeParticipant.keys.secret.list(), encrypted = true)
      checkEncryptionStatus(passiveParticipant.keys.secret.list(), encrypted = true)

      // Kill the active participant instance, then the passive one must become healthy
      externalPlugin.kill(activeParticipant.name)
      eventually(timeout) {
        assert(isHealthy(passiveParticipant.name))
      }

      activePing(passiveParticipant, participant3)

      // private keys remain encrypted
      checkEncryptionStatus(passiveParticipant.keys.secret.list(), encrypted = true)
    }

    "revert its encrypted store and have it propagated to the replicas" in { implicit env =>
      import env.*

      val p1 = rp("participant1")
      val p2 = rp("participant2")

      val (activeParticipant, passiveParticipant) = if (isActive(p1)) (p1, p2) else (p2, p1)
      val (activeName, passiveName) = (activeParticipant.name, passiveParticipant.name)

      externalPlugin.kill(activeName)
      externalPlugin.kill(passiveName)

      // update config with the revert flag of the external nodes that have KMS configured
      val updateParticipantConfigs = ConfigTransforms.updateAllParticipantConfigs {
        case (name, config) =>
          if (name == activeName || name == passiveName)
            config
              .focus(_.crypto.privateKeyStore.encryption)
              .replace(
                Some(
                  EncryptedPrivateStoreConfig
                    .Kms(
                      wrapperKeyId = Some(kmsWrapperKey),
                      reverted = true,
                    )
                )
              )
          else config
      }
      updateConfigForExternalNode(updateParticipantConfigs, activeName)
      updateConfigForExternalNode(updateParticipantConfigs, passiveName)

      externalPlugin.start(activeName)
      externalPlugin.start(passiveName)

      val (newActiveParticipant, newPassiveParticipant) = if (isActive(p1)) (p1, p2) else (p2, p1)

      eventually(timeout) {
        assert(isHealthy(newActiveParticipant.name))
        assert(!isHealthy(newPassiveParticipant.name))
      }

      // synchronise on passive participant to be sure that we can poke it, otherwise below call will fail
      eventually() {
        waitPassive(newPassiveParticipant)
      }

      // for both active and passive participants the store has been reverted
      checkEncryptionStatus(newActiveParticipant.keys.secret.list(), encrypted = false)
      checkEncryptionStatus(newPassiveParticipant.keys.secret.list(), encrypted = false)
    }

    "wrapper key rotation must work across fail-over" in { implicit env =>
      import env.*

      def rotateNodeKey(participant: ParticipantReference): Unit = {
        // rotate a canton key to force loading a private key from the store using the new wrapper key
        val newSigKey =
          participant.keys.secret
            .generate_signing_key(usage = SigningKeyUsage.ProtocolOnly)
        // TODO(#23814): Add filter usage to owner_to_key_mappings
        val currentSigKey = participant.topology.owner_to_key_mappings
          .list(
            store = TopologyStoreId.Authorized,
            filterKeyOwnerUid = participant.id.filterString,
          )
          .find(x => x.item.member == participant.id)
          .flatMap(_.item.keys.find {
            case SigningPublicKey(_, _, _, usage, _) =>
              matchesRelevantUsages(usage, SigningKeyUsage.ProtocolOnly)
            case _ => false
          })
          .getOrElse(sys.error(s"No signing key found for owner $participant"))

        participant.topology.owner_to_key_mappings.rotate_key(
          participant.id,
          currentSigKey,
          newSigKey,
        )
      }

      val p1 = rp("participant1")
      val p2 = rp("participant2")

      val (activeParticipant, passiveParticipant) = if (isActive(p1)) (p1, p2) else (p2, p1)

      eventually(timeout) {
        assert(isHealthy(activeParticipant.name))
        assert(!isHealthy(passiveParticipant.name))
      }

      val oldWrapperKeyId = activeParticipant.keys.secret.get_wrapper_key_id()
      passiveParticipant.keys.secret.get_wrapper_key_id() should be(oldWrapperKeyId)

      activeParticipant.keys.secret.rotate_wrapper_key(kmsRotationWrapperKey.unwrap)
      val newWrapperKeyId = activeParticipant.keys.secret.get_wrapper_key_id()
      oldWrapperKeyId should not be equal(newWrapperKeyId)

      // create a new signing key with the new wrapper key
      rotateNodeKey(activeParticipant)

      // Kill the active participant instance, then the passive one must become healthy
      externalPlugin.kill(activeParticipant.name)
      eventually(timeout) {
        assert(isHealthy(passiveParticipant.name))
      }
      // wait for the passive participant to also become active,
      // so that it properly updates its internal state to the new wrapper key
      waitActive(passiveParticipant)

      // create a new signing key with the new wrapper key
      rotateNodeKey(passiveParticipant)

      passiveParticipant.keys.secret.list().foreach { key =>
        key.wrapperKeyId should be(Some(newWrapperKeyId))
      }

      passiveParticipant.keys.secret.get_wrapper_key_id() should be(newWrapperKeyId)

      passiveParticipant.health.ping(passiveParticipant)
    }
  }
}

class ParticipantFailoverWithAwsEncryptedPrivateKeyStoreIntegrationTestPostgres
    extends ParticipantFailoverWithEncryptedPrivateKeyStoreIntegrationTest {

  protected val kmsWrapperKey: KmsKeyId = UseAwsKms.DefaultCantonTestKeyId
  protected val kmsRotationWrapperKey: KmsKeyId = UseAwsKms.DefaultCantonRotationTestKeyId

  override protected lazy val extraPluginsToEnable: Seq[EnvironmentSetupPlugin] = Seq(
    new UseAwsKms(
      nodes = Set("participant1", "participant2"),
      timeouts = timeouts,
      loggerFactory = loggerFactory,
    )
  )

  setupPlugins(new UsePostgres(loggerFactory))
}

class ParticipantFailoverWithGcpEncryptedPrivateKeyStoreIntegrationTestPostgres
    extends ParticipantFailoverWithEncryptedPrivateKeyStoreIntegrationTest {

  protected val kmsWrapperKey: KmsKeyId = UseGcpKms.DefaultCantonTestKeyId
  protected val kmsRotationWrapperKey: KmsKeyId = UseGcpKms.DefaultCantonRotationTestKeyId

  override protected lazy val extraPluginsToEnable: Seq[EnvironmentSetupPlugin] = Seq(
    new UseGcpKms(
      nodes = Set("participant1", "participant2"),
      timeouts = timeouts,
      loggerFactory = loggerFactory,
    )
  )

  setupPlugins(new UsePostgres(loggerFactory))
}
