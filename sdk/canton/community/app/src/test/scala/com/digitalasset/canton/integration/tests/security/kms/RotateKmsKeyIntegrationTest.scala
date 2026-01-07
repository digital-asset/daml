// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms

import com.digitalasset.canton.console.{LocalInstanceReference, LocalParticipantReference}
import com.digitalasset.canton.crypto.admin.grpc.PrivateKeyMetadata
import com.digitalasset.canton.crypto.{KeyPurpose, SigningKeyUsage}
import com.digitalasset.canton.integration.EnvironmentDefinition.allNodeNames
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.security.kms.aws.AwsKmsCryptoIntegrationTestBase
import com.digitalasset.canton.integration.tests.security.kms.gcp.GcpKmsCryptoIntegrationTestBase
import com.digitalasset.canton.integration.{CommunityIntegrationTest, SharedEnvironment}

trait RotateKmsKeyIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with KmsCryptoIntegrationTestBase {

  protected val newKmsKeyIdsMap: Map[String, String]

  "be able to rotate private keys with other existing pre-generated KMS keys" in { implicit env =>
    import env.*

    def getSigningKeyForNode(node: LocalInstanceReference): PrivateKeyMetadata =
      node.keys.secret
        .list(filterPurpose = Set(KeyPurpose.Signing), filterUsage = SigningKeyUsage.ProtocolOnly)
        .find(_.publicKey.fingerprint != node.id.uid.namespace.fingerprint)
        .valueOrFail("Could not find signing key")

    val signingKeyParticipant1 = getSigningKeyForNode(participant1).publicKey
    val keyFingerprint = signingKeyParticipant1.fingerprint.unwrap
    val newKmsKeyId = newKmsKeyIdsMap(participant1.name)

    // user-manual-entry-begin: RotateKmsNodeKey
    val newSigningKeyParticipant = participant1.keys.secret
      .rotate_kms_node_key(
        keyFingerprint,
        newKmsKeyId,
        "kms_key_rotated",
      )
    // user-manual-entry-end: RotateKmsNodeKey

    protectedNodes.foreach { nodeName =>
      val node = env.n(nodeName)
      val signingKey =
        if (nodeName == "participant1") signingKeyParticipant1
        else getSigningKeyForNode(node).publicKey

      val newSigningKey = node match {
        case participant: LocalParticipantReference if participant.name.contains("participant1") =>
          newSigningKeyParticipant
        case node =>
          node.keys.secret.rotate_kms_node_key(
            signingKey.fingerprint.unwrap,
            newKmsKeyIdsMap(node.name),
            "kms_key_rotated",
          )
      }

      node.topology.synchronisation.await_idle()

      val storedKey = node.keys.secret
        .list(filterFingerprint = newSigningKey.fingerprint.unwrap)
        .lastOption
        .valueOrFail("Could not find key")

      signingKey should not be newSigningKey
      newSigningKey shouldBe storedKey.publicKey
      storedKey.name shouldBe Some("kms_key_rotated")
    }

    assertPingSucceeds(participant1, participant2)
  }

  setupPlugins(
    withAutoInit = false,
    storagePlugin = Some(new UsePostgres(loggerFactory)),
    sequencerPlugin = new UseBftSequencer(loggerFactory),
  )
}

class RotateAwsKmsKeyReferenceIntegrationTest
    extends RotateKmsKeyIntegrationTest
    with AwsKmsCryptoIntegrationTestBase {

  // It does not matter which keys we rotate to, as long as they are distinct and not previously used.
  // We have chosen these particular keys for rotation to avoid generating new keys in the KMS,
  // which would incur extra costs.
  override protected val newKmsKeyIdsMap: Map[String, String] =
    Map(
      "participant1" -> "alias/canton-kms-test-signing-key",
      "participant2" -> "alias/canton-kms-test-signing-key-da",
      "mediator1" -> "alias/canton-kms-test-signing-key-domainManager",
      "sequencer1" -> "alias/canton-kms-test-authentication-key-domainManager",
    )

  override protected lazy val protectedNodes: Set[String] = allNodeNames(
    environmentDefinition.baseConfig
  )
}

class RotateGcpKmsKeyReferenceIntegrationTest
    extends RotateKmsKeyIntegrationTest
    with GcpKmsCryptoIntegrationTestBase {

  // It does not matter which keys we rotate to, as long as they are distinct and not previously used.
  // We have chosen these particular keys for rotation to avoid generating new keys in the KMS,
  // which would incur extra costs.
  override protected val newKmsKeyIdsMap: Map[String, String] =
    Map(
      "participant1" -> "canton-kms-test-signing-key",
      "participant2" -> "canton-kms-test-signing-key-da",
      "mediator1" -> "canton-kms-test-signing-key-domainManager",
      "sequencer1" -> "canton-kms-test-authentication-key-domainManager",
    )

  override protected lazy val protectedNodes: Set[String] =
    allNodeNames(
      environmentDefinition.baseConfig
    )
}
