// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.console.{InstanceReference, LocalInstanceReference}
import com.digitalasset.canton.crypto.KeyPurpose.Signing
import com.digitalasset.canton.crypto.SigningKeyUsage
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllMappings

import scala.concurrent.ExecutionContext

trait TopologyManagementHelper { this: BaseTest =>

  protected case class TopologyKmsKeys(
      namespaceKeyId: Option[String],
      sequencerAuthKeyId: Option[String],
      signingKeyId: Option[String],
      encryptionKeyId: Option[String],
  ) {

    // The predefined KMS key IDs are constructed by concatenating the default key IDs with the node name.
    def forNode(node: InstanceReference): TopologyKmsKeys = {
      def appendNodeName(keyId: Option[String]): Option[String] =
        keyId.map(_ + "-" + node.name)

      this.copy(
        namespaceKeyId = appendNodeName(namespaceKeyId),
        sequencerAuthKeyId = appendNodeName(sequencerAuthKeyId),
        signingKeyId = appendNodeName(signingKeyId),
        encryptionKeyId = appendNodeName(encryptionKeyId),
      )
    }

  }

  def manuallyInitNode(
      node: LocalInstanceReference,
      kmsKeysO: Option[TopologyKmsKeys] = None,
  )(implicit ec: ExecutionContext): Unit = {

    val (namespaceKey, sequencerAuthKey, signingKey, encryptionKey) = if (kmsKeysO.isDefined) {
      val kmsKeys = kmsKeysO.valueOrFail("no kms key ids defined")

      val namespaceKmsKeyId = kmsKeys.namespaceKeyId
        .valueOrFail(s"node [${node.name}] expects a namespace key id")
      val sequencerAuthKmsKeyId = kmsKeys.sequencerAuthKeyId
        .valueOrFail(s"node [${node.name}] expects a sequencer authentication key id")
      val signingKmsKeyId = kmsKeys.signingKeyId
        .valueOrFail(s"node [${node.name}] expects a signing key id")
      val encryptionKmsKeyId = kmsKeys.encryptionKeyId
        .valueOrFail(s"node [${node.name}] expects an encryption key id")

      // To refer in docs
      val intermediateNsKmsKeyId = namespaceKmsKeyId
      // user-manual-entry-begin: ManualRegisterKmsIntermediateNamespaceKey
      val intermediateKey = node.keys.secret
        .register_kms_signing_key(
          intermediateNsKmsKeyId,
          SigningKeyUsage.NamespaceOnly,
          name = s"${node.name}-${SigningKeyUsage.Namespace.identifier}",
        )
      // user-manual-entry-begin: ManualRegisterKmsNamespaceKey
      node.crypto.cryptoPrivateStore
        .existsPrivateKey(intermediateKey.id, Signing)
        .valueOrFail("intermediate key not registered")
        .futureValueUS

      // user-manual-entry-begin: ManualRegisterKmsKeys

      // Register the KMS signing key used to define the node identity.
      val namespaceKey = node.keys.secret
        .register_kms_signing_key(
          namespaceKmsKeyId,
          SigningKeyUsage.NamespaceOnly,
          name = s"${node.name}-${SigningKeyUsage.Namespace.identifier}",
        )

      // Register the KMS signing key used to authenticate the node toward the Sequencer.
      val sequencerAuthKey = node.keys.secret
        .register_kms_signing_key(
          sequencerAuthKmsKeyId,
          SigningKeyUsage.SequencerAuthenticationOnly,
          name = s"${node.name}-${SigningKeyUsage.SequencerAuthentication.identifier}",
        )

      // Register the signing key used to sign protocol messages.
      val signingKey = node.keys.secret
        .register_kms_signing_key(
          signingKmsKeyId,
          SigningKeyUsage.ProtocolOnly,
          name = s"${node.name}-${SigningKeyUsage.Protocol.identifier}",
        )

      // Register the encryption key.
      val encryptionKey = node.keys.secret
        .register_kms_encryption_key(encryptionKmsKeyId, name = node.name + "-encryption")

      // user-manual-entry-end: ManualRegisterKmsKeys

      (namespaceKey, sequencerAuthKey, signingKey, encryptionKey)
    } else {
      // architecture-handbook-entry-begin: ManualInitKeys

      // Create a signing key used to define the node identity.
      val namespaceKey =
        node.keys.secret
          .generate_signing_key(
            name = node.name + s"-${SigningKeyUsage.Namespace.identifier}",
            SigningKeyUsage.NamespaceOnly,
          )

      // Create a signing key used to authenticate the node toward the Sequencer.
      val sequencerAuthKey =
        node.keys.secret.generate_signing_key(
          name = node.name + s"-${SigningKeyUsage.SequencerAuthentication.identifier}",
          SigningKeyUsage.SequencerAuthenticationOnly,
        )

      // Create a signing key used to sign protocol messages.
      val signingKey =
        node.keys.secret
          .generate_signing_key(
            name = node.name + s"-${SigningKeyUsage.Protocol.identifier}",
            SigningKeyUsage.ProtocolOnly,
          )

      // Create the encryption key.
      val encryptionKey =
        node.keys.secret.generate_encryption_key(name = node.name + "-encryption")

      // architecture-handbook-entry-end: ManualInitKeys

      (namespaceKey, sequencerAuthKey, signingKey, encryptionKey)
    }

    // architecture-handbook-entry-begin: ManualInitNode

    // Use the fingerprint of this key for the node identity.
    val namespace = Namespace(namespaceKey.id)
    node.topology.init_id_from_uid(
      UniqueIdentifier.tryCreate("manual-" + node.name, namespace)
    )

    // Wait until the node is ready to receive the node identity.
    node.health.wait_for_ready_for_node_topology()

    // Create the self-signed root certificate.
    node.topology.namespace_delegations.propose_delegation(
      namespace,
      namespaceKey,
      CanSignAllMappings,
    )

    // Assign the new keys to this node.
    node.topology.owner_to_key_mappings.propose(
      member = node.id.member,
      keys = NonEmpty(Seq, sequencerAuthKey, signingKey, encryptionKey),
      signedBy = Seq(namespaceKey.fingerprint, sequencerAuthKey.fingerprint, signingKey.fingerprint),
    )

    // architecture-handbook-entry-end: ManualInitNode

  }

}
