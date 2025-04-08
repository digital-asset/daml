// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{InstanceReference, LocalInstanceReference}
import com.digitalasset.canton.crypto.SigningKeyUsage
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.OwnerToKeyMapping

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
  ): Unit = {

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

      // user-manual-entry-begin: ManualRegisterKmsKeys
      val namespaceKey = node.keys.secret
        .register_kms_signing_key(
          namespaceKmsKeyId,
          SigningKeyUsage.NamespaceOnly,
          name = s"${node.name}-${SigningKeyUsage.Namespace.identifier}",
        )
      val sequencerAuthKey = node.keys.secret
        .register_kms_signing_key(
          sequencerAuthKmsKeyId,
          SigningKeyUsage.SequencerAuthenticationOnly,
          name = s"${node.name}-${SigningKeyUsage.SequencerAuthentication.identifier}",
        )
      val signingKey = node.keys.secret
        .register_kms_signing_key(
          signingKmsKeyId,
          SigningKeyUsage.ProtocolOnly,
          name = s"${node.name}-${SigningKeyUsage.Protocol.identifier}",
        )
      val encryptionKey = node.keys.secret
        .register_kms_encryption_key(encryptionKmsKeyId, name = node.name + "-encryption")
      // user-manual-entry-end: ManualRegisterKmsKeys

      (namespaceKey, sequencerAuthKey, signingKey, encryptionKey)
    } else {
      // architecture-handbook-entry-begin: ManualInitKeys

      // first, let's create a signing key that is going to control our identity.
      val namespaceKey =
        node.keys.secret
          .generate_signing_key(
            name = node.name + s"-${SigningKeyUsage.Namespace.identifier}",
            SigningKeyUsage.NamespaceOnly,
          )

      // create signing and encryption keys
      val sequencerAuthKey =
        node.keys.secret.generate_signing_key(
          name = node.name + s"-${SigningKeyUsage.SequencerAuthentication.identifier}",
          SigningKeyUsage.SequencerAuthenticationOnly,
        )
      val signingKey =
        node.keys.secret
          .generate_signing_key(
            name = node.name + s"-${SigningKeyUsage.Protocol.identifier}",
            SigningKeyUsage.ProtocolOnly,
          )
      val encryptionKey =
        node.keys.secret.generate_encryption_key(name = node.name + "-encryption")

      // architecture-handbook-entry-end: ManualInitKeys

      (namespaceKey, sequencerAuthKey, signingKey, encryptionKey)
    }

    // architecture-handbook-entry-begin: ManualInitNode

    // use the fingerprint of this key for our identity
    // in production, use a "random" identifier. for testing and development, use something
    // helpful so you don't have to grep for hashes in your log files.
    val namespace = Namespace(namespaceKey.id)
    node.topology.init_id(
      UniqueIdentifier.tryCreate("manual-" + node.name, namespace)
    )
    node.health.wait_for_ready_for_node_topology()
    // create the root certificate (self-signed)
    node.topology.namespace_delegations.propose_delegation(
      namespace,
      namespaceKey,
      isRootDelegation = true,
    )

    // assign new keys to this node. only participants need an encryption key,
    // but for simplicity every node gets one
    node.topology.owner_to_key_mappings.propose(
      OwnerToKeyMapping(
        node.id.member,
        NonEmpty(Seq, sequencerAuthKey, signingKey, encryptionKey),
      ),
      serial = PositiveInt.one,
      signedBy = Seq(namespaceKey.fingerprint, sequencerAuthKey.fingerprint, signingKey.fingerprint),
    )
    // architecture-handbook-entry-end: Node

  }

}
