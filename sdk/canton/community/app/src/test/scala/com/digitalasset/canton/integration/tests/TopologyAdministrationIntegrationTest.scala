// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String185
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{CommandFailure, LocalInstanceReference}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.SigningKeyUsage.Protocol
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.TopologyManager.assignExpectedUsageToKeys
import com.digitalasset.canton.topology.TopologyManagerError
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId.{Authorized, Temporary}
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.topology.transaction.{
  NamespaceDelegation,
  OwnerToKeyMapping,
  SignedTopologyTransaction,
  TopologyTransaction,
}
import monocle.macros.syntax.lens.*

import scala.concurrent.ExecutionContext

class TopologyAdministrationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {
  registerPlugin(new UsePostgres(loggerFactory))
  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1

  "TopologyAdministration" should {
    "identity_transactions" in { implicit env =>
      import env.*
      val identityTransactions = sequencer1.topology.transactions.identity_transactions()

      val identityTransactions2 = sequencer1.topology.transactions
        .list()
        .result
        .flatMap(tx =>
          tx.transaction
            .selectMapping[NamespaceDelegation]
            .orElse(tx.transaction.selectMapping[OwnerToKeyMapping])
        )
        .filter(_.mapping.namespace == sequencer1.namespace)

      identityTransactions should not be empty
      identityTransactions2 should contain theSameElementsAs identityTransactions
    }

    "manage temporary topology stores" in { implicit env =>
      import env.*

      val testTempStoreId =
        participant1.topology.stores.create_temporary_topology_store("test", testedProtocolVersion)
      clue("creating the same store again") {
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant1.topology.stores
            .create_temporary_topology_store("test", testedProtocolVersion),
          _.shouldBeCantonErrorCode(TopologyManagerError.TemporaryTopologyStoreAlreadyExists),
        )
      }

      clue("dropping an unknown store") {
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant1.topology.stores.drop_temporary_topology_store(
            Temporary.apply(String185.tryCreate("unknown"))
          ),
          _.shouldBeCantonErrorCode(TopologyManagerError.TopologyStoreUnknown),
        )
      }

      clue("dropping a known store") {
        participant1.topology.stores.drop_temporary_topology_store(testTempStoreId)
      }

      clue("dropping a previously known store twice") {
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant1.topology.stores.drop_temporary_topology_store(testTempStoreId),
          _.shouldBeCantonErrorCode(TopologyManagerError.TopologyStoreUnknown),
        )
      }
    }

    def invalidUsageMessage(keyToAdd: SigningPublicKey, errMsg: String) =
      errMsg should include(
        SigningError
          .InvalidKeyUsage(
            keyToAdd.id,
            keyToAdd.usage,
            SigningKeyUsage.ProofOfOwnershipOnly,
          )
          .show
      )

    def addKeyToOTKMForcefully(
        node: LocalInstanceReference,
        keyToAdd: SigningPublicKey,
    )(implicit ec: ExecutionContext) = {

      // it fails because namespace-only keys are not allowed in a new OwnerToKeyMapping
      loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
        node.topology.owner_to_key_mappings.add_key(keyToAdd.id, keyToAdd.purpose),
        { logEntries =>
          val err = logEntries.head
          err.shouldBeCantonErrorCode(TopologyManagerError.InternalError)
          invalidUsageMessage(keyToAdd, err.toString)
        },
      )

      val topologySnapshotBytes = node.topology.transactions.export_topology_snapshot()

      val topologySnapshot = StoredTopologyTransactions
        .fromTrustedByteString(topologySnapshotBytes)
        .valueOrFail("failed to deserialize topology snapshot")

      val storedOtkm = topologySnapshot.result
        .find(_.mapping.isInstanceOf[OwnerToKeyMapping])
        .valueOrFail("retrieve OwnerToKeyMapping request")

      val otkm = storedOtkm.mapping.asInstanceOf[OwnerToKeyMapping]

      // we forcibly add an unintended key to the OwnerToKeyMapping
      val otkmWithNewKey = otkm.copy(keys = otkm.keys :+ keyToAdd)

      val topologyTransaction = TopologyTransaction(
        storedOtkm.transaction.transaction.operation,
        storedOtkm.transaction.transaction.serial.increment,
        otkmWithNewKey,
        testedProtocolVersion,
      )

      // we recreate the signed transaction, but assign the expected usages for the keys as if we
      // were verifying (i.e. forSigning = false). This lifts the restriction on signing, allowing a
      // namespace-only key to sign this request and be added to the OwnerToKeyMapping.
      val keysWithUsage = assignExpectedUsageToKeys(
        otkmWithNewKey,
        NonEmpty.mk(Set, keyToAdd.id) ++ storedOtkm.transaction.signatures.map(_.signedBy),
        forSigning = false,
      )

      val signedTopologyTransaction = SignedTopologyTransaction
        .signAndCreateWithAssignedKeyUsages(
          topologyTransaction,
          keysWithUsage,
          storedOtkm.transaction.isProposal,
          node.crypto.privateCrypto,
          testedProtocolVersion,
        )
        .futureValueUS
        .valueOrFail("failed to re-sign transaction")

      // we replace the previous OwnerToKeyMapping transaction in the topology snapshot with the new one
      val topologySnapshotUpdated = topologySnapshot.copy(result =
        topologySnapshot.result.updated(
          topologySnapshot.result.indexWhere(_.mapping == otkm),
          storedOtkm.focus(_.transaction).replace(signedTopologyTransaction),
        )
      )

      topologySnapshotUpdated.result
        .find(_.mapping.isInstanceOf[OwnerToKeyMapping])
        .valueOrFail("retrieve OwnerToKeyMapping request")
        .transaction
        .transaction
        .mapping
        .asInstanceOf[OwnerToKeyMapping]
        .keys
        .forgetNE should contain(keyToAdd)

      val topologySnapshotUpdatedBytes =
        topologySnapshotUpdated.toByteString(testedProtocolVersion)

      // we can import the previous topology snapshot with a namespace-only key in the OwnerToKeyMapping
      node.topology.transactions.import_topology_snapshot(
        topologySnapshotUpdatedBytes,
        TopologyStoreId.Authorized,
      )

      node.topology.owner_to_key_mappings
        .list(filterSigningKey = keyToAdd.id.toProtoPrimitive)
        .head
        .item
        .keys
        .forgetNE should contain(keyToAdd)

    }

    // This test ensures backwards compatibility with old OTK mapping requests that contain a node's namespace key.
    // We can import such a transaction because we accept keys with a `Namespace` or `ProofOfOwnership` usage during the
    // import process (i.e., during OTK signature verification). However, because the actual namespace key is present
    // in the OTK mapping, we cannot add a new key as long as the namespace key remains inside. Adding a new key
    // requires a signature from the node's namespace key, but since this key is also listed inside the OTK mapping,
    // we expect this signature to be produced by a key with a `ProofOfOwnership` usage.
    "import OwnerToKeyMappings with the real namespace key in it" in { implicit env =>
      import env.*

      val namespaceSigningKey =
        participant1.keys.public
          .list(
            filterPurpose = Set(KeyPurpose.Signing),
            filterUsage = SigningKeyUsage.NamespaceOnly,
          )
          .loneElement
          .publicKey
          .asSigningKey
          .valueOrFail("retrieve namespace key")

      addKeyToOTKMForcefully(participant1, namespaceSigningKey)

      // adding a new key fails because the node's namespace key is listed inside the OwnerToKeyMapping,
      // and therefore we expect the corresponding signature to be produced by a key with a `ProofOfOwnership` usage
      val anotherKey = participant1.crypto
        .generateSigningKey(
          usage = SigningKeyUsage.ProtocolOnly,
          name = Some(KeyName.tryCreate("another-key")),
        )
        .futureValueUS
        .valueOrFail("generate key")

      loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
        participant1.topology.owner_to_key_mappings.add_key(
          anotherKey.id,
          KeyPurpose.Signing,
        ),
        { logEntries =>
          val err = logEntries.head
          err.shouldBeCantonErrorCode(TopologyManagerError.InternalError)
          invalidUsageMessage(namespaceSigningKey, err.toString)
        },
      )

      // to be able to add a new key, we must remove the namespace key from the OwnerToKeyMapping
      participant1.topology.owner_to_key_mappings
        .remove_key(namespaceSigningKey.id, KeyPurpose.Signing)
      participant1.topology.owner_to_key_mappings
        .list()
        .flatMap(_.item.keys.forgetNE.map(_.id)) should not contain namespaceSigningKey.id

      participant1.topology.owner_to_key_mappings.add_key(
        anotherKey.id,
        KeyPurpose.Signing,
      )

    }

    // This test ensures backwards compatibility with old OTK mapping requests that contain
    // keys with an unexpected usage (i.e., `Namespace`). In this case, the OTK mapping does not include the actual
    // namespace key, so we can add a new key without removing the unintended entry.
    // However, as a best practice, the unintended key should be removed before adding a new one.
    "import OwnerToKeyMappings with an arbitrary namespace key in it" in { implicit env =>
      import env.*

      Set[LocalInstanceReference](participant1, sequencer1).foreach { node =>
        // generate an NEW arbitrary key with a `Namespace` usage
        val keyWithNamespaceUsage = node.crypto
          .generateSigningKey(
            usage = SigningKeyUsage.NamespaceOnly,
            name = Some(KeyName.tryCreate("test-key")),
          )
          .futureValueUS
          .valueOrFail("generate key")

        addKeyToOTKMForcefully(node, keyWithNamespaceUsage)

        // add another key to verify that it works when there is an arbitrary namespace key in the OwnerToKeyMapping
        val anotherKey = node.crypto
          .generateSigningKey(
            usage = SigningKeyUsage.ProtocolOnly,
            name = Some(KeyName.tryCreate("another-key")),
          )
          .futureValueUS
          .valueOrFail("generate key")

        // this time it works because the signing operation for producing the request's signature with the namespace key
        // expects a `Namespace` usage, as the key is not listed inside the OwnerToKeyMapping.
        node.topology.owner_to_key_mappings.add_key(
          anotherKey.id,
          KeyPurpose.Signing,
        )

        // it is still recommended to remove the incorrect key from the OwnerToKeyMapping
        node.topology.owner_to_key_mappings.remove_key(keyWithNamespaceUsage.id, KeyPurpose.Signing)
        node.topology.owner_to_key_mappings
          .list(filterSigningKey = keyWithNamespaceUsage.id.toProtoPrimitive) shouldBe empty
      }
    }

    "properly handle OwnerToKeyMappings with missing signing key signatures" when {
      // using participant2 in this test to not interfere with a previous test case that already uses participant1
      "submitting to the authorized store" in { implicit env =>
        import env.*

        val existingOtk = participant2.topology.owner_to_key_mappings
          .list(Authorized, filterKeyOwnerUid = participant2.id.filterString)
          .loneElement

        val signingKey = participant2.keys.secret
          .generate_signing_key("authorized-store-test", usage = Set(Protocol))

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant2.topology.owner_to_key_mappings.propose(
            existingOtk.item.copy(keys = existingOtk.item.keys :+ signingKey),
            serial = Some(existingOtk.context.serial.increment),
            // explicitly only sign with the namespace key, but not the signing key
            signedBy = Seq(participant2.fingerprint),
            store = Authorized,
          ),
          _.shouldBeCantonError(
            TopologyManagerError.UnauthorizedTransaction,
            _ should include("Not authorized"),
          ),
          _.errorMessage should include("Please contact the operator"),
        )
      }

      "submitting to a temporary store" in { implicit env =>
        import env.*

        val testTempStoreId =
          participant2.topology.stores
            .create_temporary_topology_store("test", testedProtocolVersion)

        // load the root cert into the temporary store
        val rootCert = participant2.topology.transactions
          .identity_transactions()
          .flatMap(_.selectMapping[NamespaceDelegation])
          .filter(NamespaceDelegation.isRootCertificate(_))
          .loneElement
        participant2.topology.transactions.load(Seq(rootCert), testTempStoreId)

        val signingKey = participant2.keys.secret
          .generate_signing_key("authorized-store-test", usage = Set(Protocol))

        val existingOtk = participant2.topology.owner_to_key_mappings
          .list(Authorized, filterKeyOwnerUid = participant2.id.filterString)
          .loneElement
          .item

        val otkWithNewKey = existingOtk.copy(keys = existingOtk.keys :+ signingKey)
        participant2.topology.owner_to_key_mappings.propose(
          otkWithNewKey,
          serial = Some(PositiveInt.one),
          // explicitly only sign with the namespace key, but not the signing key
          signedBy = Seq(participant2.fingerprint),
          store = testTempStoreId,
        )

        // verify that the OTK in the temporary store was accepted and stored how we expect it
        val otkInTempStore =
          participant2.topology.owner_to_key_mappings.list(testTempStoreId).loneElement
        otkInTempStore.item shouldBe otkWithNewKey
        otkInTempStore.context.signedBy.forgetNE.loneElement shouldBe participant2.fingerprint

        // cleanup
        participant2.topology.stores.drop_temporary_topology_store(testTempStoreId)
      }

      "submitting to a synchronizer store" in { implicit env =>
        import env.*

        participant2.synchronizers.connect_local(sequencer1, daName)

        val existingOtk = participant2.topology.owner_to_key_mappings
          .list(daId, filterKeyOwnerUid = participant2.id.filterString)
          .loneElement

        val signingKey = participant2.keys.secret
          .generate_signing_key("authorized-store-test", usage = Set(Protocol))

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant2.topology.owner_to_key_mappings.propose(
            existingOtk.item.copy(keys = existingOtk.item.keys :+ signingKey),
            serial = Some(existingOtk.context.serial.increment),
            // explicitly only sign with the namespace key, but not the signing key
            signedBy = Seq(participant2.fingerprint),
            store = daId,
          ),
          _.shouldBeCantonError(
            TopologyManagerError.UnauthorizedTransaction,
            _ should include("Not authorized"),
          ),
          _.errorMessage should include("Please contact the operator"),
        )
      }

    }

  }
}
