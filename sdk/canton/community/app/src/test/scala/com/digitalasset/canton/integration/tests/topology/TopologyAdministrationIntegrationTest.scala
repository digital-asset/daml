// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.data.topology.ListOwnerToKeyMappingResult
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.crypto.SigningKeyUsage.{IdentityDelegation, Namespace, Protocol}
import com.digitalasset.canton.crypto.{EncryptionPublicKey, SigningKeyUsage, SigningPublicKey}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.DelegationRestriction.{
  CanSignAllButNamespaceDelegations,
  CanSignAllMappings,
  CanSignSpecificMappings,
}
import com.digitalasset.canton.topology.{ForceFlag, ForceFlags, PartyId, TopologyManagerError}
import com.digitalasset.daml.lf.archive.DarParser

import java.io.File

trait TopologyAdministrationTest extends CommunityIntegrationTest with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1.withSetup { implicit env =>
      import env.*

      clue("participant1 connects to sequencer1") {
        participant1.synchronizers.connect_local(sequencer1, daName)
      }
    }

  "participant_synchronizer_permissions" in { implicit env =>
    import env.*
    participant1.topology.participant_synchronizer_permissions
      .find(daId, participant1.id) shouldBe empty

    val loginAfter = Some(CantonTimestamp.now())
    val limits = Some(ParticipantSynchronizerLimits(17))

    synchronizerOwners1.foreach { owner =>
      owner.topology.participant_synchronizer_permissions.propose(
        daId,
        participant1.id,
        ParticipantPermission.Observation,
        loginAfter = loginAfter,
        limits = limits,
      )
    }

    val expectedSynchronizerPermissions = ParticipantSynchronizerPermission(
      daId,
      participant1.id,
      ParticipantPermission.Observation,
      limits,
      loginAfter,
    )
    eventually() {
      val p1Permission = participant1.topology.participant_synchronizer_permissions
        .find(daId, participant1.id)
        .getOrElse(fail(s"Could not find ParticipantSynchronizerPermission for ${participant1.id}"))
      p1Permission.context.serial shouldBe PositiveInt.one
      p1Permission.item shouldBe expectedSynchronizerPermissions
    }
  }

  "synchronizer_trust_certificates" in { implicit env =>
    import env.*

    val trustCert1 = {
      val certs = participant1.topology.synchronizer_trust_certificates
        .list(store = daId, filterUid = participant1.id.filterString)
      certs should not be empty
      certs.head
    }

    val expectedTrustCert1 = SynchronizerTrustCertificate(
      participant1.id,
      daId,
    )

    trustCert1.context.serial shouldBe PositiveInt.one
    trustCert1.item shouldBe expectedTrustCert1
  }

  "identifier_delegations" in { implicit env =>
    import env.*

    val delegationKey = participant1.keys.secret
      .generate_signing_key("test_key", SigningKeyUsage.IdentityDelegationOnly)
    participant1.topology.identifier_delegations.propose(
      participant1.id.uid,
      targetKey = delegationKey,
    )

    // fails if the target key does not have the correct `IdentityDelegation` usage
    val keyWithWrongUsage = participant1.keys.secret
      .generate_signing_key("wrong_key", SigningKeyUsage.ProtocolOnly)
    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant1.topology.identifier_delegations.propose(
        participant1.id.uid,
        targetKey = keyWithWrongUsage,
      ),
      _.errorMessage should include(
        s"The key ${keyWithWrongUsage.id} must include a ${SigningKeyUsage.IdentityDelegation} usage."
      ),
    )

    eventually() {
      val delegation = participant1.topology.identifier_delegations
        .list(store = daId, filterUid = participant1.id.filterString)
        .headOption
        .value

      val expectedDelegation = IdentifierDelegation.tryCreate(
        participant1.uid,
        delegationKey,
      )

      delegation.context.serial shouldBe PositiveInt.one
      delegation.item shouldBe expectedDelegation
    }
  }

  "namespace_delegations" when {
    "propose_delegation" in { implicit env =>
      import env.*
      val rootNamespace = participant1.namespace

      // establish the baseline with the namespace's root certificate
      val rootNSD = participant1.topology.namespace_delegations
        .list(
          store = TopologyStoreId.Authorized,
          filterNamespace = rootNamespace.toProtoPrimitive,
        )
        .map(_.item)
        .loneElement

      rootNSD.namespace shouldBe rootNamespace
      rootNSD.canSign(NamespaceDelegation.code) shouldBe true
      rootNSD.target.id shouldBe rootNamespace.fingerprint

      // generate a new signing key and register a root namespace delegation
      val rootDelegationKey =
        participant1.keys.secret
          .generate_signing_key("root_delegation_key", SigningKeyUsage.NamespaceOnly)
      val delegationKey =
        participant1.keys.secret
          .generate_signing_key(
            "delegation_key",
            SigningKeyUsage.NamespaceOrIdentityDelegation,
          )

      // propose the namespace delegations
      participant1.topology.namespace_delegations.propose_delegation(
        participant1.namespace,
        rootDelegationKey,
        CanSignAllMappings,
      )

      participant1.topology.namespace_delegations.propose_delegation(
        rootNamespace,
        delegationKey,
        CanSignAllButNamespaceDelegations,
      )

      // fails if the target key does not have the correct `Namespace` usage
      val keyWithWrongUsage = participant1.keys.secret
        .generate_signing_key("wrong_key", SigningKeyUsage.ProtocolOnly)

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.topology.namespace_delegations
          .propose_delegation(
            participant1.namespace,
            keyWithWrongUsage,
            CanSignAllMappings,
          ),
        _.errorMessage should include(
          s"The key ${keyWithWrongUsage.id} must include " +
            s"a ${SigningKeyUsage.Namespace} usage."
        ),
      )

      val NSDs = participant1.topology.namespace_delegations
        .list(
          store = TopologyStoreId.Authorized,
          filterNamespace = rootNamespace.toProtoPrimitive,
        )
        .map(_.item)

      NSDs should contain theSameElementsAs Seq(
        rootNSD,
        NamespaceDelegation.tryCreate(rootNamespace, rootDelegationKey, CanSignAllMappings),
        NamespaceDelegation.tryCreate(
          rootNamespace,
          delegationKey,
          CanSignAllButNamespaceDelegations,
        ),
      )

      // now let's revoke them again in reverse order, because $rootDelegationKey might have been used
      // to sign $delegationKey, and we would get a warning when removing $rootDelegationKey first.
      participant1.topology.namespace_delegations.propose_revocation(
        rootNamespace,
        delegationKey,
      )
      participant1.topology.namespace_delegations.propose_revocation(
        rootNamespace,
        rootDelegationKey,
      )
      participant1.topology.namespace_delegations
        .list(
          store = TopologyStoreId.Authorized,
          filterNamespace = rootNamespace.toProtoPrimitive,
        )
        .map(_.item)
        .loneElement shouldBe rootNSD
    }

    "propose_delegation with a delegation restriction" in { implicit env =>
      import env.*
      // create a new key
      val restrictedKey =
        participant1.keys.secret
          .generate_signing_key(usage =
            SigningKeyUsage.ProtocolWithProofOfOwnership.incl(SigningKeyUsage.Namespace)
          )

      // restrict the key to only be used for PTK
      participant1.topology.namespace_delegations
        .propose_delegation(
          participant1.namespace,
          restrictedKey,
          CanSignSpecificMappings(PartyToKeyMapping),
        )

      // cannot sign a PTP
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.topology.party_to_participant_mappings.propose(
          PartyId.tryCreate("nsd-test", participant1.namespace),
          Seq((participant1.id, ParticipantPermission.Submission)),
          signedBy = Seq(restrictedKey.fingerprint),
        ),
        _.shouldBeCommandFailure(TopologyManagerError.NoAppropriateSigningKeyInStore),
      )

      participant1.topology.party_to_key_mappings.propose(
        PartyToKeyMapping.tryCreate(
          PartyId.tryCreate("nsd-test", participant1.namespace),
          PositiveInt.one,
          NonEmpty(Seq, restrictedKey),
        ),
        serial = PositiveInt.one,
        signedBy = Some(restrictedKey.fingerprint),
      )
    }

  }

  "owner_to_key_mappings" in { implicit env =>
    import env.*

    def readOkmHead(): ListOwnerToKeyMappingResult = participant1.topology.owner_to_key_mappings
      .list(store = daId, filterKeyOwnerUid = participant1.id.filterString)
      .head

    val okm1 = readOkmHead()
    val initialOkmSerial = okm1.context.serial // Don't assume serial 1 if other tests touch OKMs

    val addedKey = participant1.keys.secret.generate_encryption_key("added_key")
    participant1.topology.owner_to_key_mappings
      .add_key(addedKey.fingerprint, addedKey.purpose)

    eventually() {
      val okm2 = readOkmHead()
      okm2.context.serial shouldBe initialOkmSerial.increment
      okm2.item.keys.toSet -- okm1.item.keys.toSet shouldBe Set(addedKey) // observe added key
    }

    // Now remove original key
    val removedKey = okm1.item.keys.collectFirst { case encKey: EncryptionPublicKey =>
      encKey
    }.value
    participant1.topology.owner_to_key_mappings
      .remove_key(removedKey.fingerprint, removedKey.purpose)

    eventually() {
      val okm3 = readOkmHead()
      okm3.context.serial shouldBe initialOkmSerial.tryAdd(2)
      okm3.item.keys.collect { case encKey: EncryptionPublicKey => encKey } shouldBe Seq(addedKey)
    }

    // Indirect OKM testing by rotating a key via key vault
    val okmSigningKey = okm1.item.keys.collect { case signKey: SigningPublicKey => signKey }.head
    participant1.keys.secret.rotate_node_key(okmSigningKey.fingerprint.toProtoPrimitive)

    eventually() {
      val okmsRotated = readOkmHead()
      val signingKeys = okmsRotated.item.keys.collect { case signKey: SigningPublicKey => signKey }
      signingKeys.size shouldBe 2
      signingKeys should not contain okmSigningKey // signing key must have been rotated
    }

    def otkForP1(key: SigningPublicKey) =
      participant1.topology.owner_to_key_mappings.propose(
        OwnerToKeyMapping(
          participant1.id.member,
          NonEmpty(Seq, key),
        ),
        serial = initialOkmSerial.tryAdd(5),
        signedBy = Seq(key.id),
      )

    // Fails when creating an OTK signedBy by key that lacks `Namespace` or `IdentityDelegation` usage
    val keyWithWrongUsage =
      participant1.keys.secret.generate_signing_key("wrong_key", SigningKeyUsage.ProtocolOnly)
    loggerFactory.assertThrowsAndLogs[CommandFailure](
      otkForP1(keyWithWrongUsage),
      _.message should include("Topology transaction is not properly authorized"),
      _.message should include(
        "INVALID_ARGUMENT/An error occurred. Please contact the operator " +
          "and inquire about the request"
      ),
    )

    // If a key is a root or intermediate namespace key and includes `Namespace`, it can be used to authorize an OTK.
    val intermediateKey = participant1.keys.secret.generate_signing_key(
      "intermediateKey",
      NonEmpty.mk(Set, Namespace, Protocol): NonEmpty[Set[SigningKeyUsage]],
    )
    participant1.topology.namespace_delegations.propose_delegation(
      participant1.namespace,
      intermediateKey,
      CanSignAllButNamespaceDelegations,
    )
    otkForP1(intermediateKey)

  }

  "party_to_key_mappings" in { implicit env =>
    import env.*

    val bob = participant1.ledger_api.parties.allocate("Bob")

    def ptkForP1(key: SigningPublicKey) =
      participant1.topology.party_to_key_mappings.propose(
        PartyToKeyMapping
          .create(
            bob.party,
            PositiveInt.one,
            NonEmpty.mk(Seq, key),
          )
          .valueOrFail("create party to key mapping"),
        serial = PositiveInt.one,
        signedBy = Some(key.id),
      )

    // Fails when creating an PTK signedBy by key that lacks `Namespace` or `IdentityDelegation` usage
    val keyWithWrongUsage =
      participant1.keys.secret.generate_signing_key("wrong_key", SigningKeyUsage.ProtocolOnly)
    loggerFactory.assertThrowsAndLogs[CommandFailure](
      ptkForP1(keyWithWrongUsage),
      _.message should include("Topology transaction is not properly authorized"),
      _.message should include(
        "INVALID_ARGUMENT/An error occurred. Please contact the operator " +
          "and inquire about the request"
      ),
    )

    // If a key is a root or intermediate namespace key and includes `Namespace`, it can be used to authorize an OTK.
    val delegatedKey = participant1.keys.secret.generate_signing_key(
      "intermediateKey",
      NonEmpty.mk(Set, Namespace, IdentityDelegation, Protocol): NonEmpty[Set[SigningKeyUsage]],
    )
    participant1.topology.namespace_delegations.propose_delegation(
      participant1.namespace,
      delegatedKey,
      CanSignAllButNamespaceDelegations,
    )
    participant1.topology.identifier_delegations.propose(
      participant1.id.uid,
      delegatedKey,
    )
    ptkForP1(delegatedKey)
  }

  "vetted_packages.propose" in { implicit env =>
    import env.*
    val packageIds = participant1.topology.vetted_packages
      .list(store = TopologyStoreId.Authorized)
      .head
      .item
      .packages

    packageIds should not be empty

    // remove all packages
    participant1.topology.vetted_packages.propose(
      participant1.id,
      packages = Nil,
      force = ForceFlags(ForceFlag.AllowUnvetPackage),
    )
    val result = participant1.topology.vetted_packages
      .list(store = TopologyStoreId.Authorized)
    result should have size 0

    participant1.topology.vetted_packages.propose(participant1.id, packages = packageIds)
    val packageIds3 = participant1.topology.vetted_packages
      .list(store = TopologyStoreId.Authorized)
      .head
      .item
      .packages
    packageIds3 should contain theSameElementsAs packageIds
  }

  "vetted_packages.propose_delta" in { implicit env =>
    import env.*
    def getVettedPackages() = participant1.topology.vetted_packages
      .list(
        store = TopologyStoreId.Authorized,
        filterParticipant = participant1.id.filterString,
      )
      .loneElement

    val startingResult = getVettedPackages()
    val startingPackages = startingResult.item.packages.map(_.packageId)
    val startingSerial = startingResult.context.serial

    val archive = DarParser
      .readArchiveFromFile(new File(CantonTestsPath))
      .getOrElse(fail("cannot read test dar"))
    val adds = VettedPackage.unbounded(archive.all.map(p => DamlPackageStore.readPackageId(p)))

    // first check that we indeed would add new packages
    startingPackages.size should be <= adds.size

    participant1.dars.upload(CantonTestsPath, vetAllPackages = false)

    // vet some more packages
    participant1.topology.vetted_packages.propose_delta(participant1.id, adds = adds)

    val newPackageIdsResult = getVettedPackages()
    newPackageIdsResult.context.serial shouldBe startingSerial.increment
    newPackageIdsResult.item.packages should contain allElementsOf adds
    newPackageIdsResult.item.packages should contain allElementsOf VettedPackage.unbounded(
      startingPackages
    )

    // unvet the starting packages
    participant1.topology.vetted_packages.propose_delta(
      participant1.id,
      removes = startingPackages,
      force = ForceFlags(ForceFlag.AllowUnvetPackage),
    )

    val removedPackagesResult = getVettedPackages()
    removedPackagesResult.context.serial shouldBe newPackageIdsResult.context.serial.increment
    removedPackagesResult.item.packages.map(
      _.packageId
    ) should contain noElementsOf startingPackages

    // having the same package in adds and removes should cause an error
    the[IllegalArgumentException]
      .thrownBy(
        participant1.topology.vetted_packages.propose_delta(
          participant1.id,
          adds = VettedPackage.unbounded(startingPackages),
          removes = startingPackages,
          force = ForceFlags(ForceFlag.AllowUnvetPackage),
        )
      )
      .getMessage should include("Cannot both add and remove a packageId: ")

    participant1.topology.synchronisation.await_idle()
    sequencer1.topology.synchronisation.await_idle()
    mediator1.topology.synchronisation.await_idle()
  }

  "transactions.genesis_state" should {
    "happy path" in { implicit env =>
      import env.*
      val p1GenesisState = participant1.topology.transactions.genesis_state(daId)
      val s1GenesisState = sequencer1.topology.transactions.genesis_state()
      val m1GenesisState = mediator1.topology.transactions.genesis_state()

      p1GenesisState.isEmpty shouldBe false
      p1GenesisState shouldEqual s1GenesisState
      s1GenesisState shouldEqual m1GenesisState
    }
    "fail for invalid filterSynchronizerStore on participants" in { implicit env =>
      import env.*

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.topology.transactions.genesis_state(),
        _.errorMessage should include("reason=>FieldNotSet(filter_synchronizer_store)"),
      )
    }
  }
}

// Default meaning in-memory
//class TopologyConsoleCommandsTestDefault extends TopologyConsoleCommandsTest {
//  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))
//}

class TopologyAdministrationTestPostgres extends TopologyAdministrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
