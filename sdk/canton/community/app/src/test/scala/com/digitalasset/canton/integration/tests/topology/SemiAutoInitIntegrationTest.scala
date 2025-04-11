// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.daml.test.evidence.scalatest.OperabilityTestHelpers
import com.digitalasset.canton.config.IdentityConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.LocalInstanceReference
import com.digitalasset.canton.crypto.{SigningKeyUsage, SigningPublicKey}
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.{Namespace, UniqueIdentifier}
import com.digitalasset.canton.util.SingleUseCell
import com.digitalasset.canton.version.ProtocolVersion
import monocle.macros.syntax.lens.*

import scala.util.Random

/** Test semi automatic auto initialization
  *
  * Auto-initialization of the identity is split into two stages:
  *   - determining the node identity
  *   - creating the necessary topology transactions
  *
  * Most of our tests are using auto-init. Some are using completely manual init. This test covers
  * the intermediate case where the identity is manually configured, but the topology transactions
  * are automatically created.
  */
class SemiAutoInitIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with OperabilityTestHelpers {

  private val dbSuffix = Random.alphanumeric.map(_.toLower).take(7).mkString

  // use the same database for the two mediator nodes as we want to test the "external" identity config.
  // to test this, we first need to create a key on mediator (like e.g. creating it manually in the KMS).
  // but as we don't use a KMS, we just trick the system by using mediator1 in manual mode, startup the node
  // create the key, shut it down and then continue with mediator2 against the same database.
  private def adjustDbNames(name: String): String =
    if (name.startsWith("mediator")) "mediator"
    else name

  registerPlugin(new UsePostgres(loggerFactory, customDbNames = Some((adjustDbNames, dbSuffix))))

  private val rootCert = better.files.File.newTemporaryFile("root-cert").toJava
  private val intermediateCert = better.files.File.newTemporaryFile("intermediate-cert").toJava
  rootCert.deleteOnExit()
  intermediateCert.deleteOnExit()

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S2M2_Manual
      .addConfigTransforms(
        ConfigTransforms.updateSequencerConfig("sequencer1")(
          _.focus(_.init.identity)
            .replace(IdentityConfig.Manual)
            .focus(
              _.init.generateIntermediateKey
            ) // also testing that intermediate key is generated
            .replace(true)
        ),
        // we will use mediator1 to start and generate a key
        // while mediator2 will be used to test starting up with identity-config external
        ConfigTransforms.updateMediatorConfig("mediator1")(
          _.focus(_.init.identity)
            .replace(IdentityConfig.Manual)
        ),
        ConfigTransforms.updateMediatorConfig("mediator2")(
          _.focus(_.init.identity)
            .replace(
              IdentityConfig.External(
                identifier = "themediator",
                certificates = Seq(rootCert, intermediateCert),
              )
            )
        ),
        ConfigTransforms.updateParticipantConfig("participant1")(
          _.focus(_.init.identity)
            .replace(IdentityConfig.Manual)
        ),
      )
      .withSetup(_.participant2.start())

  "identity is manual, but keys are automatic" when {
    "node can be started" in { implicit env =>
      import env.*

      sequencer1.start()
      eventually() {
        sequencer1.health.is_ready_for_id() shouldBe true
      }
      // create namespace key
      val namespace = sequencer1.keys.secret
        .generate_signing_key("my-namespace-key", usage = SigningKeyUsage.NamespaceOnly)
      // initialize node id
      sequencer1.topology.init_id(
        UniqueIdentifier.tryCreate("mysequencer1", namespace.fingerprint)
      )
      // wait until node is initialized
      eventually() {
        sequencer1.health.initialized()
      }

      sequencer1.health.is_ready_for_initialization() shouldBe true

    }

    "node has created and used the intermediate certificate" in { implicit env =>
      import env.*
      val intermediateKey = sequencer1.keys.secret
        .list(filterName = "sequencer1-intermediate-namespace")
        .headOption
        .valueOrFail("should have")
      val otk =
        sequencer1.topology.owner_to_key_mappings.list().headOption.valueOrFail("should have")
      otk.context.signedBy.forgetNE should contain(intermediateKey.id)
    }

    "node can be restarted" in { implicit env =>
      import env.*
      sequencer1.stop()
      sequencer1.start()

      eventually() {
        sequencer1.health.initialized()
      }

    }
  }

  private def createExternalRootKey(
      offsite: LocalInstanceReference,
      node: LocalInstanceReference,
      name: String,
  ): (GenericSignedTopologyTransaction, GenericSignedTopologyTransaction, SigningPublicKey) = {
    val intermediateKey = node.keys.secret
      .generate_signing_key("intermediate-signing-key", SigningKeyUsage.NamespaceOnly)

    val rootKey = offsite.keys.secret.generate_signing_key(
      "external-root-namespace-for-" + name,
      SigningKeyUsage.NamespaceOnly,
    )

    val storeId = offsite.topology.stores
      .create_temporary_topology_store("root-key-signing-" + name, ProtocolVersion.latest)
    val rootNd = offsite.topology.namespace_delegations.propose_delegation(
      Namespace(rootKey.id),
      rootKey,
      isRootDelegation = true,
      store = storeId,
      mustFullyAuthorize = true,
    )

    val intermediateNd = offsite.topology.namespace_delegations.propose_delegation(
      Namespace(rootKey.id),
      intermediateKey,
      isRootDelegation = false,
      store = storeId,
      mustFullyAuthorize = true,
    )

    (rootNd, intermediateNd, intermediateKey)

  }

  "identity is external, but keys are automatic" when {
    "we can startup and create a signing key" in { implicit env =>
      import env.*

      mediator1.start()
      mediator1.health.wait_for_ready_for_id()
      val (rootNd, intermediateNd, intermediateKey) =
        createExternalRootKey(participant2, mediator1, "mediator")
      mediator1.stop()
      rootNd.writeToFile(rootCert.getPath)
      intermediateNd.writeToFile(intermediateCert.getPath)
      mediator2.start()
      eventually() {
        mediator2.health.is_ready_for_initialization() shouldBe true
      }
      mediator2.keys.secret.list().map(_.id) should contain(intermediateKey.id)

    }

    "mediator can be restarted and resumes without certs" in { implicit env =>
      import env.*
      mediator2.stop()
      rootCert.delete()
      mediator2.start()
      eventually() {
        mediator2.health.is_ready_for_initialization() shouldBe true
      }
    }

  }

  "participant can start with external init via admin-api" should {

    val certs = new SingleUseCell[
      (GenericSignedTopologyTransaction, GenericSignedTopologyTransaction, SigningPublicKey)
    ]()

    "start up and create certs" in { implicit env =>
      import env.*

      participant1.start()
      participant1.health.wait_for_ready_for_id()

      val (rootNd, intermediateNd, intermediateKey) =
        createExternalRootKey(participant2, participant1, "participant")
      intermediateNd.writeToFile(intermediateCert.getPath)
      certs.putIfAbsent((rootNd, intermediateNd, intermediateKey))
    }

    "refuse to start with invalid certs" in { implicit env =>
      import env.*

      val (rootNd, _, intermediateKey) = certs.get.valueOrFail("should have certs")

      // fail to start with wrong uid
      this.assertThrowsAndLogsCommandFailures(
        participant1.topology.init_id(
          //  using wrong namespace of intermediateKey and not the rootKey
          UniqueIdentifier.tryCreate("myparticipant", intermediateKey.fingerprint),
          delegationFiles = Seq(intermediateCert.getPath),
          delegations = Seq(rootNd),
        ),
        _.errorMessage should include("does not match the certificate"),
      )

      // fail to start without root cert
      this.assertThrowsAndLogsCommandFailures(
        participant1.topology.init_id(
          UniqueIdentifier.tryCreate("myparticipant", rootNd.mapping.namespace),
          delegationFiles = Seq(intermediateCert.getPath),
        ),
        _.errorMessage should include("No delegation found for keys"),
      )

      // fail to start without intermediate cert
      this.assertThrowsAndLogsCommandFailures(
        participant1.topology.init_id(
          UniqueIdentifier.tryCreate("myparticipant", rootNd.mapping.namespace),
          delegations = Seq(rootNd),
        ),
        _.errorMessage should include("No signing key found locally for the provided namespace"),
      )

    }

    "successfully start with valid certs" in { implicit env =>
      import env.*

      val (rootNd, intermediateNd, intermediateKey) = certs.get.valueOrFail("should have certs")

      participant1.topology.init_id(
        UniqueIdentifier.tryCreate("myparticipant", rootNd.mapping.namespace),
        delegationFiles = Seq(intermediateCert.getPath),
        delegations = Seq(rootNd),
      )

    }

    "happily restart" in { implicit env =>
      import env.*
      participant1.stop()
      participant1.start()

    }

  }

  "finally, bootstrap and ping" in { implicit env =>
    import env.*

    val bootstrap = new NetworkBootstrapper(
      NetworkTopologyDescription(
        synchronizerAlias = daName,
        synchronizerOwners = Seq(sequencer1),
        synchronizerThreshold = PositiveInt.tryCreate(1),
        sequencers = Seq(sequencer1),
        mediators = Seq(mediator2),
      )
    )
    bootstrap.bootstrap()

    participant1.synchronizers.connect_local(sequencer1, daName)

    participant1.health.ping(participant1)

  }

}
