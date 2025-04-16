// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.daml.nonempty.NonEmpty
import com.daml.test.evidence.scalatest.OperabilityTestHelpers
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.admin.api.client.data.NodeStatus.NotInitialized
import com.digitalasset.canton.admin.api.client.data.{NodeStatus, ParticipantStatus, WaitingForId}
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.InitConfigBase.NodeIdentifierConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DbConfig, IdentityConfig}
import com.digitalasset.canton.console.{
  LocalInstanceReference,
  LocalParticipantReference,
  LocalSequencerReference,
}
import com.digitalasset.canton.crypto.SigningKeyUsage
import com.digitalasset.canton.integration.bootstrap.InitializedSynchronizer
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllMappings
import com.digitalasset.canton.topology.transaction.OwnerToKeyMapping
import com.digitalasset.canton.topology.{Namespace, UniqueIdentifier}
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion

trait MemberAutoInitIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with OperabilityTestHelpers {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S2M2_Manual
      .addConfigTransform(
        ConfigTransforms.disableAutoInit(Set("mediator2", "sequencer2", "participant2"))
      )
      .addConfigTransforms(
        ConfigTransforms.updateMediatorConfig("mediator1")(
          _.focus(_.init.identity).replace(assignIdentity("themediator"))
        ),
        ConfigTransforms.updateSequencerConfig("sequencer1")(
          _.focus(_.init.identity).replace(assignIdentity("thesequencer"))
        ),
        ConfigTransforms.updateParticipantConfig("participant1")(
          _.focus(_.init.identity).replace(assignIdentity("theparticipant"))
        ),
      )

  private def assignIdentity(name: String): IdentityConfig =
    IdentityConfig.Auto(identifier = NodeIdentifierConfig.Explicit(name))

  protected val remedy: String => String => String => ResultOfTaggedAsInvocationOnString =
    operabilityTest("Nodes")("Node identity")

  private def testInitialization(node: LocalInstanceReference, name: String, autoInit: Boolean) = {

    node.health.is_running() shouldBe false
    node.health.has_identity() shouldBe false
    node.health.status match {
      case NodeStatus.Failure(msg) => msg should include("has not been started")
      case msg => fail(msg.toString)
    }
    node.start()
    node.health.is_running() shouldBe true
    clue(s"checking has_identity for $name?") {
      node.health.has_identity() shouldBe autoInit
    }
    if (autoInit) {
      node.identifier.unwrap shouldBe name
    } else {
      node.health.is_ready_for_id() shouldBe true
    }
  }

  private def shouldBeNotInitialized(node: LocalInstanceReference): Assertion =
    node.health.status should matchPattern { case NodeStatus.NotInitialized(true, _) => }

  private def testParticipantCanConnect(
      participant: LocalParticipantReference,
      sequencer: LocalSequencerReference,
      alias: SynchronizerAlias,
  ): Assertion = {
    participant.synchronizers.connect_local(sequencer, alias)
    participant.health.maybe_ping(participant.id) should not be empty
  }

  "Automatic node initialization is enabled" when_ { setting =>
    def createCase(
        nodeFromEnv: TestConsoleEnvironment => LocalInstanceReference,
        base: String,
        extra: LocalInstanceReference => Assertion,
    ): Unit =
      s"The $base node is starting up for the first time" must_ { cause =>
        remedy(setting)(cause)(
          "Should be automatically initialized with the configured identifier"
        ) in { implicit env =>
          val node = nodeFromEnv(env)
          testInitialization(node, "the" + base, autoInit = true)
          extra(node)
        }
      }

    createCase(_.sequencer1, "sequencer", shouldBeNotInitialized)
    createCase(_.mediator1, "mediator", shouldBeNotInitialized)
    createCase(
      _.participant1,
      "participant",
      node =>
        node.health.status.trySuccess match {
          case _: ParticipantStatus => succeed
          case x => fail(s"expected participant status, got ${x.toString}")
        },
    )

    "the participant can connect to the sequencer" in { implicit env =>
      import env.*
      val staticSynchronizerParameters =
        EnvironmentDefinition.defaultStaticSynchronizerParameters
      val synchronizerId = env.bootstrap.synchronizer(
        daName.unwrap,
        sequencers = Seq(sequencer1),
        mediators = Seq(mediator1),
        synchronizerOwners = Seq(sequencer1),
        synchronizerThreshold = PositiveInt.one,
        staticSynchronizerParameters,
      )

      val synchronizerAlias = SynchronizerAlias.tryCreate(daName.unwrap)
      env.initializedSynchronizers.put(
        synchronizerAlias,
        InitializedSynchronizer(
          synchronizerId,
          staticSynchronizerParameters.toInternal,
          synchronizerOwners = Set(sequencer1),
        ),
      )
      testParticipantCanConnect(participant1, sequencer1, daName)
    }
  }

  "Manual initialization is turned on" when_ { setting =>
    def createCase(
        nodeFromEnv: TestConsoleEnvironment => LocalInstanceReference,
        base: String,
        extra: LocalInstanceReference => Assertion,
    ): Unit =
      s"The $base node is starting up for the first time" must_ { cause =>
        remedy(setting)(cause)(
          "Should not be automatically initialized"
        ) in { implicit env =>
          val node = nodeFromEnv(env)
          testInitialization(node, base, autoInit = false)
          extra(node)
        }

        remedy(setting)(cause)("Responds reasonably to Api requests before initialisation") in {
          implicit env =>
            val node = nodeFromEnv(env)
            this.assertThrowsAndLogsCommandFailures(
              node.id,
              _.commandFailureMessage should include(
                "is not initialized and therefore does not have an Id assigned yet"
              ),
            )
            node.health.status shouldBe NotInitialized(active = true, Some(WaitingForId))
        }

        "can be initialized manually" in { implicit env =>
          val node = nodeFromEnv(env)
          // TODO(#23936): provide helper methods to generate namespace, protocol, etc. signing keys
          val namespaceKey =
            node.keys.secret
              .generate_signing_key(
                name = s"${node.name}-${SigningKeyUsage.Namespace.identifier}",
                SigningKeyUsage.NamespaceOnly,
              )
          val sequencerAuthKey =
            node.keys.secret
              .generate_signing_key(
                name = s"${node.name}-${SigningKeyUsage.SequencerAuthentication.identifier}",
                SigningKeyUsage.SequencerAuthenticationOnly,
              )
          val signingKey =
            node.keys.secret
              .generate_signing_key(
                name = s"${node.name}-${SigningKeyUsage.Protocol.identifier}",
                SigningKeyUsage.ProtocolOnly,
              )
          // Only participants need an encryption key, but for simplicity every node gets one
          val encryptionKey =
            node.keys.secret.generate_encryption_key(name = node.name + "-encryption")
          val namespace = Namespace(namespaceKey.id)

          // waiting for ready-for-id is automatically done in init_id,
          // but doing it explicitly here for the purpose of the test
          node.health.wait_for_ready_for_id()
          node.topology.init_id_from_uid(
            UniqueIdentifier.tryCreate("manual-" + base, namespace)
          )

          node.health.wait_for_ready_for_node_topology()
          logger.debug(s"Adding root certificate for manual-$base")
          node.topology.namespace_delegations.propose_delegation(
            namespace,
            namespaceKey,
            CanSignAllMappings,
          )
          logger.debug(s"Adding owner-to-key mappings for manual-$base")
          node.topology.owner_to_key_mappings.propose(
            OwnerToKeyMapping(
              node.id.member,
              NonEmpty(Seq, sequencerAuthKey, signingKey, encryptionKey),
            ),
            serial = PositiveInt.one,
            signedBy =
              Seq(namespaceKey.fingerprint, sequencerAuthKey.fingerprint, signingKey.fingerprint),
          )
        }
      }

    createCase(_.sequencer2, "sequencer", shouldBeNotInitialized)
    createCase(_.mediator2, "mediator", shouldBeNotInitialized)
    createCase(_.participant2, "participant", shouldBeNotInitialized)

    "bootstrap the synchronizer" in { implicit env =>
      import env.*

      logger.debug("Wait until participant2 is fully initialized")
      eventually() {
        participant2.health.status.successOption shouldBe defined
      }

      // At this point, sequencer2 and mediator2 will be waiting
      // to be initialized, which happens either during the synchronizer
      // bootstrap or after having been onboarded to an existing synchronizer.
      sequencer2.health.wait_for_ready_for_initialization()
      mediator2.health.wait_for_ready_for_initialization()

      logger.debug("Bootstrap the synchronizer")

      val staticSynchronizerParameters =
        EnvironmentDefinition.defaultStaticSynchronizerParameters
      val synchronizerId = bootstrap.synchronizer(
        "manualSynchronizer",
        sequencers = Seq(sequencer2),
        mediators = Seq(mediator2),
        synchronizerOwners = Seq(sequencer2),
        synchronizerThreshold = PositiveInt.one,
        staticSynchronizerParameters = staticSynchronizerParameters,
      )
      val synchronizerAlias = SynchronizerAlias.tryCreate("manualSynchronizer")
      env.initializedSynchronizers.put(
        synchronizerAlias,
        InitializedSynchronizer(
          synchronizerId,
          staticSynchronizerParameters.toInternal,
          synchronizerOwners = Set(sequencer2),
        ),
      )

      testParticipantCanConnect(participant2, sequencer2, daName)
    }
  }

}

class MemberAutoInitReferenceIntegrationTestPostgres extends MemberAutoInitIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      MultiSynchronizer(
        Seq(Set(InstanceName.tryCreate("sequencer1")), Set(InstanceName.tryCreate("sequencer2")))
      ),
    )
  )
}

class MemberAutoInitBftOrderingIntegrationTestPostgres extends MemberAutoInitIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer(
        Seq(Set(InstanceName.tryCreate("sequencer1")), Set(InstanceName.tryCreate("sequencer2")))
      ),
    )
  )
}
