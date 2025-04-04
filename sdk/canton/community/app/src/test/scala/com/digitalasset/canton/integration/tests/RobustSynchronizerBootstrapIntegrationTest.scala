// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.commands.{MediatorSetupGroup, SequencerAdministration}
import com.digitalasset.canton.console.{
  CommandFailure,
  ConsoleEnvironment,
  LocalMediatorReference,
  LocalSequencerReference,
  MediatorReference,
  SequencerReference,
}
import com.digitalasset.canton.crypto.KeyPurpose
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.sequencing.{SequencerConnectionValidation, SequencerConnections}
import com.digitalasset.canton.synchronizer.mediator.MediatorNodeConfig
import com.digitalasset.canton.synchronizer.sequencer.admin.grpc.InitializeSequencerResponse
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeConfig
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId.Temporary
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.{StoredTopologyTransactions, TimeQuery}
import com.digitalasset.canton.topology.transaction.OwnerToKeyMapping
import com.digitalasset.canton.topology.{SynchronizerId, TopologyManagerError}
import com.digitalasset.canton.{HasExecutionContext, SynchronizerAlias}
import com.google.protobuf.ByteString
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicInteger

/*
 * Verifies that a the bootstrap process can be retried after failures
 * from initializing the sequencer and mediator
 */
sealed trait RobustSynchronizerBootstrapIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasExecutionContext {

  private def name(name: String) = InstanceName.tryCreate(name)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4S1M1_Manual
      .addConfigTransform(c =>
        c.focus(_.sequencers)
          .replace(
            c.sequencers +
              (name("secondSequencer") -> SequencerNodeConfig()) +
              (name("sequencerToFail") -> SequencerNodeConfig())
          )
          .focus(_.mediators)
          .replace(c.mediators + (name("secondMediator") -> MediatorNodeConfig()))
      )
      .addConfigTransform(ConfigTransforms.globallyUniquePorts)
      // Apply protocol version updates to the `second` nodes defined above
      .addConfigTransforms(ConfigTransforms.optSetProtocolVersion*)

  private def mediatorThatFailsFirstInit(
      mediatorReference: LocalMediatorReference,
      env: ConsoleEnvironment,
  ): LocalMediatorReference =
    new LocalMediatorReference(env, mediatorReference.name) {
      val counter = new AtomicInteger(0)
      override def setup: MediatorSetupGroup = new MediatorSetupGroup(this) {
        override def assign(
            synchronizerId: SynchronizerId,
            sequencerConnections: SequencerConnections,
            validation: SequencerConnectionValidation,
            waitForReady: Boolean,
        ): Unit =
          if (counter.incrementAndGet() == 1)
            throw new RuntimeException("First time mediator init fails")
          else super.assign(synchronizerId, sequencerConnections, validation)
      }
    }

  private def sequencerThatFailsFirstInit(
      sequencerReference: LocalSequencerReference,
      env: ConsoleEnvironment,
  ): LocalSequencerReference =
    new LocalSequencerReference(env, sequencerReference.name) {
      val counter = new AtomicInteger(0)
      override def setup: SequencerAdministration = new SequencerAdministration(this) {
        override def assign_from_genesis_state(
            genesisState: ByteString,
            synchronizerParameters: StaticSynchronizerParameters,
            waitForReady: Boolean,
        ): InitializeSequencerResponse =
          if (counter.incrementAndGet() == 1)
            throw new RuntimeException("First time sequencer init fails")
          else
            super.assign_from_genesis_state(genesisState, synchronizerParameters)
      }
    }

  var synchronizerId1: SynchronizerId = _

  "distributed environment" should {
    "fail if trying to bootstrap a sequencer with multiple effective transactions per unique key" in {
      implicit env =>
        import env.*

        val sequencerToFail = ls("sequencerToFail")
        sequencerToFail.start()

        // for this test we construct a topology snapshot with two effective OTK transaction serial=1 and serial=2.
        // since the sequencer anyway should reject this topology snapshot, it doesn't matter that the snapshot is not actually suitable
        // for initialization.

        // create OTK serial=2
        val encKey = sequencerToFail.keys.secret.generate_encryption_key("enc_key_test")
        sequencerToFail.topology.owner_to_key_mappings.add_key(
          encKey.fingerprint,
          KeyPurpose.Encryption,
        )

        // fetch the two OTKs
        val otks = sequencerToFail.topology.transactions
          .list(
            filterMappings = Seq(OwnerToKeyMapping.code),
            timeQuery = TimeQuery.Range(None, None),
            filterNamespace = sequencerToFail.id.uid.namespace.filterString,
          )
          .result
          .sortBy(_.serial)

        // now we explicitly set all validUntil=None to force the error
        val multipleFullyAuthorized = otks.map(_.copy(validUntil = None))

        // let's have effective proposals for both serials
        val proposals = multipleFullyAuthorized.map(_.focus(_.transaction.isProposal).replace(true))

        def assertFailure(
            transactions: Seq[GenericStoredTopologyTransaction],
            errorMessage: String,
        ): Assertion =
          assertThrowsAndLogsCommandFailures(
            sequencerToFail.setup.assign_from_genesis_state(
              StoredTopologyTransactions(transactions).toByteString(testedProtocolVersion),
              StaticSynchronizerParameters.defaultsWithoutKMS(testedProtocolVersion),
            ),
            _.shouldBeCommandFailure(
              TopologyManagerError.InconsistentTopologySnapshot,
              errorMessage,
            ),
          )

        assertFailure(
          multipleFullyAuthorized,
          "concurrently effective transactions with the same unique key",
        )

        assertFailure(
          proposals,
          "muliple effective proposals with different serials",
        )
        assertFailure(
          proposals ++ proposals,
          "multiple effective proposals for the same transaction hash",
        )

        assertFailure(
          otks ++ proposals,
          "effective proposals with serial less than or equal to the highest fully authorized transaction",
        )

    }

    "succeed bootstrapping after retrying multiple times" in { implicit env =>
      import env.*

      val mediator: LocalMediatorReference = mediatorThatFailsFirstInit(mediator1, env)
      val sequencer: LocalSequencerReference = sequencerThatFailsFirstInit(sequencer1, env)

      mediator.start()
      sequencer.start()

      def bootstrapSynchronizer = bootstrap.synchronizer(
        daName.unwrap,
        synchronizerOwners = Seq(sequencer),
        synchronizerThreshold = PositiveInt.one,
        sequencers = Seq(sequencer),
        mediators = Seq(mediator),
        staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
      )

      clue("bootstrap synchronizer #1")(intercept[RuntimeException] {
        bootstrapSynchronizer
      }.getMessage shouldBe "First time sequencer init fails")

      clue("bootstrap synchronizer #2")(intercept[RuntimeException] {
        sequencer1.topology.stores.drop_temporary_topology_store(
          Temporary.tryCreate(s"${daName.unwrap}-setup")
        )
        bootstrapSynchronizer
      }.getMessage shouldBe "First time mediator init fails")

      // after failing to initialize the sequencer and mediator, it will now succeed
      clue("bootstrap synchronizer #3") {
        sequencer1.topology.stores.drop_temporary_topology_store(
          Temporary.tryCreate(s"${daName.unwrap}-setup")
        )
        synchronizerId1 = bootstrapSynchronizer
      }
      sequencer.health.wait_for_initialized()
      mediator.health.wait_for_initialized()
      clue("start participants") {
        participant1.start()
        participant2.start()
      }

      participant1.synchronizers.connect_local(sequencer, daName)
      participant2.synchronizers.connect_local(sequencer, daName)

      participant1.health.ping(participant2.id)

      // manually stop all nodes so the sequencer will have no active connections and can shutdown cleanly
      participant1.stop()
      participant2.stop()
      sequencer.stop()
      mediator.stop()
    }

    "fail if trying to boostrap a synchronizer with a sequencer or mediator already initialized previously with another synchronizer" in {
      implicit env =>
        import env.*

        val sequencer2 = s("secondSequencer")
        val mediator2 = m("secondMediator")

        sequencers.local.start()
        mediators.local.start()

        def bootstrapSynchronizer(
            sequencer: SequencerReference,
            mediator: MediatorReference,
            name: SynchronizerAlias,
        ) = bootstrap.synchronizer(
          name.unwrap,
          synchronizerOwners = Seq(sequencer),
          synchronizerThreshold = PositiveInt.one,
          sequencers = Seq(sequencer),
          mediators = Seq(mediator),
          staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
        )

        clue(
          "bootstrap with sequencer1 and mediator1 and the same name as they have already been bootstrapped with"
        )(
          loggerFactory.assertLogsSeq(SuppressionRule.Level(Level.INFO))(
            bootstrapSynchronizer(sequencer1, mediator1, daName),
            entries =>
              forAtLeast(1, entries) {
                _.infoMessage should include(
                  s"Synchronizer ${daName.unwrap} has already been bootstrapped with ID"
                )
              },
          )
        )
        clue(
          "bootstrap with sequencer1 and mediator1 and a different name as they have already been bootstrapped with"
        )(
          loggerFactory.assertThrowsAndLogs[CommandFailure](
            bootstrapSynchronizer(sequencer1, mediator1, acmeName),
            _.errorMessage should include(
              s"The synchronizer cannot be bootstrapped: ${sequencer1.id.member} has already been initialized for synchronizer $synchronizerId1 instead of"
            ),
          )
        )
        clue("bootstrap with sequencer2 and mediator1")(
          loggerFactory.assertThrowsAndLogs[CommandFailure](
            bootstrapSynchronizer(sequencer2, mediator1, acmeName),
            _.errorMessage should include(
              s"The synchronizer cannot be bootstrapped: ${mediator1.id.member} has already been initialized for synchronizer $synchronizerId1"
            ),
          )
        )

        clue("boostrap with sequencer2 and mediator2")(
          bootstrapSynchronizer(sequencer2, mediator2, acmeName)
        )

        participant3.start()
        participant4.start()

        participant3.synchronizers.connect_local(sequencer2, daName)
        participant4.synchronizers.connect_local(sequencer2, daName)
        participant3.health.ping(participant4.id)

        nodes.local.stop()
    }
  }
}

class RobustSynchronizerBootstrapIntegrationTestPostgres
    extends RobustSynchronizerBootstrapIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("secondSequencer"), Set("sequencerToFail")).map(
          _.map(InstanceName.tryCreate)
        )
      ),
    )
  )
}
