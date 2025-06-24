// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import better.files.*
import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.plugins.{UseCommunityReferenceBlockSequencer, UseH2}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.{
  ForceFlag,
  PhysicalSynchronizerId,
  SynchronizerId,
  UniqueIdentifier,
}

import scala.concurrent.duration.DurationInt

/** This test simulates the case when the mediator and sequencer nodes are in separate consoles and
  * we want to bootstrap the synchronizer. We do that by exchanging the essential information via
  * files.
  */
trait SynchronizerBootstrapWithSeparateConsolesIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasExecutionContext {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S2M2_Config

  // in this test we interleave various consoles, but we can assume that the individual nodes all have
  // the synchronizerId in scope after the decentralized namespace definition
  protected var synchronizerId: SynchronizerId = _
  protected var physicalSynchronizerId: PhysicalSynchronizerId = _

  "Nodes in separate consoles" should {
    "be able to bootstrap distributed synchronizer by exchanging files" in { implicit env =>
      import env.*

      val synchronizerName = daName.unwrap

      // the process below is equivalent to calling
      // bootstrap.synchronizer(
      //   daName.unwrap,
      //   StaticSynchronizerParameters(EnvironmentDefinition.defaultStaticSynchronizerParameters),
      //   synchronizerOwners = Seq(sequencer1, sequencer2),
      //   sequencers = Seq(sequencer1, sequencer2),
      //   mediators = Seq(mediator1, mediator2),
      // )
      for {
        // this file will contain the static synchronizer params
        paramsFile <- File.temporaryFile("params", ".proto").map(_.canonicalPath)
        // this file will be used for sharing a decentralized namespace transaction
        decentralizedNamespaceFile <- File
          .temporaryFile("decentralizedNamespace", ".proto")
          .map(_.canonicalPath)
        // these files will be used for sharing identity+pubkey transactions
        seqIdentityFile <- File.temporaryFile("identitySeq", ".proto").map(_.canonicalPath)
        medIdentityFile <- File.temporaryFile("identityMed", ".proto").map(_.canonicalPath)
        // this file will be used for synchronizer bootstrap transactions
        synchronizerBootstrapFile <- File
          .temporaryFile("syncBootstrap", ".proto")
          .map(_.canonicalPath)
      } yield {
        // all synchronizer founders, sequencers and mediators share their identifiers
        val sequencer1Id = sequencer1.id
        val sequencer2Id = sequencer2.id

        // All synchronizer founders need to agree on the static synchronizer parameters. They don't have to be passed around as file,
        // but for the sake of this test, we do it here.
        {
          EnvironmentDefinition.defaultStaticSynchronizerParameters.writeToFile(
            paramsFile
          )
        }

        // load the static synchronizer parameters
        val synchronizerParams = StaticSynchronizerParameters.tryReadFromFile(paramsFile)

        // Sequencer1 console:
        // * extract sequencer1's and mediator1's identity+pubkey topology transactions and share via files
        // * load mediator1's identity+pubkey topology transactions
        {
          sequencer1.topology.transactions.export_identity_transactions(seqIdentityFile)
          mediator1.topology.transactions.export_identity_transactions(medIdentityFile)

          sequencer1.topology.transactions
            .import_topology_snapshot_from(medIdentityFile, TopologyStoreId.Authorized)
        }

        // Sequencer2 console:
        // * load sequencer1's and mediator1's identity+pubkey topology transactions
        // * extract sequencer2's and mediator2's identity+pubkey topology transactions and share via files
        // * load mediator2's identity+pubkey topology transactions
        {
          sequencer2.topology.transactions
            .import_topology_snapshot_from(seqIdentityFile, TopologyStoreId.Authorized)
          sequencer2.topology.transactions
            .import_topology_snapshot_from(medIdentityFile, TopologyStoreId.Authorized)

          sequencer2.topology.transactions.export_identity_transactions(seqIdentityFile)
          mediator2.topology.transactions.export_identity_transactions(medIdentityFile)

          sequencer2.topology.transactions
            .import_topology_snapshot_from(medIdentityFile, TopologyStoreId.Authorized)
        }

        // Sequencer1 console:
        // * load sequencer2's identity topology transactions
        // * generate the decentralized namespace declaration and share via decentralizedNamespaceFile
        {
          // load sequencer2's identity
          sequencer1.topology.transactions
            .import_topology_snapshot_from(seqIdentityFile, TopologyStoreId.Authorized)
          sequencer1.topology.transactions
            .import_topology_snapshot_from(medIdentityFile, TopologyStoreId.Authorized)

          // propose the decentralized namespace declaration with the sequencer's signature
          val seq1DND = sequencer1.topology.decentralized_namespaces.propose_new(
            owners = Set(sequencer1Id.namespace, sequencer2Id.namespace),
            threshold = PositiveInt.two,
            store = TopologyStoreId.Authorized,
          )

          // share the decentralized namespace declaration
          seq1DND.writeToFile(decentralizedNamespaceFile)

          synchronizerId = SynchronizerId(
            UniqueIdentifier.tryCreate(synchronizerName, seq1DND.mapping.namespace.toProtoPrimitive)
          )

          physicalSynchronizerId =
            PhysicalSynchronizerId(synchronizerId, synchronizerParams.toInternal)
        }

        // Sequencer2 console:
        // * load sequencer1's decentralized namespace declaration
        // * sign the decentralized namespace declaration and share it again (this time including sequencer1's signature) via decentralizedNamespaceFile
        // * generate the synchronizer bootstrap transactions with the sequencer1's signature and share via synchronizerBootstrapFile
        {
          // load sequencer1's decentralized namespace declaration
          sequencer2.topology.transactions.load_single_from_file(
            decentralizedNamespaceFile,
            TopologyStoreId.Authorized,
            ForceFlag.AlienMember,
          )

          // propose the same decentralized namespace declaration, but this time with sequencer2's signature
          val seq2DND = sequencer2.topology.decentralized_namespaces.propose_new(
            owners = Set(sequencer1Id.namespace, sequencer2Id.namespace),
            threshold = PositiveInt.two,
            store = TopologyStoreId.Authorized,
          )
          seq2DND.writeToFile(decentralizedNamespaceFile)

          // generate and export the synchronizer bootstrap transactions with sequencer2's signature
          sequencer2.topology.synchronizer_bootstrap.download_genesis_topology(
            physicalSynchronizerId,
            synchronizerOwners = Seq(sequencer1Id, sequencer2Id),
            sequencers = Seq(sequencer1Id, sequencer2Id),
            mediators = Seq(mediator1.id, mediator2.id),
            outputFile = synchronizerBootstrapFile,
            store = TopologyStoreId.Authorized,
          )
        }

        // Sequencer1's console:
        // * load sequencer2's decentralized namespace declaration
        // * load sequencer2's synchronizer bootstrap transactions
        // * generate the sequencer's synchronizer bootstrap transactions, merging signatures from the sequencers
        // * bootstrap the sequencer with the fully authorized initial topology snapshot
        {
          // load sequencer2's identity and decentralized namespace declaration
          sequencer1.topology.transactions.load_single_from_file(
            decentralizedNamespaceFile,
            TopologyStoreId.Authorized,
            ForceFlag.AlienMember,
          )

          // load sequencer2's synchronizer bootstrap
          sequencer1.topology.transactions.load_multiple_from_file(
            synchronizerBootstrapFile,
            TopologyStoreId.Authorized,
            ForceFlag.AlienMember,
          )

          // generate and export the synchronizer bootstrap transactions with sequencer1's signature
          sequencer1.topology.synchronizer_bootstrap.download_genesis_topology(
            physicalSynchronizerId,
            synchronizerOwners = Seq(sequencer1Id, sequencer2Id),
            sequencers = Seq(sequencer1Id, sequencer2Id),
            mediators = Seq(mediator1.id, mediator2.id),
            outputFile = synchronizerBootstrapFile,
            store = TopologyStoreId.Authorized,
          )

          // create the initial topology snapshot by loading all transactions from sequencer1's authorized store
          val initialSnapshot =
            sequencer1.topology.transactions
              .export_topology_snapshot(store = TopologyStoreId.Authorized)

          // load the static synchronizer parameters
          val synchronizerParams = StaticSynchronizerParameters.tryReadFromFile(paramsFile)

          // and finally initialize the sequencer with the topology snapshot
          sequencer1.setup.assign_from_genesis_state(initialSnapshot, synchronizerParams)
        }

        // Sequencer2's console:
        // * load the synchronizer bootstrap transactions with both sequencers' signatures
        // * bootstrap the sequencer with the fully authorized initial topology snapshot
        {
          // load bootstrap signed by both sequencers
          sequencer2.topology.transactions.load_multiple_from_file(
            synchronizerBootstrapFile,
            TopologyStoreId.Authorized,
            ForceFlag.AlienMember,
          )

          // create the initial topology snapshot by loading all transactions from the sequencer's authorized store
          val initialSnapshot =
            sequencer2.topology.transactions
              .export_topology_snapshot(store = TopologyStoreId.Authorized)

          // load the static synchronizer parameters
          val synchronizerParams = StaticSynchronizerParameters.tryReadFromFile(paramsFile)

          // and finally initialize the sequencer with the topology snapshot
          sequencer2.setup.assign_from_genesis_state(initialSnapshot, synchronizerParams)
        }

        // Sequencer1 console:
        // * initialize mediator1 with the sequencer connection and synchronizer parameters
        {
          mediator1.setup.assign(
            physicalSynchronizerId,
            SequencerConnections.single(sequencer1.sequencerConnection),
          )
          mediator1.health.wait_for_initialized()
        }

        // Sequencer2 console:
        // * initialize mediator2 with the sequencer connection and synchronizer parameters
        {
          mediator2.setup.assign(
            physicalSynchronizerId,
            SequencerConnections.single(sequencer2.sequencerConnection),
          )
          mediator2.health.wait_for_initialized()
        }
      }

      // verify that the synchronizer is properly bootstrapped by pinging between two participants
      //  connected to different sequencers
      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant2.synchronizers.connect_local(sequencer2, alias = daName)
      participant1.health.ping(participant2, timeout = 30.seconds)
    }
  }
}

class SynchronizerBootstrapWithSeparateConsolesIntegrationTestH2
    extends SynchronizerBootstrapWithSeparateConsolesIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))
}
