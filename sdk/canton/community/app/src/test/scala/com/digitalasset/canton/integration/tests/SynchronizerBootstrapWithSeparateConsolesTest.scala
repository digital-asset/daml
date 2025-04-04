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
import com.digitalasset.canton.topology.{ForceFlag, SynchronizerId, UniqueIdentifier}

/** This test simulates the case when the mediator and sequencer nodes are in separate consoles and
  * we want to bootstrap the synchronizer. We do that by exchanging the essential information via
  * files.
  */
trait SynchronizerBootstrapWithSeparateConsolesIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasExecutionContext {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 1,
        numSequencers = 1,
        numMediators = 1,
      )

  // in this test we interleave various consoles, but we can assume that the individual nodes all have
  // the synchronizerId in scope after the decentralized namespace definition
  protected var synchronizerId: SynchronizerId = _

  "Nodes in separate consoles" should {
    "be able to bootstrap distributed synchronizer by exchanging files" in { implicit env =>
      import env.*

      val synchronizerName = daName.unwrap

      // the process below is equivalent to calling
      // bootstrap.synchronizer(
      //   daName.unwrap,
      //   StaticSynchronizerParameters(EnvironmentDefinition.defaultStaticSynchronizerParameters),
      //   synchronizerOwners = Seq(sequencer1, mediator1),
      //   sequencers = Seq(sequencer1),
      //   mediators = Seq(mediator1),
      // )
      for {
        // this file will contain the static synchronizer params
        paramsFile <- File.temporaryFile("params-file", ".proto").map(_.canonicalPath)
        // this file will be used for sharing a decentralized namespace transaction
        decentralizedNamespaceFile <- File
          .temporaryFile("decentralized_namespace", ".proto")
          .map(_.canonicalPath)
        // this file will be used for sharing identity transactions
        identityFile <- File.temporaryFile("identity", ".proto").map(_.canonicalPath)
        // this file will be used for synchronizer bootstrap transactions
        synchronizerBootstrapFile <- File.temporaryFile("identity", ".proto").map(_.canonicalPath)
        // this file will be used to share the sequencer connection
        sequencerConnectionFile <- File
          .temporaryFile("sequencer_connection", ".proto")
          .map(_.canonicalPath)
      } yield {
        // all synchronizer founders share their identifiers
        val mediatorId = mediator1.id
        val sequencerId = sequencer1.id

        // All synchronizer founders need to agree on the static synchronizer parameters. They don't have to be passed around as file,
        // but for the sake of this test, we do it here.
        {
          EnvironmentDefinition.defaultStaticSynchronizerParameters.writeToFile(
            paramsFile
          )
        }

        // Sequencer's console:
        // * extract the sequencer's identity topology transactions and share via identityFile
        {
          sequencer1.topology.transactions.export_identity_transactions(identityFile)
        }

        // Mediator's console:
        // * load the sequencer's identity topology transactions
        // * share its own identity topology transactions via identityFile
        {
          mediator1.topology.transactions
            .import_topology_snapshot_from(identityFile, TopologyStoreId.Authorized)

          mediator1.topology.transactions.export_identity_transactions(identityFile)
        }

        // Sequencer's console:
        // * load the mediator's identity topology transactions
        // * generate the decentralized namespace declaration and share via decentralizedNamespaceFile
        {
          // load mediator's identity
          sequencer1.topology.transactions
            .import_topology_snapshot_from(identityFile, TopologyStoreId.Authorized)

          // propose the decentralized namespace declaration with the sequencer's signature
          val seqDND = sequencer1.topology.decentralized_namespaces.propose_new(
            owners = Set(sequencerId.namespace, mediatorId.namespace),
            threshold = PositiveInt.one,
            store = TopologyStoreId.Authorized,
          )

          // share the decentralized namespace declaration
          seqDND.writeToFile(decentralizedNamespaceFile)
          synchronizerId = SynchronizerId(
            UniqueIdentifier.tryCreate(synchronizerName, seqDND.mapping.namespace.toProtoPrimitive)
          )
        }

        // Mediator's console:
        // * load the sequencer's decentralized namespace declaration
        // * sign the decentralized namespace declaration and share it again (this time including the mediator's signature) via decentralizedNamespaceFile
        // * generate the synchronizer bootstrap transactions with the mediator's signature and share via synchronizerBootstrapFile
        {
          // load sequencer's decentralized namespace declaration
          mediator1.topology.transactions.load_single_from_file(
            decentralizedNamespaceFile,
            TopologyStoreId.Authorized,
            ForceFlag.AlienMember,
          )

          // propose the same decentralized namespace declaration, but this time with the mediator's signature
          val medDND = mediator1.topology.decentralized_namespaces.propose_new(
            owners = Set(sequencerId.namespace, mediatorId.namespace),
            threshold = PositiveInt.one,
            store = TopologyStoreId.Authorized,
          )
          medDND.writeToFile(decentralizedNamespaceFile)

          // generate the synchronizer bootstrap transactions with the mediator's signature
          mediator1.topology.synchronizer_bootstrap.download_genesis_topology(
            synchronizerId,
            synchronizerOwners = Seq(mediatorId, sequencerId),
            sequencers = Seq(sequencerId),
            mediators = Seq(mediatorId),
            outputFile = synchronizerBootstrapFile,
            store = TopologyStoreId.Authorized,
          )
        }

        // Sequencer's console:
        // * load the mediator's decentralized namespace declaration
        // * load the mediator's synchronizer bootstrap transactions
        // * generate the sequencer's synchronizer bootstrap transactions, merging signatures from the sequencer and the mediator
        // * bootstrap the sequencer with the fully authorized initial topology snapshot
        // * share the sequencer connection via sequencerConnectionFile
        {
          // load mediator's identity and decentralized namespace declaration
          sequencer1.topology.transactions.load_single_from_file(
            decentralizedNamespaceFile,
            TopologyStoreId.Authorized,
            ForceFlag.AlienMember,
          )

          // load mediator's synchronizer bootstrap
          sequencer1.topology.transactions.load_multiple_from_file(
            synchronizerBootstrapFile,
            TopologyStoreId.Authorized,
            ForceFlag.AlienMember,
          )

          // create sequencer's synchronizer bootstrap, effectively signing the mediator's
          // bootstrap transactions with the sequencer's signature
          sequencer1.topology.synchronizer_bootstrap.generate_genesis_topology(
            synchronizerId,
            synchronizerOwners = Seq(mediatorId, sequencerId),
            sequencers = Seq(sequencerId),
            mediators = Seq(mediatorId),
            store = TopologyStoreId.Authorized,
          )

          // create the initial topology snapshot by loading all transactions from the sequencer's authorized store
          val initialSnapshot = sequencer1.topology.transactions
            .export_topology_snapshot(store = TopologyStoreId.Authorized)

          // load the static synchronizer parameters
          val synchronizerParams = StaticSynchronizerParameters.tryReadFromFile(paramsFile)

          // and finally initialize the sequencer with the topology snapshot
          sequencer1.setup.assign_from_genesis_state(initialSnapshot, synchronizerParams)

          // share the sequencer connection via sequencerConnectionFile
          SequencerConnections
            .single(sequencer1.sequencerConnection)
            .writeToFile(sequencerConnectionFile)
        }

        // Mediator's console:
        // * read the sequencer connection and synchronizer parameters
        // * initialize the mediator with the sequencer connection and synchronizer parameters
        {
          val sequencerConnections =
            SequencerConnections.tryReadFromTrustedFile(sequencerConnectionFile)

          mediator1.setup.assign(
            synchronizerId,
            sequencerConnections,
          )
          mediator1.health.wait_for_initialized()
        }
      }

      // verify that the synchronizer is properly bootstrapped
      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant1.health.ping(participant1)
    }
  }
}

class SynchronizerBootstrapWithSeparateConsolesIntegrationTestH2
    extends SynchronizerBootstrapWithSeparateConsolesIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))
}
