// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade

import better.files.File
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.admin.api.client.data.{
  SequencerConnectionPoolDelays,
  SequencerConnections,
  StaticSynchronizerParameters,
  SubmissionRequestAmplification,
}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  InstanceReference,
  LocalInstanceReference,
  LocalMediatorReference,
  LocalSequencerReference,
  MediatorReference,
  SequencerReference,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.tests.upgrade.LogicalUpgradeUtils.{
  SynchronizerNodes,
  UpgradeDataFiles,
}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.sequencing.client.{SendCallback, SendResult}
import com.digitalasset.canton.sequencing.protocol.{Batch, Deliver}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.{NamespaceDelegation, OwnerToKeyMapping}
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.{BaseTest, FutureHelpers, SequencerAlias}
import com.google.protobuf.ByteString
import org.scalatest.Assertion
import org.scalatest.OptionValues.*
import org.scalatest.matchers.should.Matchers.*

trait LogicalUpgradeUtils extends FutureHelpers {
  protected def testName: String
  protected def logger: TracedLogger

  /** Export nodes data for the specified nodes.
    * @return
    *   Directory containing the data
    */
  protected def exportNodesData(
      nodes: SynchronizerNodes,
      successorPSId: PhysicalSynchronizerId,
  ): File = {
    val exportDirectory: File = File.newTemporaryDirectory(s"$testName-$successorPSId")

    logger.info(s"Starting to export nodes data for upgrade into directory $exportDirectory")(
      TraceContext.empty
    )

    val sequencers = nodes.sequencers

    nodes.all.foreach(writeUidToFile)
    nodes.all.foreach(writeKeysToFile)
    nodes.all.foreach(writeAuthorizeStoreToFile)
    sequencers.foreach(writeSequencerGenesisState)

    // Helpers
    def writeUidToFile(node: InstanceReference): Unit =
      BinaryFileUtil.writeByteStringToFile(
        s"${exportDirectory / node.name}-uid",
        ByteString.copyFromUtf8(node.id.uid.toProtoPrimitive),
      )

    def writeKeysToFile(
        node: InstanceReference
    ): Unit = {
      val publicKeysWithNames = node.keys.public.list()
      publicKeysWithNames.foreach { publicKey =>
        /*
        We want all keys to be saved in a file whose name is `nodeName-keyPurpose.keys`

        If we don't change the name explicitly, then consecutive LSUs fail:
        - First LSU: sequencer key is exported to `sequencer1-signing.keys`
        - Second LSU: sequencer key is also exported to `sequencer1-signing.keys` (instead of `sequencer2-signing.keys`)
         */

        val keyName = publicKey.name.value.toProtoPrimitive

        // Format of key name: sequencer1-sequencer-auth, sequencer1-signing
        // Drop the initial component and use the node name
        val keyPurpose = keyName.split("-").drop(1).mkString("-")
        val filename = s"${node.name}-$keyPurpose.keys"

        node.keys.secret.download_to(
          publicKey.id,
          outputFile = s"$exportDirectory/$filename.keys",
        )
      }
    }

    def writeAuthorizeStoreToFile(node: InstanceReference): Unit = {
      val byteString = node.topology.transactions
        .export_topology_snapshotV2(
          filterMappings = Seq(NamespaceDelegation.code, OwnerToKeyMapping.code),
          filterNamespace = node.id.uid.namespace.filterString,
        )
      BinaryFileUtil.writeByteStringToFile(
        s"${exportDirectory / node.name}-authorized-store",
        byteString,
      )
    }

    def writeSequencerGenesisState(sequencer: SequencerReference): Unit = {
      val genesisState = sequencer.topology.transactions.logical_upgrade_state()
      BinaryFileUtil.writeByteStringToFile(
        s"${exportDirectory / sequencer.name}-genesis-state",
        genesisState,
      )
    }

    exportDirectory
  }

  /** This method is used to migrate a node from the old version to the current version.
    *
    *   - Import all keys
    *   - Import transactions to the authorized store
    *   - Initialize the node
    *   - Initialize the sequencer
    *   - Initialize the mediator
    *
    * Note that the IDs of the old nodes are preserved.
    */
  def migrateNode(
      migratedNode: InstanceReference,
      newStaticSynchronizerParameters: StaticSynchronizerParameters,
      synchronizerId: SynchronizerId,
      newSequencers: Seq[SequencerReference],
      sequencerTrustThreshold: PositiveInt = PositiveInt.one,
      sequencerLivenessMargin: NonNegativeInt = NonNegativeInt.zero,
      exportDirectory: File,
      newNodeToOldNodeName: Map[String, String],
  )(implicit consoleEnvironment: ConsoleEnvironment): Unit =
    migratedNode match {
      case newSequencer: SequencerReference =>
        migrateSequencer(
          newSequencer,
          newStaticSynchronizerParameters,
          exportDirectory,
          newNodeToOldNodeName.get(migratedNode.name).value,
        )

      case newMediator: MediatorReference =>
        migrateMediator(
          newMediator,
          PhysicalSynchronizerId(synchronizerId, newStaticSynchronizerParameters.toInternal),
          newSequencers,
          exportDirectory,
          newNodeToOldNodeName.get(migratedNode.name).value,
          sequencerTrustThreshold,
          sequencerLivenessMargin,
        )

      case _ =>
        throw new IllegalStateException(s"Unsupported of $migratedNode")
    }

  /** This method is used to migrate a sequencer from the old version to the current version.
    *
    *   - Import all keys
    *   - Import transactions to the authorized store
    *   - Initialize the node
    *   - Initialize the sequencer
    *
    * Note that the IDs of the old nodes are preserved.
    */
  def migrateSequencer(
      migratedSequencer: SequencerReference,
      newStaticSynchronizerParameters: StaticSynchronizerParameters,
      exportDirectory: File,
      oldNodeName: String,
  ): Unit = {
    migrateNodeGeneric(migratedSequencer, exportDirectory, oldNodeName)

    val files = UpgradeDataFiles.from(oldNodeName, exportDirectory)
    initializeSequencer(
      migratedSequencer,
      files.genesisState,
      newStaticSynchronizerParameters,
    )
  }

  /** This method is used to migrate a mediator from the old version to the current version.
    *
    *   - Import all keys
    *   - Import transactions to the authorized store
    *   - Initialize the node
    *   - Initialize the mediator
    *
    * Note that the IDs of the old nodes are preserved.
    */
  def migrateMediator(
      migratedMediator: MediatorReference,
      newPSId: PhysicalSynchronizerId,
      newSequencers: Seq[SequencerReference],
      exportDirectory: File,
      oldNodeName: String,
      sequencerTrustThreshold: PositiveInt = PositiveInt.one,
      sequencerLivenessMargin: NonNegativeInt = NonNegativeInt.zero,
  )(implicit consoleEnvironment: ConsoleEnvironment): Unit = {
    migrateNodeGeneric(migratedMediator, exportDirectory, oldNodeName)

    migratedMediator.setup.assign(
      newPSId,
      SequencerConnections.tryMany(
        newSequencers
          .map(s => s.sequencerConnection.withAlias(SequencerAlias.tryCreate(s.name))),
        sequencerTrustThreshold,
        sequencerLivenessMargin,
        SubmissionRequestAmplification.NoAmplification,
        SequencerConnectionPoolDelays.default,
      ),
    )
  }

  /** This method is used to migrate a node from the old version to the current version.
    *
    *   - Import all keys
    *   - Import transactions to the authorized store
    *
    * Note that the ID of the node is preserved
    */
  private def migrateNodeGeneric(
      migratedNode: InstanceReference,
      exportDirectory: File,
      oldNodeName: String,
  ): Unit = {
    val files = UpgradeDataFiles.from(oldNodeName, exportDirectory)

    files.keys.foreach { case (keys, name) =>
      migratedNode.keys.secret.upload(keys, name)
    }
    migratedNode.topology.init_id_from_uid(files.uid)
    migratedNode.health.wait_for_ready_for_node_topology()
    migratedNode.topology.transactions
      .import_topology_snapshotV2(files.authorizedStore, TopologyStoreId.Authorized)
  }

  def waitForTargetTimeOnSequencer(
      sequencer: LocalSequencerReference,
      targetTime: CantonTimestamp,
  ): Assertion =
    BaseTest.eventually() {
      // send time proofs until we see a successful deliver with
      // a sequencing time greater than or equal to the target time.
      val sendCallback = SendCallback.future
      sequencer.underlying.value.sequencer.client
        .send(Batch(Nil, BaseTest.testedProtocolVersion), callback = sendCallback)(
          TraceContext.empty,
          MetricsContext.Empty,
        )
        .futureValueUS shouldBe Right(())

      sendCallback.future.futureValueUS should matchPattern {
        case SendResult.Success(d: Deliver[?]) if d.timestamp >= targetTime =>
      }
    }

  private def initializeSequencer(
      migrated: SequencerReference,
      genesisState: ByteString,
      staticSynchronizerParameters: StaticSynchronizerParameters,
  ): Unit = {
    migrated.health.wait_for_ready_for_initialization()
    migrated.setup.initialize_from_synchronizer_predecessor(
      genesisState,
      staticSynchronizerParameters,
    )
  }
}

object LogicalUpgradeUtils {
  final case class SynchronizerNodes(
      sequencers: Seq[LocalSequencerReference],
      mediators: Seq[LocalMediatorReference],
  ) {
    def all: Seq[LocalInstanceReference] = (sequencers: Seq[LocalInstanceReference]) ++ mediators
  }

  final case class UpgradeDataFiles(
      uidFile: File,
      keyFiles: Seq[File],
      authorizedStoreFile: File,
      genesisStateFile: File,
  ) {
    def uid: UniqueIdentifier =
      UniqueIdentifier.tryFromProtoPrimitive(uidFile.contentAsString)

    def keys: Seq[(ByteString, Option[String])] =
      keyFiles.map { file =>
        val key = BinaryFileUtil.tryReadByteStringFromFile(file.canonicalPath)
        val name = file.name.stripSuffix(".keys")
        key -> Option(name)
      }

    def authorizedStore: ByteString =
      BinaryFileUtil.tryReadByteStringFromFile(authorizedStoreFile.canonicalPath)

    def genesisState: ByteString =
      BinaryFileUtil.tryReadByteStringFromFile(genesisStateFile.canonicalPath)
  }

  object UpgradeDataFiles {
    def from(nodeName: String, baseDirectory: File): UpgradeDataFiles = {
      val keys =
        baseDirectory.list
          .filter(file => file.name.startsWith(nodeName) && file.name.endsWith(".keys"))
          .toList
      UpgradeDataFiles(
        uidFile = baseDirectory / s"$nodeName-uid",
        keyFiles = keys,
        authorizedStoreFile = baseDirectory / s"$nodeName-authorized-store",
        genesisStateFile = baseDirectory / s"$nodeName-genesis-state",
      )
    }
  }
}
