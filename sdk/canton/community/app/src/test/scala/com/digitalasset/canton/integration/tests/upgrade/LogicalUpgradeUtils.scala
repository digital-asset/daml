// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade

import better.files.File
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{
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
import com.digitalasset.canton.sequencing.client.{SendCallback, SendResult}
import com.digitalasset.canton.sequencing.protocol.{Batch, Deliver}
import com.digitalasset.canton.sequencing.{
  SequencerConnectionPoolDelays,
  SequencerConnections,
  SubmissionRequestAmplification,
}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.{NamespaceDelegation, OwnerToKeyMapping}
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.{BaseTest, SequencerAlias}
import com.google.protobuf.ByteString
import org.scalatest.Assertion

trait LogicalUpgradeUtils { self: BaseTest =>
  protected def testName: String

  // Directory used by the test to write data
  protected lazy val baseExportDirectory: File =
    File.newTemporaryDirectory(testName)

  protected def createDirectory(): Unit = {
    Seq(baseExportDirectory).filter(_.exists).map(_.delete(swallowIOExceptions = true))
    baseExportDirectory.createDirectoryIfNotExists()
  }

  protected def exportNodesData(
      nodes: SynchronizerNodes
  ): Unit = {

    logger.info(s"Starting to export nodes data for upgrade into directory $baseExportDirectory")

    val sequencers = nodes.sequencers

    nodes.all.foreach(writeUidToFile)
    nodes.all.foreach(writeKeysToFile)
    nodes.all.foreach(writeAuthorizeStoreToFile)
    sequencers.foreach(writeSequencerGenesisState)

    // Helpers
    def writeUidToFile(node: InstanceReference): Unit =
      BinaryFileUtil.writeByteStringToFile(
        s"${baseExportDirectory / node.name}-uid",
        ByteString.copyFromUtf8(node.id.uid.toProtoPrimitive),
      )

    def writeKeysToFile(
        node: InstanceReference
    ): Unit = {
      val publicKeysWithNames = node.keys.public.list()
      publicKeysWithNames.foreach { pb =>
        node.keys.secret.download_to(
          pb.id,
          outputFile =
            s"$baseExportDirectory/${pb.name.map(_.toProtoPrimitive).getOrElse("key")}.keys",
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
        s"${baseExportDirectory / node.name}-authorized-store",
        byteString,
      )
    }

    def writeSequencerGenesisState(sequencer: SequencerReference): Unit = {
      val genesisState = sequencer.topology.transactions.logical_upgrade_state()
      BinaryFileUtil.writeByteStringToFile(
        s"${baseExportDirectory / sequencer.name}-genesis-state",
        genesisState,
      )
    }

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
      sourceNodeNames: Map[String, String] = Map.empty,
  ): Unit = {
    val files = UpgradeDataFiles.from(
      sourceNodeNames.getOrElse(migratedNode.name, migratedNode.name),
      exportDirectory,
    )

    files.keys.foreach { case (keys, name) =>
      migratedNode.keys.secret.upload(keys, name)
    }
    migratedNode.topology.init_id_from_uid(files.uid)
    migratedNode.health.wait_for_ready_for_node_topology()
    migratedNode.topology.transactions
      .import_topology_snapshotV2(files.authorizedStore, TopologyStoreId.Authorized)

    migratedNode match {
      case newSequencer: SequencerReference =>
        initializeSequencer(
          newSequencer,
          files.genesisState,
          newStaticSynchronizerParameters,
        )

      case newMediator: MediatorReference =>
        newMediator.setup.assign(
          PhysicalSynchronizerId(synchronizerId, newStaticSynchronizerParameters.toInternal),
          SequencerConnections.tryMany(
            newSequencers
              .map(s => s.sequencerConnection.withAlias(SequencerAlias.tryCreate(s.name))),
            sequencerTrustThreshold,
            sequencerLivenessMargin,
            SubmissionRequestAmplification.NoAmplification,
            SequencerConnectionPoolDelays.default,
          ),
        )

      case _ =>
        throw new IllegalStateException(
          s"Unsupported migration from $files to $migratedNode"
        )
    }
  }

  def waitForTargetTimeOnSequencer(
      sequencer: LocalSequencerReference,
      targetTime: CantonTimestamp,
  ): Assertion =
    eventually() {
      // send time proofs until we see a successful deliver with
      // a sequencing time greater than or equal to the target time.
      val sendCallback = SendCallback.future
      sequencer.underlying.value.sequencer.client
        .send(Batch(Nil, testedProtocolVersion), callback = sendCallback)(
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

private[upgrade] object LogicalUpgradeUtils {
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
