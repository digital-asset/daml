// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade

import better.files.File
import com.daml.ledger.api.v2.CommandsOuterClass
import com.daml.ledger.javaapi as javab
import com.daml.ledger.javaapi.data.DisclosedContract
import com.digitalasset.canton.admin.api.client.data.{
  SequencerConnectionPoolDelays,
  SequencerConnections,
  StaticSynchronizerParameters,
  SubmissionRequestAmplification,
}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.commands.ConsoleCommandGroup
import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  InstanceReference,
  MediatorReference,
  ParticipantReference,
  SequencerReference,
}
import com.digitalasset.canton.crypto.{CryptoKeyPair, PrivateKey}
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseExternalProcess
import com.digitalasset.canton.integration.plugins.UseExternalProcess.RunVersion
import com.digitalasset.canton.integration.tests.manual.S3Synchronization.ContinuityDumpRef
import com.digitalasset.canton.integration.tests.manual.{DataContinuityTest, S3Synchronization}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.{
  NamespaceDelegation,
  OwnerToKeyMapping,
  VettedPackages,
}
import com.digitalasset.canton.topology.{
  ExternalParty,
  PartyId,
  PhysicalSynchronizerId,
  SynchronizerId,
  UniqueIdentifier,
}
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.version.ReleaseVersion
import com.digitalasset.canton.{BaseTest, SequencerAlias, SynchronizerAlias}
import com.google.protobuf.ByteString
import org.scalactic.source
import org.slf4j.event.Level

import scala.sys.process.*
import scala.util.Try

trait MajorUpgradeUtils extends S3Synchronization { self: BaseTest =>
  import MajorUpgradeUtils.*

  protected def testName: String

  /** If defined, force a specific dump to be used. E.g., it can be
    * `Option(ContinuityDumpLocalRef("/tmp/canton/data-continuity-dumps/3.3.0-SNAPSHOT/pv=33"))` to
    * use a local dump.
    */
  private val forceContinuityDumpRef: Option[ContinuityDumpRef] = None

  // Directory used by the test to write data
  protected lazy val baseExportDirectory: File =
    File.newTemporaryDirectory(testName)

  protected def createDirectory(): Unit = {
    Seq(baseExportDirectory).filter(_.exists).map(_.delete(swallowIOExceptions = true))
    baseExportDirectory.createDirectoryIfNotExists()
  }

  /** Returns the last dump of the previous minor version
    */
  protected def getPreviousDump(): S3Synchronization.ContinuityDumpRef = {
    val (major, minor) = ReleaseVersion.current.majorMinor

    forceContinuityDumpRef.getOrElse {
      val (ref, _) = S3Dump
        .getDumpDirectories(majorUpgradeTestFrom = Some((major, minor - 1)))
        .loneElement

      ref
    }
  }

  // Copy files from export directory to /tmp/
  protected def copyFiles(): Unit = {
    val targetDirectory = DataContinuityTest.baseDumpForVersion(
      testedProtocolVersion
    ) / testName

    DataContinuityTest.synchronizedOperation({
      Try(s"mkdir -p ${targetDirectory.directory}".!).discard

      if (!targetDirectory.directory.exists) {
        logger.warn(s"""
                       |Unable to create directory ${targetDirectory.directory}
                       |Files need to be copied manually from $baseExportDirectory to ${targetDirectory.directory}
                       |""".stripMargin)
      } else {
        baseExportDirectory.list.foreach(_.copyToDirectory(targetDirectory.directory))
      }
    })
  }

  protected def exportDisclosure(
      disclosureName: String,
      disclosure: CommandsOuterClass.DisclosedContract,
  ): Unit =
    BinaryFileUtil.writeByteStringToFile(
      s"${baseExportDirectory / disclosureName}-disclosure.binpb",
      disclosure.toByteString,
    )

  protected def exportExternalPartyKeys(
      party: ExternalParty
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    val fingerprints = party.partyId.uid.namespace.fingerprint +: party.signingFingerprints

    fingerprints.foreach { fingerprint =>
      val privateKey = CryptoKeyPair
        .fromTrustedByteString(global_secret.keys.secret.download(fingerprint))
        .value
        .privateKey

      BinaryFileUtil.writeByteStringToFile(
        s"${baseExportDirectory / s"${party.identifier}_private_key"}-${privateKey.id.toProtoPrimitive}.key",
        privateKey.toProtoPrivateKey.toByteString,
      )
    }
  }

  protected def readExternalPartyPrivateKeys(
      party: PartyId,
      exportDirectory: File,
  ): Seq[PrivateKey] = {
    val globed = exportDirectory.glob(s"${party.identifier}_private_key-*.key").toList
    globed.map(f =>
      f.inputStream()(is =>
        PrivateKey
          .fromProtoPrivateKey(com.digitalasset.canton.crypto.v30.PrivateKey.parseFrom(is))
          .value
      )
    )
  }

  protected def exportNodesData(
      nodesOverride: Option[CantonNodes] = None
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    logger.info(s"Starting to export nodes data for upgrade into directory $baseExportDirectory")

    val sequencers = nodesOverride.map(_.sequencers).getOrElse(env.sequencers.all)
    val participants = nodesOverride.map(_.participants).getOrElse(env.participants.all)
    val nodes = nodesOverride.map(_.all).getOrElse(env.nodes.all)

    logger.debug("Stop traffic on Canton by setting confirmationRequestsMaxRate to 0")
    sequencers.headOption.value.topology.synchronizer_parameters.propose_update(
      daId,
      _.update(
        confirmationRequestsMaxRate = NonNegativeInt.zero
      ),
    )

    eventually() {
      participants.foreach { par =>
        val confirmationRequestsMaxRate = par.topology.synchronizer_parameters
          .latest(store = daId)
          .participantSynchronizerLimits
          .confirmationRequestsMaxRate
          .unwrap
        confirmationRequestsMaxRate shouldBe 0
      }
    }

    writeSynchronizerIdToFile()

    nodes.foreach(writeUidToFile)
    nodes.foreach(writeKeysToFile)
    nodes.foreach(writeAuthorizeStoreToFile)
    sequencers.foreach(writeSequencerGenesisState)
    participants.foreach(writeParticipantAcs(_, daId))

    // Helpers
    def writeSynchronizerIdToFile(): Unit = BinaryFileUtil.writeByteStringToFile(
      (baseExportDirectory / "synchronizer-id").canonicalPath,
      ByteString.copyFromUtf8(daId.logical.toProtoPrimitive),
    )

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
          filterMappings =
            Seq(NamespaceDelegation.code, OwnerToKeyMapping.code, VettedPackages.code),
          filterNamespace = node.id.uid.namespace.filterString,
        )
      BinaryFileUtil.writeByteStringToFile(
        s"${baseExportDirectory / node.name}-authorized-store",
        byteString,
      )
    }

    def writeSequencerGenesisState(sequencer: SequencerReference): Unit = {
      val genesisState = sequencer.topology.transactions.genesis_stateV2()
      BinaryFileUtil.writeByteStringToFile(
        s"${baseExportDirectory / sequencer.name}-genesis-state",
        genesisState,
      )
    }

    def writeParticipantAcs(
        participant: ParticipantReference,
        synchronizerId: SynchronizerId,
    ): Unit = {
      val silentSynchronizerValidFrom = participant.topology.synchronizer_parameters
        .list(store = daId)
        .filter { change =>
          change.item.participantSynchronizerLimits.confirmationRequestsMaxRate == NonNegativeInt.zero
        }
        .map(change => change.context.validFrom)
        .loneElement

      val ledgerOffset =
        participant.parties.find_highest_offset_by_timestamp(
          synchronizerId,
          silentSynchronizerValidFrom,
          force = true,
        )

      ledgerOffset should be > 0L

      val parties = participant.parties.list().map(_.party).toSet
      participant.repair.export_acs(
        parties,
        ledgerOffset = ledgerOffset,
        synchronizerId = Some(synchronizerId),
        exportFilePath = s"${baseExportDirectory / participant.name}-acs-snapshot",
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
      dars: Seq[String],
      sequencerTrustThreshold: PositiveInt = PositiveInt.one,
      sequencerLivenessMargin: NonNegativeInt = NonNegativeInt.zero,
      exportDirectory: File,
      sourceNodeNames: Map[String, String] = Map.empty,
  )(implicit consoleEnvironment: ConsoleEnvironment): Unit = {
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

      case newParticipant: ParticipantReference =>
        val node = newParticipant
        // user-manual-entry-begin: WaitForParticipantInitialization
        node.health.wait_for_initialized()
        // user-manual-entry-end: WaitForParticipantInitialization
        newParticipant.dars.upload_many(dars, vetAllPackages = false)

      case _ =>
        throw new IllegalStateException(
          s"Unsupported migration from $files to $migratedNode"
        )
    }
  }

  protected def importDisclosures(
      disclosureNamePrefix: String,
      exportDirectory: File,
  ): Seq[DisclosedContract] =
    exportDirectory
      .glob(s"$disclosureNamePrefix*")
      .toList
      .map(_.canonicalPath)
      .map { path =>
        CommandsOuterClass.DisclosedContract.parseFrom(
          BinaryFileUtil.tryReadByteStringFromFile(path)
        )
      }
      .map(javab.data.DisclosedContract.fromProto)

  protected def importAcs(
      migratedNode: ParticipantReference,
      migratedSequencers: Seq[SequencerReference],
      synchronizerAlias: SynchronizerAlias,
      exportDirectory: File,
  ): Unit = {
    val oldNode = UpgradeDataFiles.from(migratedNode.name, exportDirectory)

    migratedSequencers.foreach { seq =>
      migratedNode.synchronizers.register(seq, synchronizerAlias)
    }

    migratedNode.repair.import_acs(oldNode.acsSnapshotFile.canonicalPath)
    migratedSequencers.foreach { seq =>
      migratedNode.synchronizers.connect(seq, synchronizerAlias)
    }
  }

  // TODO(#28588): Allow participants to ignore Dar Self-Consistency log warnings on dar file uploads
  protected def suppressDarSelfConsistencyWarning[A](within: => A)(implicit
      pos: source.Position
  ): A =
    loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.WARN))(
      within,
      LogEntry.assertLogSeq(
        Seq.empty,
        Seq(
          _.warningMessage should (include regex "For package .*, the set of package dependencies is not self consistent, the extra dependencies are ")
        ),
      ),
    )

  private def initializeSequencer(
      migrated: SequencerReference,
      genesisState: ByteString,
      staticSynchronizerParameters: StaticSynchronizerParameters,
  ): Unit = {
    migrated.health.wait_for_ready_for_initialization()
    migrated.setup.assign_from_genesis_stateV2(
      genesisState,
      staticSynchronizerParameters,
    )

    migrated.health.initialized()
  }
}

private[upgrade] object MajorUpgradeUtils {
  final case class CantonNodes(
      participants: Seq[ParticipantReference],
      sequencers: Seq[SequencerReference],
      mediators: Seq[MediatorReference],
  ) {
    def all: Seq[InstanceReference & ConsoleCommandGroup] =
      sequencers ++ mediators ++ participants
  }

  final case class UpgradeDataFiles(
      uidFile: File,
      keyFiles: Seq[File],
      authorizedStoreFile: File,
      acsSnapshotFile: File,
      genesisStateFile: File,
  ) {
    def uid: UniqueIdentifier =
      UniqueIdentifier.tryFromProtoPrimitive(
        uidFile.contentAsString
      )

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
        acsSnapshotFile = baseDirectory / s"$nodeName-acs-snapshot",
        genesisStateFile = baseDirectory / s"$nodeName-genesis-state",
      )
    }
  }

  private[upgrade] def startNodes(
      externalCanton: UseExternalProcess,
      cantonNodes: CantonNodes,
      release: RunVersion,
  ): Unit =
    cantonNodes.all.foreach(node => externalCanton.start(node.name, release))

  private[upgrade] def stopNodes(
      externalCanton: UseExternalProcess,
      cantonNodes: CantonNodes,
  ): Unit =
    cantonNodes.all.foreach(node => externalCanton.kill(node.name))
}
