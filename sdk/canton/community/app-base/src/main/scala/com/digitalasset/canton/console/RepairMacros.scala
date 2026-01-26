// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import better.files.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import com.digitalasset.canton.admin.api.client.data.{
  SequencerConnections,
  StaticSynchronizerParameters,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.data.ActiveContract
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
  TimeQuery,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, TextFileUtil}
import com.digitalasset.canton.version.ProtocolVersion

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

class RepairMacros(override val loggerFactory: NamedLoggerFactory)
    extends NamedLogging
    with Helpful {

  private def createAndCheckTargetDirectory(targetDir: File)(implicit
      traceContext: TraceContext
  ): File = {
    targetDir.createIfNotExists(asDirectory = true)
    ErrorUtil.requireArgument(targetDir.isDirectory, s"$targetDir exists, but is not a directory")
    targetDir
  }

  @Help.Summary("Commands used to repair / recover the identity of a node", FeatureFlag.Repair)
  @Help.Group("Identity")
  object identity extends Helpful {

    /** File name pattern for storing the node's secret keys to be downloaded or uploaded */
    private val SECRET_PREFIX = "secret"

    /** File name for storing the node's unique identifier */
    private val IDENTITY = "nodeid.txt"

    /** File name for storing the topology transactions from the authorized store */
    private val TOPOLOGY_AUTHORIZED = "topology-authorized.bin"

    /** File name for storing the genesis topology transactions for the synchronizer */
    private val TOPOLOGY_SYNCHRONIZER_GENESIS = "topology-synchronizer-genesis.bin"

    @Help.Summary(
      "Download the identity, keys and topology transactions of the node into the directory targetPath"
    )
    @Help.Description(
      "The downloaded files can be uploaded to an uninitialized node using the ``upload`` macro."
    )
    // TODO(#16009) improve repair UX: consider creating node-specific upload methods
    def download(
        node: LocalInstanceReference,
        synchronizerId: SynchronizerId,
        protocolVersion: ProtocolVersion,
        targetPath: String,
    ): Unit =
      TraceContext.withNewTraceContext("download_identity") { implicit traceContext =>
        val targetDir = createAndCheckTargetDirectory(File(targetPath))
        logger.info(s"Downloading identity from node ${node.name} to $targetDir")
        if (!node.is_running) {
          node.start()
        }
        ErrorUtil.requireState(
          node.is_initialized,
          s"Node is not initialized. Therefore, I can not download anything",
        )
        // store keys onto local drive
        // TODO(#25974): proper KMS handling
        val keys = node.keys.secret.list()
        val keysCount = keys.size
        keys.zipWithIndex.foreach { case (keyEntry, idx) =>
          val basename = s"$SECRET_PREFIX$idx"
          logger.info(s"Storing secret key #$idx/$keysCount in $targetDir")
          node.keys.secret.download_to(
            keyEntry.publicKey.fingerprint,
            File(targetDir, s"$basename.bin").pathAsString,
          )
          val key = node.keys.secret
            .list(filterFingerprint = keyEntry.publicKey.fingerprint.unwrap)
            .headOption
            .getOrElse(
              throw new IllegalStateException(
                s"No public key entry for secret key ${keyEntry.publicKey.fingerprint}"
              )
            )
          key.name.foreach { keyName =>
            File(targetDir, s"$basename.name").overwrite(keyName.unwrap)
          }
        }
        // store the id of the node into a file
        val idStr = node.uid.toProtoPrimitive
        val identityFile = File(targetDir, IDENTITY)
        logger.info(s"Storing identity into $identityFile")
        identityFile.overwrite(idStr)

        // Own identity only, the rest will be loaded from the reinitialized sequencer
        val transactionsFromAuthorizedStore =
          node.topology.transactions.identity_transactions().map { tx =>
            StoredTopologyTransaction(
              sequenced = SequencedTime.MinValue,
              validFrom = EffectiveTime.MinValue,
              validUntil = None,
              transaction = tx,
              None,
            )
          }

        val authorizedFile = File(targetDir, TOPOLOGY_AUTHORIZED)
        StoredTopologyTransactions(transactionsFromAuthorizedStore).writeToFile(
          authorizedFile.pathAsString,
          protocolVersion,
        )

        if (node.id.member.code == SequencerId.Code) { // The sequencer needs to know more than just its own identity

          // Initial synchronizer state, needed for the sequencer to open the init service offering `assign_from_genesis_state`
          val synchronizerGenesisTransactions = node.topology.transactions
            .list(
              store = synchronizerId,
              timeQuery = TimeQuery.Snapshot(
                SignedTopologyTransaction.InitialTopologySequencingTime.immediateSuccessor // Convention used only by internal Canton tooling
              ),
            )
            .result

          val synchronizerFile = File(targetDir, TOPOLOGY_SYNCHRONIZER_GENESIS)
          logger.info(
            s"Storing ${synchronizerGenesisTransactions.length} identity topology transactions into $synchronizerFile:\n${synchronizerGenesisTransactions
                .map { tx =>
                  (tx.mapping.code, tx.mapping.maybeUid.map(_.show).getOrElse(tx.mapping.namespace.show))
                }
                .mkString("\n")}"
          )
          StoredTopologyTransactions(synchronizerGenesisTransactions).writeToFile(
            synchronizerFile.pathAsString,
            protocolVersion,
          )
        }
      }

    private def loadStoredTopologyTransactions(
        node: LocalInstanceReference,
        txs: Seq[StoredTopologyTransaction[TopologyChangeOp, TopologyMapping]],
        store: TopologyStoreId,
        description: String,
    )(implicit traceContext: TraceContext): Unit = {
      node.health.wait_for_ready_for_node_topology()
      // Topology transactions are needed to advance bootstrap as far as possible after initializing the node ID
      logger.info(
        s"Uploading ${txs.length} topology txs ($description) to the node ${node.name}"
      )
      txs.foreach(x =>
        node.topology.transactions.load(Seq(x.transaction), store, ForceFlag.AlienMember)
      )
    }

    private def initId(node: LocalInstanceReference, sourceDir: File)(implicit
        traceContext: TraceContext
    ): Unit = {
      val identityFile = File(sourceDir, IDENTITY)
      logger.info(s"Reading unique identifier from $identityFile")
      val idStr = TextFileUtil.tryReadStringFromFile(identityFile.toJava)
      val nodeId = UniqueIdentifier.tryFromProtoPrimitive(idStr)
      logger.info(s"Initializing node ${node.name} with uid ${nodeId.toString}")
      node.topology.init_id_from_uid(nodeId)
    }

    @tailrec
    private def uploadKeys(node: LocalInstanceReference, sourceDir: File, idx: Int = 0)(implicit
        traceContext: TraceContext
    ): Int = {
      val basename = s"$SECRET_PREFIX$idx"
      val keyFile = File(sourceDir, basename + ".bin")
      val nameFile = File(sourceDir, basename + ".name")
      if (keyFile.exists) {
        val keyName = Option.when(nameFile.exists) {
          TextFileUtil.tryReadStringFromFile(nameFile.toJava)
        }
        logger.info(s"Upload secret key $idx $keyName from $keyFile")
        node.keys.secret.upload_from(keyFile.pathAsString, keyName)
        uploadKeys(node, sourceDir, idx + 1)
      } else idx
    }

    @Help.Summary(
      "Upload the identity, keys and initial topology transactions from the directory sourcePath to the node"
    )
    @Help.Description(
      """The node must not have been initialized. Afterwards, the node will impersonate the one
        |whose credentials were downloaded into the directory.
        """
    )
    // TODO(#16009) improve repair UX: consider creating node-specific upload methods
    def upload(
        node: LocalInstanceReference,
        sourcePath: String,
        synchronizerId: SynchronizerId,
        staticSynchronizerParameters: StaticSynchronizerParameters,
        sequencerConnections: SequencerConnections,
    ): Unit =
      TraceContext.withNewTraceContext("upload_identity") { implicit traceContext =>
        val sourceDir = File(sourcePath)

        ErrorUtil.requireArgument(sourceDir.exists, s"Directory $sourceDir does not exist")
        ErrorUtil.requireArgument(sourceDir.isDirectory, s"Path $sourceDir must be a directory")
        logger.info(s"Uploading identity of node ${node.name} from $sourceDir")

        if (!node.is_running) {
          node.start()
        }

        ErrorUtil.requireState(
          !node.is_initialized,
          s"Can not upload identity data to an already initialised node ${node.name}",
        )

        // TODO(#25974): proper KMS handling
        val num = uploadKeys(node, sourceDir)
        logger.info(s"Uploaded ${num + 1} secret keys to node ${node.name}")
        initId(node, sourceDir)

        val authorizedStoreFile = File(sourceDir, TOPOLOGY_AUTHORIZED).pathAsString
        logger.info(s"Reading authorized store topology from $authorizedStoreFile")
        val authorizedStoreTopologyTxs =
          StoredTopologyTransactions.tryReadFromTrustedFile(
            authorizedStoreFile
          )
        logger.info(
          s"Uploading initial topology transactions to the node ${node.name}"
        )

        loadStoredTopologyTransactions(
          node,
          authorizedStoreTopologyTxs.result,
          store = TopologyStoreId.Authorized,
          description = "initial",
        )

        node match {

          case sequencer: SequencerReference =>
            val synchronizerGenesisFile =
              File(sourceDir, TOPOLOGY_SYNCHRONIZER_GENESIS).pathAsString
            logger.info(s"Reading synchronizer genesis topology from $synchronizerGenesisFile")

            val synchronizerGenesisTransactions =
              StoredTopologyTransactions.tryReadFromTrustedFile(
                synchronizerGenesisFile
              )

            sequencer.setup
              .assign_from_genesis_state(
                synchronizerGenesisTransactions
                  .collectOfType[TopologyChangeOp.Replace]
                  .toByteString(staticSynchronizerParameters.protocolVersion),
                staticSynchronizerParameters,
              )
              .discard

          case mediator: MediatorReference =>
            mediator.setup
              .assign(
                PhysicalSynchronizerId(synchronizerId, staticSynchronizerParameters.toInternal),
                sequencerConnections,
              )

          case _: LocalParticipantReference =>
            () // nothing more to do for a participant

          case other =>
            sys.error(s"Unexpected node type $other")
        }
        node.health.wait_for_initialized()
      }
  }

  @Help.Summary("Commands used to repair / recover the dars of a node", FeatureFlag.Repair)
  @Help.Group("Dars")
  object dars extends Helpful {

    private val DARS = "dars"

    def download(
        node: LocalParticipantReference,
        targetPath: String,
    ): Unit =
      TraceContext.withNewTraceContext("download_dars") { implicit traceContext =>
        val darsDir = createAndCheckTargetDirectory(File(targetPath, DARS))
        node.dars.list().filterNot(_.name.startsWith("AdminWorkflow")).foreach { dar =>
          noTracingLogger.info(s"Downloading dar ${dar.name}")
          node.dars.download(dar.mainPackageId, darsDir.pathAsString)
        }
      }

    def upload(
        node: LocalParticipantReference,
        sourcePath: String,
    ): Unit =
      TraceContext.withNewTraceContext("upload_dars") { implicit traceContext =>
        ErrorUtil.requireState(node.is_running, s"Node ${node.name} is not running")
        val darsDir = File(sourcePath, DARS)
        val files = darsDir.list
        files.filter(_.name.endsWith(".dar")).foreach { file =>
          logger.info(s"Uploading DAR file $file")
          node.dars
            .upload(
              file.pathAsString,
              // Do not vet, because the DAR may not have been vetted on the
              // previous participant, and because vetting all recovered DARs
              // may not be possible.
              vetAllPackages = false,
              synchronizeVetting = false,
            )
            .discard[String]
        }
      }
  }

  @Help.Summary("Commands useful to repair the contract stores", FeatureFlag.Repair)
  @Help.Group("Active Contract Store")
  object acs extends Helpful {
    @Help.Summary("Read contracts from a file")
    @Help.Description(
      "Expects a file name. Returns a streaming iterator of serializable contracts."
    )
    def read_from_file(source: String)(implicit
        consoleEnvironment: ConsoleEnvironment
    ): Seq[com.daml.ledger.api.v2.state_service.ActiveContract] =
      ActiveContract
        .fromFile(File(source))
        .map(_.map(_.contract))
        .valueOr(err =>
          consoleEnvironment.raiseError(s"Unable to read contracts from $source: $err")
        )
        .toSeq

    @Help.Summary("Write active contracts to a file")
    @Help.Description(
      """The file can be imported using command `import_acs`.
        |
        |The arguments are:
        |- contracts: Contracts to be written
        |- protocolVersion: Protocol version of the synchronizer of the contracts
        |"""
    )
    def write_contracts_to_file(
        contracts: Seq[com.daml.ledger.api.v2.state_service.ActiveContract],
        protocolVersion: ProtocolVersion,
        exportFilePath: String = "canton-acs-export.gz",
    )(implicit consoleEnvironment: ConsoleEnvironment): Unit = {
      val output = File(exportFilePath).newGzipOutputStream()
      val res = contracts.traverse_ { lapiContract =>
        val contract = com.digitalasset.canton.participant.admin.data.ActiveContract
          .create(lapiContract)(protocolVersion)

        Try(contract.writeDelimitedTo(output))
      }
      output.close()

      res match {
        case Failure(exception) =>
          consoleEnvironment.raiseError(
            s"Unable to write contracts to file $exportFilePath: ${exception.getMessage}"
          )
        case Success(()) =>
      }
    }
  }

}
