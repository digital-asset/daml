// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import better.files.*
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.console.ConsoleEnvironment.Implicits.*
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.data.ActiveContractOld
import com.digitalasset.canton.protocol.messages.HasSynchronizerId
import com.digitalasset.canton.protocol.{
  HasSerializableContract,
  LfContractId,
  SerializableContract,
}
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.ForceFlag.DisablePartyWithActiveContracts
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

import scala.annotation.tailrec
import scala.util.control.NonFatal

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
        targetPath: String,
    ): Unit =
      TraceContext.withNewTraceContext { implicit traceContext =>
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
          authorizedFile.pathAsString
        )

        if (node.id.member.code == SequencerId.Code) { // The sequencer needs to know more than just its own identity

          // Initial synchronizer state, needed for the sequencer to open the init service offering `assign_from_genesis_state`
          val synchronizerGenesisTransactions = node.topology.transactions
            .list(
              store = TopologyStoreId.Synchronizer(synchronizerId),
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
            synchronizerFile.pathAsString
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
      node.topology.init_id(nodeId)
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
      "The node must not have been initialized. Afterwards, the node will impersonate the one whose credentials were downloaded into the directory."
    )
    // TODO(#16009) improve repair UX: consider creating node-specific upload methods
    def upload(
        node: LocalInstanceReference,
        sourcePath: String,
        synchronizerId: SynchronizerId,
        staticSynchronizerParameters: StaticSynchronizerParameters,
        sequencerConnections: SequencerConnections,
    ): Unit =
      TraceContext.withNewTraceContext { implicit traceContext =>
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
                synchronizerId,
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
      TraceContext.withNewTraceContext { implicit traceContext =>
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
      TraceContext.withNewTraceContext { implicit traceContext =>
        ErrorUtil.requireState(node.is_running, s"Node ${node.name} is not running")
        val darsDir = File(sourcePath, DARS)
        val files = darsDir.list
        files.filter(_.name.endsWith(".dar")).foreach { file =>
          logger.info(s"Uploading DAR file $file")
          node.dars.upload(file.pathAsString, synchronizeVetting = false).discard[String]
        }
      }
  }

  @Help.Summary("Commands useful to repair the contract stores", FeatureFlag.Repair)
  @Help.Group("Active Contract Store")
  object acs extends Helpful {

    @Help.Summary("Load contracts from a file. (DEPRECATED)")
    @Help.Description(
      """Expects a file name. Returns a streaming iterator of serializable contracts.
        |
        |DEPRECATION NOTICE: A future release removes this command.
        |"""
    )
    def import_acs_from_file_old(
        source: String
    ): Iterator[
      (SynchronizerId, SerializableContract)
    ] = // TODO(#24728) - Remove, use import_acs and then the LAPI
      ActiveContractOld.fromFile(File(source)).map {
        case ActiveContractOld(synchronizerId, contract, _) =>
          synchronizerId -> contract
      }
  }

  /** party migration commands!
    *
    * The following group contains a set of party migration commands. These commands can be used to
    * migrate a party from one node to another. The commands come with some limitations / caveats /
    * capabilities:
    *
    *   - If the party is not managed by the source participant, then the appropriate topology state
    *     needs to be manually prepared before running any of the steps.
    *   - In theory, we don't need to migrate the party off the origin node. This means we can
    *     support parties on multiple participants. However, when the parties are re-enabled, we
    *     need to ensure that they are re-enabled synchronously on all participants. This can
    *     currently not be guaranteed / enforced. Therefore, this operation is quite brittle.
    */
  @Help.Summary(
    "Commands useful to migrate parties from one participant to another",
    FeatureFlag.Repair,
  )
  @Help.Group("Party Migration")
  object party_migration extends Helpful {

    def step1_hold_and_store_acs(
        partyId: PartyId,
        partiesOffboarding: Boolean,
        sourceParticipant: ParticipantReference,
        targetParticipantId: ParticipantId,
        targetFile: String,
        synchronize: Boolean = true,
    )(implicit env: ConsoleEnvironment): Unit = {
      haltParty(sourceParticipant, partyId, synchronize)
      sourceParticipant.repair.export_acs_old(
        Set(partyId),
        partiesOffboarding = partiesOffboarding,
        targetFile,
      )
      val synchronizers =
        acs
          .import_acs_from_file_old(targetFile)
          .map { case (synchronizerId, _) => synchronizerId }
          .toSet
      ensureTargetPartyToParticipantIsPermissioned(
        partyId,
        sourceParticipant,
        targetParticipantId,
        synchronizers,
      )
      if (synchronize) {
        synchronizers.foreach(synchronizerId =>
          // wait to observe either the fully authorized party mapping or the proposal.
          // normally it would be a proposal, but it could be a fully authorized transaction, if
          // the target participant signed a NamespaceDelegation or IdentifierDelegation for a
          // key available on the source participant
          ConsoleMacros.utils.retry_until_true(env.commandTimeouts.bounded)(
            sourceParticipant.topology.party_to_participant_mappings
              .list(
                synchronizerId,
                proposals = true,
                filterParty = partyId.filterString,
                filterParticipant = targetParticipantId.filterString,
              )
              .nonEmpty ||
              sourceParticipant.topology.party_to_participant_mappings
                .list(
                  synchronizerId,
                  filterParty = partyId.filterString,
                  filterParticipant = targetParticipantId.filterString,
                )
                .nonEmpty
          )
        )
      }
    }

    def step2_import_acs(
        partyId: PartyId,
        targetParticipant: ParticipantReference,
        sourceFile: String,
        workflowIdPrefix: String = "",
    )(implicit env: ConsoleEnvironment): Unit = {
      val synchronizers =
        acs
          .import_acs_from_file_old(sourceFile)
          .map { case (synchronizerId, _) => synchronizerId }
          .toSet
      ensureTargetPartyToParticipantIsPermissioned(
        partyId,
        targetParticipant,
        targetParticipant.id,
        synchronizers,
      )
      // this is needed to ensure that we can switch to repair mode (necessary party notification is already arrived)
      ConsoleMacros.utils.retry_until_true(env.commandTimeouts.bounded)(
        condition = targetParticipant.ledger_api.parties
          .list()
          .exists(partyDetails => partyDetails.party == partyId && partyDetails.isLocal),
        s"Cannot find party $partyId on target participant.",
      )
      try {
        noTracingLogger.info(
          "Disconnecting the participant from all synchronizers"
        )
        targetParticipant.synchronizers.disconnect_all()
        noTracingLogger.info(s"Participant disconnected from all synchronizers")
        noTracingLogger.info(s"Importing ACS from $sourceFile")
        targetParticipant.repair.import_acs_old(sourceFile, workflowIdPrefix).discard
        noTracingLogger.info("ACS import finished")
      } finally {
        noTracingLogger.info(
          "Automatically reconnecting the participant to the synchronizers where the migrating contracts are assigned"
        )
        try {
          targetParticipant.synchronizers.reconnect_all().discard
          noTracingLogger.info(s"Participant reconnected to all synchronizers")
        } catch {
          case NonFatal(e) =>
            noTracingLogger.error(
              s"Unable to reconnect automatically to all synchronizers, please retry manually",
              e,
            )
        }
      }
    }

    def step4_clean_up_source(
        partyId: PartyId,
        sourceParticipant: ParticipantReference,
        sourceFile: String,
        batchSize: Int = 1000,
    )(implicit env: ConsoleEnvironment): Unit = {
      import env.*
      consoleLogger.info(s"Purging contracts of $partyId")

      readGrouped(sourceParticipant, sourceFile, batchSize).foreach(_.foreach {
        case (alias, contractIds) =>
          sourceParticipant.repair.purge(alias, contractIds)
      })
    }
  }

  // TODO(i15519) add a test to migrate a multi-hosted party
  private def haltParty(
      sourceParticipant: ParticipantReference,
      partyId: PartyId,
      synchronize: Boolean,
  )(implicit env: ConsoleEnvironment): Unit = {
    // remove any topology transaction that points to source participant
    sourceParticipant.topology.party_to_participant_mappings
      .propose_delta(
        partyId,
        removes = List(sourceParticipant.id),
        forceFlags = ForceFlags(DisablePartyWithActiveContracts),
      )
      .discard
    if (synchronize) {
      // there's a gap between having received the transaction, but it hasn't been fully processed yet,
      // so the node status will return that the node is idle, when that's not really the case.
      // to avoid flakiness for now, we'll retry
      ConsoleMacros.utils.retry_until_true(env.commandTimeouts.bounded)(
        condition =
          sourceParticipant.parties.list(partyId.filterString).flatMap(_.participants).isEmpty,
        // above code will only work if we are managing the party ourselves. otherwise, we need some help
        s"Cannot disable party $partyId completely. Ask your identity manager for help.",
      )
    }
  }

  private def ensureTargetPartyToParticipantIsPermissioned(
      partyId: PartyId,
      participant: ParticipantReference,
      targetParticipantId: ParticipantId,
      synchronizerIds: Set[SynchronizerId],
  ): Unit = {
    noTracingLogger.info(
      s"Participant '${participant.id}' is ensuring that the party '$partyId' is enabled on the target '$targetParticipantId'"
    )
    synchronizerIds.foreach { synchronizerId =>
      // check that target participant is present on all synchronizers
      val active =
        participant.topology.participant_synchronizer_states.active(
          synchronizerId,
          targetParticipantId,
        )
      if (!active) {
        throw new Exception(
          s"Target participant $targetParticipantId is not active on synchronizer $synchronizerId"
        )
      }
    }

    synchronizerIds.foreach { synchronizerId =>
      val mappingExists = participant.topology.party_to_participant_mappings.is_known(
        synchronizerId,
        partyId,
        Seq(targetParticipantId),
      )
      if (mappingExists) {
        noTracingLogger.info(
          s"The party-to-participant mapping $partyId -> $targetParticipantId already exists on store $synchronizerId"
        )
      } else {
        noTracingLogger.info(
          s"Adding party-to-participant mapping $partyId -> $targetParticipantId on store $synchronizerId"
        )
        participant.topology.party_to_participant_mappings
          .propose_delta(
            partyId,
            adds = List((targetParticipantId, ParticipantPermission.Submission)),
            store = synchronizerId,
          )
          .discard
      }
    }
  }

  private def readGrouped(
      targetParticipant: ParticipantReference,
      sourceFile: String,
      batchSize: Int,
  ): Iterator[Map[SynchronizerAlias, Seq[LfContractId]]] = {
    val idToAlias = targetParticipant.synchronizers
      .list_registered()
      .map { case (synchronizerConnectionConfig, _) =>
        val synchronizerAlias = synchronizerConnectionConfig.synchronizerAlias
        (
          targetParticipant.synchronizers.id_of(synchronizerAlias),
          synchronizerAlias,
        )
      }
      .toMap

    def mapSynchronizerToContracts(
        items: Iterator[HasSynchronizerId & HasSerializableContract]
    ): Iterator[Map[SynchronizerAlias, Seq[LfContractId]]] =
      items
        .grouped(batchSize)
        .map(_.groupBy(_.synchronizerId))
        .map(_.map { case (synchronizerId, items) =>
          val alias = idToAlias.getOrElse(
            synchronizerId,
            throw new IllegalArgumentException(
              s"Data contains contract(s) for unknown synchronizer $synchronizerId. Possibly that synchronizer is currently disconnected."
            ),
          )
          (alias, items.map(_.contract.contractId))
        })

    mapSynchronizerToContracts(ActiveContractOld.fromFile(File(sourceFile)))
  }

}
