// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import cats.syntax.either.*
import cats.syntax.functorFilter.*
import cats.syntax.traverse.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.admin.api.client.commands.TopologyAdminCommands.Write.GenerateTransactions
import com.digitalasset.canton.admin.api.client.commands.{GrpcAdminCommand, TopologyAdminCommands}
import com.digitalasset.canton.admin.api.client.data.topology.*
import com.digitalasset.canton.admin.api.client.data.{
  DynamicSynchronizerParameters as ConsoleDynamicSynchronizerParameters,
  TopologyQueueStatus,
}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{ConsoleCommandTimeout, NonNegativeDuration, RequireTypes}
import com.digitalasset.canton.console.CommandErrors.{CommandError, GenericCommandError}
import com.digitalasset.canton.console.ConsoleEnvironment.Implicits.*
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  CommandErrors,
  CommandSuccessful,
  ConsoleCommandResult,
  ConsoleEnvironment,
  ConsoleMacros,
  FeatureFlag,
  FeatureFlagFilter,
  Help,
  Helpful,
  InstanceReference,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.grpc.ByteStringStreamObserver
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId.Authorized
import com.digitalasset.canton.topology.admin.grpc.{BaseQuery, TopologyStoreId}
import com.digitalasset.canton.topology.admin.v30.{
  ExportTopologySnapshotResponse,
  GenesisStateResponse,
}
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
  TimeQuery,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.transaction.TopologyMapping.MappingHash
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.google.protobuf.ByteString
import io.grpc.Context

import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.reflect.ClassTag
import scala.util.control.NoStackTrace

class TopologyAdministrationGroup(
    instance: InstanceReference,
    topologyQueueStatus: => Option[TopologyQueueStatus],
    val consoleEnvironment: ConsoleEnvironment,
    val loggerFactory: NamedLoggerFactory,
) extends ConsoleCommandGroup
    with Helpful
    with FeatureFlagFilter {

  protected val runner: AdminCommandRunner = instance
  import runner.*
  private def timeouts: ConsoleCommandTimeout = consoleEnvironment.commandTimeouts
  implicit protected[canton] lazy val executionContext: ExecutionContext =
    consoleEnvironment.environment.executionContext

  /** run a topology change command */
  private[console] def runAdminCommand[T](grpcCommand: => GrpcAdminCommand[_, _, T]): T =
    consoleEnvironment.run(adminCommand(grpcCommand))

  @Help.Summary("Initialize the node with a unique identifier")
  @Help.Description("""Every node in Canton is identified using a unique identifier, which is composed
                      |of a user-chosen string and the fingerprint of a signing key. The signing key is the root key
                      |defining a so-called namespace, where the signing key has the ultimate control over
                      |issuing new identifiers.
                      |During initialisation, we have to pick such a unique identifier.
                      |By default, initialisation happens automatically, but it can be changed to either initialize
                      |manually or to read a set of identities and certificates from a file.
                      |
                      |Automatic node initialisation is usually turned off to preserve the identity of a participant or synchronizer
                      |node (during major version upgrades) or if the root namespace key of the node is
                      |kept offline.
                      |
                      |Optionally, a set of delegations can be provided if the root namespace key is not available.
                      |These delegations can be either in files or passed as objects. Their version needs to match the
                      |necessary protocol version of the synchronizers we are going to connect to.
                      |""")
  def init_id(
      identifier: UniqueIdentifier,
      delegations: Seq[GenericSignedTopologyTransaction] = Seq.empty,
      delegationFiles: Seq[String] = Seq.empty,
      waitForReady: Boolean = true,
  ): Unit = {
    if (waitForReady) instance.health.wait_for_ready_for_id()
    val certs = delegationFiles.traverse(
      BinaryFileUtil
        .readByteStringFromFile(_)
        .flatMap(bytes =>
          SignedTopologyTransaction
            .fromByteString(
              ProtocolVersionValidation.NoValidation,
              ProtocolVersionValidation.NoValidation,
              bytes,
            )
            .leftMap(_.message)
        )
    )

    consoleEnvironment.run {
      ConsoleCommandResult.fromEither(certs).flatMap { fileBasedDelegations =>
        adminCommand(
          TopologyAdminCommands.Init.InitId(
            identifier = identifier.identifier.toProtoPrimitive,
            namespace = identifier.namespace.toProtoPrimitive,
            delegations = delegations ++ fileBasedDelegations,
          )
        )
      }
    }
  }

  private def getIdCommand(): ConsoleCommandResult[UniqueIdentifier] =
    adminCommand(TopologyAdminCommands.Init.GetId())

  // small cache to avoid repetitive calls to fetchId (as the id is immutable once set)
  private val idCache =
    new AtomicReference[Option[UniqueIdentifier]](None)

  private[console] def clearCache(): Unit =
    idCache.set(None)

  private[console] def idHelper[T](
      apply: UniqueIdentifier => T
  ): T =
    apply(idCache.get() match {
      case Some(v) => v
      case None =>
        val r = consoleEnvironment.run {
          getIdCommand()
        }
        idCache.set(Some(r))
        r
    })

  private[console] def maybeIdHelper[T](
      apply: UniqueIdentifier => T
  ): Option[T] =
    (idCache.get() match {
      case Some(v) => Some(v)
      case None =>
        consoleEnvironment.run {
          CommandSuccessful(getIdCommand() match {
            case CommandSuccessful(v) =>
              idCache.set(Some(v))
              Some(v)
            case _: CommandError => None
          })
        }
    }).map(apply)

  @Help.Summary("Topology synchronisation helpers", FeatureFlag.Preview)
  @Help.Group("Synchronisation Helpers")
  object synchronisation {

    @Help.Summary("Check if the topology processing of a node is idle")
    @Help.Description(
      """Topology transactions pass through a set of queues before becoming effective on a synchronizer.
        |This function allows to check if all the queues are empty.
        |While both synchronizer and participant nodes support similar queues, there is some ambiguity around
        |the participant queues. While the synchronizer does really know about all in-flight transactions at any
        |point in time, a participant won't know about the state of any transaction that is currently being processed
        |by the synchronizer topology dispatcher."""
    )
    def is_idle(): Boolean =
      topologyQueueStatus
        .forall(_.isIdle) // report un-initialised as idle to not break manual init process

    @Help.Summary("Wait until the topology processing of a node is idle")
    @Help.Description("""This function waits until the `is_idle()` function returns true.""")
    def await_idle(
        timeout: NonNegativeDuration = consoleEnvironment.commandTimeouts.bounded
    ): Unit =
      ConsoleMacros.utils.retry_until_true(timeout)(
        is_idle(),
        s"topology queue status never became idle $topologyQueueStatus after $timeout",
      )
  }

  @Help.Summary("Inspect all topology transactions at once")
  @Help.Group("All Transactions")
  object transactions {

    @Help.Summary("Downloads the node's topology identity transactions")
    @Help.Description(
      "The node's identity is defined by topology transactions of type NamespaceDelegation and OwnerToKeyMapping."
    )
    def identity_transactions(): Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]] =
      instance.topology.transactions
        .list(
          filterMappings = Seq(NamespaceDelegation.code, OwnerToKeyMapping.code),
          filterNamespace = instance.namespace.filterString,
        )
        .result
        .map(_.transaction)

    @Help.Summary("Serializes node's topology identity transactions to a file")
    @Help.Description(
      "Transactions serialized this way should be loaded into another node with load_from_file"
    )
    def export_identity_transactions(file: String): Unit = {
      val bytes = instance.topology.transactions
        .export_topology_snapshot(
          filterMappings = Seq(NamespaceDelegation.code, OwnerToKeyMapping.code),
          filterNamespace = instance.namespace.filterString,
        )
      writeToFile(file, bytes)
    }

    @Help.Summary("Loads topology transactions from a file into the specified topology store")
    @Help.Description("The file must contain data serialized by TopologyTransactions.")
    def import_topology_snapshot_from(file: String, store: TopologyStoreId): Unit =
      BinaryFileUtil.readByteStringFromFile(file).map(import_topology_snapshot(_, store)).valueOr {
        err =>
          throw new IllegalArgumentException(s"import_topology_snapshot failed: $err")
      }
    def import_topology_snapshot(
        topologyTransactions: ByteString,
        store: TopologyStoreId,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.unbounded
        ),
    ): Unit =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Write
            .ImportTopologySnapshot(
              topologyTransactions,
              store,
              synchronize,
            )
        )
      }

    def load(
        transactions: Seq[GenericSignedTopologyTransaction],
        store: TopologyStoreId,
        forceFlags: ForceFlags = ForceFlags.none,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.unbounded
        ),
    ): Unit =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Write
            .AddTransactions(transactions, store, forceFlags, synchronize)
        )
      }

    @Help.Summary("Loads topology transactions from a file into the specified topology store")
    @Help.Description("The file must contain data serialized by SignedTopologyTransaction.")
    def load_single_from_file(
        file: String,
        store: TopologyStoreId,
        forceFlags: ForceFlags = ForceFlags.none,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.unbounded
        ),
    ): Unit = {
      val transaction = SignedTopologyTransaction
        .readFromTrustedFilePVV(file)
        .valueOr { err =>
          consoleEnvironment.run(
            CommandErrors.GenericCommandError(s"Unable to read from `$file`: $err")
          )
        }

      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Write
            .AddTransactions(Seq(transaction), store, forceFlags, synchronize)
        )
      }
    }

    @Help.Summary(
      "Loads topology transactions from a list of files into the specified topology store"
    )
    @Help.Description("The files must contain data serialized by SignedTopologyTransaction.")
    def load_single_from_files(
        files: Seq[String],
        store: TopologyStoreId,
        forceFlags: ForceFlags = ForceFlags.none,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.unbounded
        ),
    ): Unit = {
      val transactions = files.map { file =>
        SignedTopologyTransaction
          .readFromTrustedFilePVV(file)
          .valueOr { err =>
            consoleEnvironment.run(
              CommandErrors.GenericCommandError(s"Unable to read from `$file`: $err")
            )
          }
      }

      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Write
            .AddTransactions(transactions, store, forceFlags, synchronize)
        )
      }
    }

    @Help.Summary("Loads topology transactions from a file into the specified topology store")
    @Help.Description("The file must contain data serialized by SignedTopologyTransactions.")
    def load_multiple_from_file(
        file: String,
        store: TopologyStoreId,
        forceFlags: ForceFlags = ForceFlags.none,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.unbounded
        ),
    ): Unit = {
      val transactions = SignedTopologyTransactions
        .readFromTrustedFile(ProtocolVersionValidation.NoValidation, file)
        .valueOr { err =>
          consoleEnvironment.run(
            CommandErrors.GenericCommandError(s"Unable to read from `$file`: $err")
          )
        }

      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Write
            .AddTransactions(transactions.transactions, store, forceFlags, synchronize)
        )
      }
    }

    def generate(
        proposals: Seq[GenerateTransactions.Proposal]
    ): Seq[TopologyTransaction[TopologyChangeOp, TopologyMapping]] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Write
            .GenerateTransactions(proposals)
        )
      }

    def sign(
        transactions: Seq[GenericSignedTopologyTransaction],
        store: TopologyStoreId,
        signedBy: Seq[Fingerprint] = Seq.empty,
        forceFlags: ForceFlags = ForceFlags.none,
    ): Seq[GenericSignedTopologyTransaction] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Write.SignTransactions(transactions, store, signedBy, forceFlags)
        )
      }

    def authorize[M <: TopologyMapping: ClassTag](
        txHash: TxHash,
        mustBeFullyAuthorized: Boolean,
        store: TopologyStoreId,
        signedBy: Seq[Fingerprint] = Seq.empty,
    ): SignedTopologyTransaction[TopologyChangeOp, M] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Write.Authorize(
            txHash.hash.toHexString,
            mustFullyAuthorize = mustBeFullyAuthorized,
            signedBy = signedBy,
            store = store,
          )
        )
      }

    @Help.Summary("List all transaction")
    def list(
        store: TopologyStoreId = TopologyStoreId.Authorized,
        proposals: Boolean = false,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = Some(TopologyChangeOp.Replace),
        filterMappings: Seq[TopologyMapping.Code] = Nil,
        excludeMappings: Seq[TopologyMapping.Code] = Nil,
        filterAuthorizedKey: Option[Fingerprint] = None,
        protocolVersion: Option[String] = None,
        filterNamespace: String = "",
    ): StoredTopologyTransactions[TopologyChangeOp, TopologyMapping] = {
      if (filterMappings.nonEmpty && excludeMappings.nonEmpty) {
        consoleEnvironment.run(
          CommandErrors
            .GenericCommandError("Cannot specify both filterMappings and excludeMappings")
        )
      }
      val excludeMappingsCodes = if (filterMappings.nonEmpty) {
        TopologyMapping.Code.all.diff(filterMappings).map(_.code)
      } else excludeMappings.map(_.code)

      consoleEnvironment
        .run {
          adminCommand(
            TopologyAdminCommands.Read.ListAll(
              BaseQuery(
                store,
                proposals,
                timeQuery,
                operation,
                filterSigningKey = filterAuthorizedKey.map(_.toProtoPrimitive).getOrElse(""),
                protocolVersion.map(ProtocolVersion.tryCreate),
              ),
              excludeMappings = excludeMappingsCodes,
              filterNamespace = filterNamespace,
            )
          )
        }
    }

    @Help.Summary("export topology snapshot")
    @Help.Description(
      """This command export the node's topology transactions as byte string.
        |
        |The arguments are:
        |excludeMappings: a list of topology mapping codes to exclude from the export. If not provided, all mappings are included.
        |filterNamespace: the namespace to filter the transactions by.
        |protocolVersion: the protocol version used to serialize the topology transactions. If not provided, the latest protocol version is used.
        """
    )
    def export_topology_snapshot(
        store: TopologyStoreId = TopologyStoreId.Authorized,
        proposals: Boolean = false,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = None,
        filterMappings: Seq[TopologyMapping.Code] = Nil,
        excludeMappings: Seq[TopologyMapping.Code] = Nil,
        filterAuthorizedKey: Option[Fingerprint] = None,
        protocolVersion: Option[String] = None,
        filterNamespace: String = "",
        timeout: NonNegativeDuration = timeouts.unbounded,
    ): ByteString = {
      if (filterMappings.nonEmpty && excludeMappings.nonEmpty) {
        consoleEnvironment.run(
          CommandErrors
            .GenericCommandError("Cannot specify both filterMappings and excludeMappings")
        )
      }
      val excludeMappingsCodes = if (filterMappings.nonEmpty) {
        TopologyMapping.Code.all.diff(filterMappings).map(_.code)
      } else excludeMappings.map(_.code)

      consoleEnvironment
        .run {
          val responseObserver =
            new ByteStringStreamObserver[ExportTopologySnapshotResponse](_.chunk)

          def call: ConsoleCommandResult[Context.CancellableContext] =
            adminCommand(
              TopologyAdminCommands.Read.ExportTopologySnapshot(
                responseObserver,
                BaseQuery(
                  store,
                  proposals,
                  timeQuery,
                  operation,
                  filterSigningKey = filterAuthorizedKey.map(_.toProtoPrimitive).getOrElse(""),
                  protocolVersion.map(ProtocolVersion.tryCreate),
                ),
                excludeMappings = excludeMappingsCodes,
                filterNamespace = filterNamespace,
              )
            )
          processResult(
            call,
            responseObserver.resultBytes,
            timeout,
            s"Exporting the topology state from store $store",
          )
        }
    }

    @Help.Summary(
      "Download the genesis state for a sequencer. This method should be used when performing a major synchronizer upgrade."
    )
    @Help.Description(
      """Download the topology snapshot which includes the entire history of topology transactions to initialize a sequencer for a major synchronizer upgrades. The validFrom and validUntil are set to SignedTopologyTransaction.InitialTopologySequencingTime.
        |filterSynchronizerStore: Must be specified if the genesis state is requested from a participant node.
        |timestamp: If not specified, the max effective time of the latest topology transaction is used. Otherwise, the given timestamp is used.
        """
    )
    def genesis_state(
        filterSynchronizerStore: Option[TopologyStoreId.Synchronizer] = None,
        timestamp: Option[CantonTimestamp] = None,
        timeout: NonNegativeDuration = timeouts.unbounded,
    ): ByteString =
      consoleEnvironment.run {
        val responseObserver = new ByteStringStreamObserver[GenesisStateResponse](_.chunk)

        def call: ConsoleCommandResult[Context.CancellableContext] =
          adminCommand(
            TopologyAdminCommands.Read.GenesisState(
              responseObserver,
              synchronizerStore = filterSynchronizerStore,
              timestamp = timestamp,
            )
          )

        processResult(call, responseObserver.resultBytes, timeout, "Downloading the genesis state")
      }

    @Help.Summary("Find the latest transaction for a given mapping hash")
    @Help.Description(
      """
        mappingHash: the unique key of the topology mapping to find
        store: - "Authorized": the topology transaction will be looked up in the node's authorized store.
               - "<synchronizer id>": the topology transaction will be looked up in the specified synchronizer store.
        includeProposals: when true, the result could be the latest proposal, otherwise will only return the latest fully authorized transaction"""
    )
    def find_latest_by_mapping_hash[M <: TopologyMapping: ClassTag](
        mappingHash: MappingHash,
        store: TopologyStoreId,
        includeProposals: Boolean = false,
    ): Option[StoredTopologyTransaction[TopologyChangeOp, M]] = {
      val latestAuthorized = list(store = store)
        .collectOfMapping[M]
        .filter(_.mapping.uniqueKey == mappingHash)
        .result
      val latestProposal =
        if (includeProposals)
          list(store = store, proposals = true)
            .collectOfMapping[M]
            .filter(_.mapping.uniqueKey == mappingHash)
            .result
        else Seq.empty
      (latestAuthorized ++ latestProposal).maxByOption(_.serial)
    }

    @Help.Summary("Find the latest transaction for a given mapping hash")
    @Help.Description(
      """
        store: - "Authorized": the topology transaction will be looked up in the node's authorized store.
               - "<synchronizer id>": the topology transaction will be looked up in the specified synchronizer store.
        includeProposals: when true, the result could be the latest proposal, otherwise will only return the latest fully authorized transaction"""
    )
    def find_latest_by_mapping[M <: TopologyMapping: ClassTag](
        store: TopologyStoreId,
        includeProposals: Boolean = false,
    ): Option[StoredTopologyTransaction[TopologyChangeOp, M]] = {
      val latestAuthorized = list(store = store)
        .collectOfMapping[M]
        .result
      val latestProposal =
        if (includeProposals)
          list(store = store, proposals = true)
            .collectOfMapping[M]
            .result
        else Seq.empty
      (latestAuthorized ++ latestProposal).maxByOption(_.serial)
    }

    @Help.Summary("Manage topology transaction purging", FeatureFlag.Preview)
    @Help.Group("Purge Topology Transactions")
    object purge extends Helpful {
      def list(
          store: TopologyStoreId,
          proposals: Boolean = false,
          timeQuery: TimeQuery = TimeQuery.HeadState,
          operation: Option[TopologyChangeOp] = Some(TopologyChangeOp.Replace),
          filterSynchronizer: String = "",
          filterSigningKey: String = "",
          protocolVersion: Option[String] = None,
      ): Seq[ListPurgeTopologyTransactionResult] = consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Read.PurgeTopologyTransaction(
            BaseQuery(
              store,
              proposals,
              timeQuery,
              operation,
              filterSigningKey,
              protocolVersion.map(ProtocolVersion.tryCreate),
            ),
            filterSynchronizer,
          )
        )
      }

      // TODO(#15236): implement write service for purging
    }
  }

  object synchronizer_bootstrap {

    @Help.Summary(
      """Creates and returns proposals of topology transactions to bootstrap a synchronizer, specifically
        |SynchronizerParametersState, SequencerSynchronizerState, and MediatorSynchronizerState.""".stripMargin
    )
    def generate_genesis_topology(
        synchronizerId: SynchronizerId,
        synchronizerOwners: Seq[Member],
        sequencers: Seq[SequencerId],
        mediators: Seq[MediatorId],
        store: TopologyStoreId,
    ): Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]] = {
      val isSynchronizerOwner = synchronizerOwners.contains(instance.id)
      require(isSynchronizerOwner, s"Only synchronizer owners should call $functionFullName.")

      def latest[M <: TopologyMapping: ClassTag](hash: MappingHash) =
        instance.topology.transactions
          .find_latest_by_mapping_hash[M](
            hash,
            store = store,
            includeProposals = true,
          )
          .map(_.transaction)

      // create and sign the initial synchronizer parameters
      val synchronizerParameterState =
        latest[SynchronizerParametersState](SynchronizerParametersState.uniqueKey(synchronizerId))
          .getOrElse(
            instance.topology.synchronizer_parameters.propose(
              synchronizerId,
              ConsoleDynamicSynchronizerParameters
                .initialValues(
                  consoleEnvironment.environment.clock,
                  ProtocolVersion.latest,
                ),
              signedBy = None,
              store = Some(store),
              synchronize = None,
            )
          )

      val mediatorState =
        latest[MediatorSynchronizerState](
          MediatorSynchronizerState.uniqueKey(synchronizerId, NonNegativeInt.zero)
        )
          .getOrElse(
            instance.topology.mediators.propose(
              synchronizerId,
              threshold = PositiveInt.one,
              group = NonNegativeInt.zero,
              active = mediators,
              signedBy = None,
              store = Some(store),
            )
          )

      val sequencerState =
        latest[SequencerSynchronizerState](SequencerSynchronizerState.uniqueKey(synchronizerId))
          .getOrElse(
            instance.topology.sequencers.propose(
              synchronizerId,
              threshold = PositiveInt.one,
              active = sequencers,
              signedBy = None,
              store = Some(store),
            )
          )

      Seq(synchronizerParameterState, sequencerState, mediatorState)
    }

    @Help.Summary(
      """Creates and returns proposals of topology transactions to bootstrap a synchronizer, specifically
        |SynchronizerParametersState, SequencerSynchronizerState, and MediatorSynchronizerState,
        |and stores the result in a file which can be loaded using `node.topology.transactions.load_multiple_from_file`""".stripMargin
    )
    def download_genesis_topology(
        synchronizerId: SynchronizerId,
        synchronizerOwners: Seq[Member],
        sequencers: Seq[SequencerId],
        mediators: Seq[MediatorId],
        outputFile: String,
        store: TopologyStoreId,
    ): Unit = {

      val transactions =
        generate_genesis_topology(
          synchronizerId,
          synchronizerOwners,
          sequencers,
          mediators,
          store,
        )

      SignedTopologyTransactions(transactions, ProtocolVersion.latest).writeToFile(outputFile)
    }
  }

  @Help.Summary("Manage decentralized namespaces")
  @Help.Group("Decentralized namespaces")
  object decentralized_namespaces extends Helpful {
    def list(
        store: TopologyStoreId,
        proposals: Boolean = false,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = Some(TopologyChangeOp.Replace),
        filterNamespace: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListDecentralizedNamespaceDefinitionResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommands.Read.ListDecentralizedNamespaceDefinition(
          BaseQuery(
            store,
            proposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterNamespace,
        )
      )
    }

    @Help.Summary("Propose the creation of a new decentralized namespace")
    @Help.Description("""
        owners: the namespaces of the founding members of the decentralized namespace, which are used to compute the name of the decentralized namespace.
        threshold: this threshold specifies the minimum number of signatures of decentralized namespace members that are required to
                   satisfy authorization requirements on topology transactions for the namespace of the decentralized namespace.

        store: - "Authorized": the topology transaction will be stored in the node's authorized store and automatically
                               propagated to connected synchronizers, if applicable.
               - "<synchronizer id>": the topology transaction will be directly submitted to the specified synchronizer without
                                storing it locally first. This also means it will _not_ be synchronized to other synchronizers
                                automatically.
        mustFullyAuthorize: when set to true, the proposal's previously received signatures and the signature of this node must be
                            sufficient to fully authorize the topology transaction. if this is not the case, the request fails.
                            when set to false, the proposal retains the proposal status until enough signatures are accumulated to
                            satisfy the mapping's authorization requirements.
        signedBy: the fingerprint of the key to be used to sign this proposal
        serial: the expected serial this topology transaction should have. Serials must be contiguous and start at 1.
                This transaction will be rejected if another fully authorized transaction with the same serial already
                exists, or if there is a gap between this serial and the most recently used serial.
                If None, the serial will be automatically selected by the node.""")
    def propose_new(
        owners: Set[Namespace],
        threshold: PositiveInt,
        store: TopologyStoreId,
        mustFullyAuthorize: Boolean = false,
        signedBy: Option[Fingerprint] = None,
        serial: Option[PositiveInt] = None,
        synchronize: Option[config.NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): SignedTopologyTransaction[TopologyChangeOp, DecentralizedNamespaceDefinition] = {
      val ownersNE = NonEmpty
        .from(owners)
        .getOrElse(
          consoleEnvironment.run(
            CommandErrors
              .GenericCommandError("Proposed decentralized namespace needs at least one owner")
          )
        )
      val decentralizedNamespace = DecentralizedNamespaceDefinition
        .create(
          DecentralizedNamespaceDefinition.computeNamespace(owners),
          threshold,
          ownersNE,
        )
        .valueOr(error => consoleEnvironment.run(GenericCommandError(error)))
      propose(
        decentralizedNamespace,
        store,
        mustFullyAuthorize,
        signedBy.toList,
        serial,
        synchronize,
      )
    }

    @Help.Summary("Propose changes to a decentralized namespace")
    @Help.Description("""
        decentralizedNamespace: the DecentralizedNamespaceDefinition to propose

        store: - "Authorized": the topology transaction will be stored in the node's authorized store and automatically
                               propagated to connected synchronizers, if applicable.
               - "<synchronizer id>": the topology transaction will be directly submitted to the specified synchronizer without
                                storing it locally first. This also means it will _not_ be synchronized to other synchronizers
                                automatically.
        mustFullyAuthorize: when set to true, the proposal's previously received signatures and the signature of this node must be
                            sufficient to fully authorize the topology transaction. if this is not the case, the request fails.
                            when set to false, the proposal retains the proposal status until enough signatures are accumulated to
                            satisfy the mapping's authorization requirements.
        signedBy: the fingerprint of the key to be used to sign this proposal
        serial: the expected serial this topology transaction should have. Serials must be contiguous and start at 1.
                This transaction will be rejected if another fully authorized transaction with the same serial already
                exists, or if there is a gap between this serial and the most recently used serial.
                If None, the serial will be automatically selected by the node.""")
    def propose(
        decentralizedNamespace: DecentralizedNamespaceDefinition,
        store: TopologyStoreId,
        mustFullyAuthorize: Boolean = false,
        signedBy: Seq[Fingerprint] = Seq.empty,
        serial: Option[PositiveInt] = None,
        synchronize: Option[config.NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): SignedTopologyTransaction[TopologyChangeOp, DecentralizedNamespaceDefinition] = {

      val command = TopologyAdminCommands.Write.Propose(
        decentralizedNamespace,
        signedBy = signedBy.toList,
        serial = serial,
        change = TopologyChangeOp.Replace,
        mustFullyAuthorize = mustFullyAuthorize,
        forceChanges = ForceFlags.none,
        store = store,
        waitToBecomeEffective = synchronize,
      )

      runAdminCommand(command)
    }
  }

  @Help.Summary("Manage namespace delegations")
  @Help.Group("Namespace delegations")
  object namespace_delegations extends Helpful {
    @Help.Summary("Propose a new namespace delegation")
    @Help.Description(
      """A namespace delegation allows the owner of a namespace to delegate signing privileges for
        |topology transactions on behalf of said namespace to additional signing keys.

        namespace: the namespace for which the target key can be used to sign topology transactions
        targetKey: the target key to be used for signing topology transactions on behalf of the namespace
        isRootDelegation: when set to true, the target key may authorize topology transactions with any kind of mapping,
                          including other namespace delegations.
                          when set to false, the target key may not authorize other namespace delegations for this namespace.

        store: - "Authorized": the topology transaction will be stored in the node's authorized store and automatically
                               propagated to connected synchronizers, if applicable.
               - "<synchronizer id>": the topology transaction will be directly submitted to the specified synchronizer without
                                storing it locally first. This also means it will _not_ be synchronized to other synchronizers
                                automatically.
        mustFullyAuthorize: when set to true, the proposal's previously received signatures and the signature of this node must be
                            sufficient to fully authorize the topology transaction. If this is not the case, the request fails.
                            When set to false, the proposal retains the proposal status until enough signatures are accumulated to
                            satisfy the mapping's authorization requirements.
        serial: the expected serial this topology transaction should have. Serials must be contiguous and start at 1.
                This transaction will be rejected if another fully authorized transaction with the same serial already
                exists, or if there is a gap between this serial and the most recently used serial.
                If None, the serial will be automatically selected by the node.
        """
    )
    def propose_delegation(
        namespace: Namespace,
        targetKey: SigningPublicKey,
        isRootDelegation: Boolean,
        store: TopologyStoreId = TopologyStoreId.Authorized,
        mustFullyAuthorize: Boolean = true,
        serial: Option[PositiveInt] = None,
        signedBy: Seq[Fingerprint] = Seq.empty,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        forceFlags: ForceFlags = ForceFlags.none,
    ): SignedTopologyTransaction[TopologyChangeOp, NamespaceDelegation] =
      runAdminCommand(
        TopologyAdminCommands.Write.Propose(
          NamespaceDelegation.create(namespace, targetKey, isRootDelegation),
          signedBy = signedBy,
          store = store,
          serial = serial,
          change = TopologyChangeOp.Replace,
          mustFullyAuthorize = mustFullyAuthorize,
          forceChanges = forceFlags,
          waitToBecomeEffective = synchronize,
        )
      )

    @Help.Summary("Revoke an existing namespace delegation")
    @Help.Description(
      """A namespace delegation allows the owner of a namespace to delegate signing privileges for
        |topology transactions on behalf of said namespace to additional signing keys.

        namespace: the namespace for which the target key should be revoked
        targetKey: the target key to be revoked

        store: - "Authorized": the topology transaction will be stored in the node's authorized store and automatically
                               propagated to connected synchronizers, if applicable.
               - "<synchronizer id>": the topology transaction will be directly submitted to the specified synchronizer without
                                storing it locally first. This also means it will _not_ be synchronized to other synchronizers
                                automatically.
        mustFullyAuthorize: when set to true, the proposal's previously received signatures and the signature of this node must be
                            sufficient to fully authorize the topology transaction. If this is not the case, the request fails.
                            When set to false, the proposal retains the proposal status until enough signatures are accumulated to
                            satisfy the mapping's authorization requirements.
        serial: the expected serial this topology transaction should have. Serials must be contiguous and start at 1.
                This transaction will be rejected if another fully authorized transaction with the same serial already
                exists, or if there is a gap between this serial and the most recently used serial.
                If None, the serial will be automatically selected by the node.
        force: must be set to true when performing a dangerous operation, such as revoking a root certificate
        """
    )
    def propose_revocation(
        namespace: Namespace,
        targetKey: SigningPublicKey,
        store: TopologyStoreId = TopologyStoreId.Authorized,
        mustFullyAuthorize: Boolean = true,
        serial: Option[PositiveInt] = None,
        signedBy: Seq[Fingerprint] = Seq.empty,
        forceChanges: ForceFlags = ForceFlags.none,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): SignedTopologyTransaction[TopologyChangeOp, NamespaceDelegation] =
      list(
        store,
        filterNamespace = namespace.toProtoPrimitive,
        filterTargetKey = Some(targetKey.id),
      ) match {
        case Seq(nsd) =>
          runAdminCommand(
            TopologyAdminCommands.Write.Propose(
              nsd.item,
              signedBy = signedBy,
              store = store,
              serial = serial,
              change = TopologyChangeOp.Remove,
              mustFullyAuthorize = mustFullyAuthorize,
              forceChanges = forceChanges,
              waitToBecomeEffective = synchronize,
            )
          )

        case Nil =>
          throw new IllegalArgumentException(
            s"Namespace delegation from namespace $namespace to key ${targetKey.id} not found."
          )
        case multiple =>
          throw new IllegalStateException(
            s"The query for namespace $namespace and target key ${targetKey.id} unexpectedly yielded multiple results: ${multiple
                .map(_.item)}"
          )
      }

    def list(
        store: TopologyStoreId,
        proposals: Boolean = false,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = Some(TopologyChangeOp.Replace),
        filterNamespace: String = "",
        filterSigningKey: String = "",
        filterTargetKey: Option[Fingerprint] = None,
        protocolVersion: Option[String] = None,
    ): Seq[ListNamespaceDelegationResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommands.Read.ListNamespaceDelegation(
          BaseQuery(
            store,
            proposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterNamespace,
          filterTargetKey,
        )
      )
    }
  }

  @Help.Summary("Manage identifier delegations")
  @Help.Group("Identifier delegations")
  object identifier_delegations extends Helpful {

    @Help.Summary("Propose new identifier delegations")
    @Help.Description(
      """An identifier delegation allows the owner of a unique identifier to delegate signing privileges for
        |topology transactions on behalf of said identifier to additional/specific signing keys.

        uid: the unique identifier for which the target key can be used to sign topology transactions
        targetKey: the target key to be used for signing topology transactions on behalf of the unique identifier

        store: - "Authorized": the topology transaction will be stored in the node's authorized store and automatically
                               propagated to connected synchronizers, if applicable.
               - "<synchronizer id>": the topology transaction will be directly submitted to the specified synchronizer without
                                storing it locally first. This also means it will _not_ be synchronized to other synchronizers
                                automatically.
        mustFullyAuthorize: when set to true, the proposal's previously received signatures and the signature of this node must be
                            sufficient to fully authorize the topology transaction. if this is not the case, the request fails.
                            when set to false, the proposal retains the proposal status until enough signatures are accumulated to
                            satisfy the mapping's authorization requirements.
        serial: the expected serial this topology transaction should have. Serials must be contiguous and start at 1.
                This transaction will be rejected if another fully authorized transaction with the same serial already
                exists, or if there is a gap between this serial and the most recently used serial.
                If None, the serial will be automatically selected by the node.
        signedBy: the list of keys used to sign the proposal. If empty, it will be auto-computed.
        """
    )
    def propose(
        uid: UniqueIdentifier,
        targetKey: SigningPublicKey,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        // Using the authorized store by default
        store: TopologyStoreId = TopologyStoreId.Authorized,
        mustFullyAuthorize: Boolean = true,
        serial: Option[PositiveInt] = None,
        signedBy: Seq[Fingerprint] = Seq.empty,
        change: TopologyChangeOp = TopologyChangeOp.Replace,
    ): SignedTopologyTransaction[TopologyChangeOp, IdentifierDelegation] = {
      val command = TopologyAdminCommands.Write.Propose(
        mapping = IdentifierDelegation(
          identifier = uid,
          target = targetKey,
        ),
        signedBy = signedBy,
        serial = serial,
        change = change,
        mustFullyAuthorize = mustFullyAuthorize,
        store = store,
        waitToBecomeEffective = synchronize,
      )
      runAdminCommand(command)
    }

    def list(
        store: TopologyStoreId,
        proposals: Boolean = false,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = Some(TopologyChangeOp.Replace),
        filterUid: String = "",
        filterSigningKey: String = "",
        filterTargetKey: Option[Fingerprint] = None,
        protocolVersion: Option[String] = None,
    ): Seq[ListIdentifierDelegationResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommands.Read.ListIdentifierDelegation(
          BaseQuery(
            store,
            proposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterUid,
          filterTargetKey,
        )
      )
    }
  }

  // TODO(#14057) complete @Help.Description's (by adapting TopologyAdministrationGroup descriptions from main-2.x)
  @Help.Summary("Manage owner to key mappings")
  @Help.Group("Owner to key mappings")
  object owner_to_key_mappings extends Helpful {

    @Help.Summary("List owner to key mapping transactions")
    def list(
        store: Option[TopologyStoreId] = None,
        proposals: Boolean = false,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = Some(TopologyChangeOp.Replace),
        filterKeyOwnerType: Option[MemberCode] = None,
        filterKeyOwnerUid: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListOwnerToKeyMappingResult] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Read.ListOwnerToKeyMapping(
            BaseQuery(
              store,
              proposals,
              timeQuery,
              operation,
              filterSigningKey,
              protocolVersion.map(ProtocolVersion.tryCreate),
            ),
            filterKeyOwnerType,
            filterKeyOwnerUid,
          )
        )
      }

    @Help.Summary("Add a key to an owner to key mapping")
    @Help.Description(
      """Add a key to an owner to key mapping. A key owner is anyone in the system that needs a key-pair known
        |to all members (participants, mediators, sequencers) of a synchronizer. If no owner to key mapping exists for the
        |specified key owner, create a new mapping with the specified key. The specified key needs to have
        |been created previously via the `keys.secret` api.

        key: Fingerprint of the key
        purpose: The key purpose, i.e. whether the key is for signing or encryption
        keyOwner: The member that owns the key
        signedBy: Optional fingerprint of the authorizing key which in turn refers to a specific, locally existing certificate.
        synchronize: Synchronize timeout can be used to ensure that the state has been propagated into the node
        mustFullyAuthorize: Whether to only add the key if the member is in the position to authorize the change.
      """
    )
    def add_key(
        key: Fingerprint,
        purpose: KeyPurpose,
        keyOwner: Member = instance.id.member,
        signedBy: Seq[Fingerprint] = Seq.empty,
        synchronize: Option[config.NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        // configurable in case of a key under a decentralized namespace
        mustFullyAuthorize: Boolean = true,
    ): Unit = update(
      NonEmpty.mk(Seq, (key, purpose)),
      keyOwner,
      signedBy,
      synchronize,
      add = true,
      mustFullyAuthorize = mustFullyAuthorize,
      force = ForceFlags.none,
    )

    @Help.Summary("Add a set of keys to an owner to key mapping")
    @Help.Description(
      """Add a set of keys to an owner to key mapping. A key owner is anyone in the system that needs a key-pair known
        |to all members (participants, mediators, sequencers) of a synchronizer. If no owner to key mapping exists for the
        |specified key owner, create a new mapping with the specified keys. The specified keys needs to have
        |been created previously via the `keys.secret` api.

        keys: Fingerprint and key purpose of the keys
        keyOwner: The member that owns the key
        signedBy: Optional fingerprint of the authorizing key which in turn refers to a specific, locally existing certificate.
        synchronize: Synchronize timeout can be used to ensure that the state has been propagated into the node
        mustFullyAuthorize: Whether to only add the key if the member is in the position to authorize the change.
      """
    )
    def add_keys(
        keys: Seq[(Fingerprint, KeyPurpose)],
        keyOwner: Member = instance.id.member,
        signedBy: Seq[Fingerprint] = Seq.empty,
        synchronize: Option[config.NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        // configurable in case of a key under a decentralized namespace
        mustFullyAuthorize: Boolean = true,
    ): Unit =
      update(
        NonEmpty
          .from(keys)
          .getOrElse(throw new IllegalArgumentException("At least one key is required")),
        keyOwner,
        signedBy,
        synchronize,
        add = true,
        mustFullyAuthorize = mustFullyAuthorize,
        force = ForceFlags.none,
      )

    @Help.Summary("Remove a key from an owner to key mapping")
    @Help.Description(
      """Remove a key from an owner to key mapping. A key owner is anyone in the system that needs a key-pair known
        |to all members (participants, mediators, sequencers) of a synchronizer. If the specified key is the last key in the
        |owner to key mapping (which requires the force to be true), the owner to key mapping will be removed.
        |The specified key needs to have been created previously via the `keys.secret` api.

        key: Fingerprint of the key
        purpose: The key purpose, i.e. whether the key is for signing or encryption
        keyOwner: The member that owns the key
        signedBy: Optional fingerprint of the authorizing key which in turn refers to a specific, locally existing certificate.
        synchronize: Synchronize timeout can be used to ensure that the state has been propagated into the node
        mustFullyAuthorize: Whether to only add the key if the member is in the position to authorize the change.
        force: removing the last key is dangerous and must therefore be manually forced
      """
    )
    def remove_key(
        key: Fingerprint,
        purpose: KeyPurpose,
        keyOwner: Member = instance.id.member,
        signedBy: Seq[Fingerprint] = Seq.empty,
        synchronize: Option[config.NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        // configurable in case of a key under a decentralized namespace
        mustFullyAuthorize: Boolean = true,
        force: ForceFlags = ForceFlags.none,
    ): Unit = update(
      NonEmpty.mk(Seq, (key, purpose)),
      keyOwner,
      signedBy,
      synchronize,
      add = false,
      mustFullyAuthorize = mustFullyAuthorize,
      force = force,
    )

    @Help.Summary("Rotate the key for an owner to key mapping")
    @Help.Description(
      """Rotates an existing key of the owner's owner to key mapping by adding the new key and removing the previous
        |key.

        nodeInstance: The node instance that is used to verify that both the current and new key pertain to this node.
                      This avoids conflicts when there are different nodes with the same uuid (i.e., multiple sequencers).
        owner: The member that owns the owner to key mapping
        currentKey: The current public key that will be rotated
        newKey: The new public key that has been generated
      """
    )
    def rotate_key(
        member: Member,
        currentKey: PublicKey,
        newKey: PublicKey,
        synchronize: Option[config.NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): Unit = {
      val keysInStore = instance.keys.secret.list().map(_.publicKey)
      require(
        keysInStore.contains(currentKey),
        "The current key must exist and pertain to this node",
      )
      require(keysInStore.contains(newKey), "The new key must exist and pertain to this node")
      require(currentKey.purpose == newKey.purpose, "The rotated keys must have the same purpose")

      def verifyKeyPresence(key: PublicKey, contains: Boolean): Unit =
        // retry until we observe the change in the respective store
        ConsoleMacros.utils.retry_until_true(
          instance.topology.owner_to_key_mappings
            .list(
              filterKeyOwnerUid = instance.uid.toProtoPrimitive
            )
            .filter(_.context.storeId != Authorized)
            .forall(_.item.keys.contains(key) == contains)
        )(consoleEnvironment)

      // Authorize the new key
      // The owner will now have two keys, but by convention the first one added is always
      // used by everybody.
      update(
        NonEmpty.mk(Seq, (newKey.fingerprint, newKey.purpose)),
        member,
        signedBy = Seq.empty,
        add = true,
        synchronize = synchronize,
      )

      verifyKeyPresence(newKey, contains = true)

      // Remove the old key by sending the matching `Remove` transaction
      update(
        NonEmpty.mk(Seq, (currentKey.fingerprint, currentKey.purpose)),
        member,
        signedBy = Seq.empty,
        add = false,
        synchronize = synchronize,
      )

      verifyKeyPresence(currentKey, contains = false)

      // TODO(#24218): reduce the risk of using a new key that is not yet recognized by another node.
    }

    private def ensurePrivateKeyExists(fingerprint: Fingerprint, purpose: KeyPurpose): PublicKey =
      instance.keys.secret.list(
        filterFingerprint = fingerprint.toProtoPrimitive,
        filterPurpose = Set(purpose),
      ) match {
        case privateKeyMetadata +: Nil => privateKeyMetadata.publicKey
        case Nil =>
          throw new IllegalArgumentException("The specified key is unknown to the key owner")
        case multipleKeys =>
          throw new IllegalArgumentException(
            s"Found ${multipleKeys.size} keys where only one key was expected. Specify a full key instead of a prefix"
          )
      }

    private def update(
        keys: NonEmpty[Seq[(Fingerprint, KeyPurpose)]],
        keyOwner: Member,
        signedBy: Seq[Fingerprint],
        synchronize: Option[config.NonNegativeDuration],
        add: Boolean,
        mustFullyAuthorize: Boolean = true,
        force: ForceFlags = ForceFlags.none,
    ): Unit = {

      val publicKeys = keys.map { case (fingerprint, purpose) =>
        // Ensure the specified key has a private key in the vault and get the public key
        ensurePrivateKeyExists(fingerprint, purpose)
      }

      // Look for an existing authorized OKM mapping.
      val maybePreviousState = expectAtMostOneResult(
        list(
          store = Some(TopologyStoreId.Authorized),
          filterKeyOwnerUid = keyOwner.filterString,
          filterKeyOwnerType = Some(keyOwner.code),
        )
      ).map(res => (res.item, res.context.operation, res.context.serial))

      val (proposedMapping, serial, ops) = if (add) {
        // Add key to mapping with serial + 1 or create new mapping.
        maybePreviousState match {
          case None =>
            (
              OwnerToKeyMapping(keyOwner, publicKeys),
              PositiveInt.one,
              TopologyChangeOp.Replace,
            )
          case Some((_, TopologyChangeOp.Remove, previousSerial)) =>
            (
              OwnerToKeyMapping(keyOwner, publicKeys),
              previousSerial.increment,
              TopologyChangeOp.Replace,
            )
          case Some((okm, TopologyChangeOp.Replace, previousSerial)) =>
            require(
              okm.keys.intersect(publicKeys).isEmpty,
              "The owner-to-key mapping already contains the specified keys to add",
            )
            (
              okm.copy(keys = okm.keys ++ publicKeys),
              previousSerial.increment,
              TopologyChangeOp.Replace,
            )
        }
      } else {
        // Remove key from mapping with serial + 1 or error.
        maybePreviousState match {
          case None | Some((_, TopologyChangeOp.Remove, _)) =>
            throw new IllegalArgumentException(
              "No authorized owner-to-key mapping exists for specified key owner"
            )
          case Some((okm, TopologyChangeOp.Replace, previousSerial)) =>
            require(
              // All publicKeys to be removed must be in okm.keys
              publicKeys.forall(okm.keys.contains),
              "The owner-to-key mapping does not contain the specified keys to remove",
            )
            // Remove publicKeys from okm.keys
            NonEmpty.from(okm.keys.filterNot(publicKeys.contains)) match {
              case Some(fewerKeys) =>
                (okm.copy(keys = fewerKeys), previousSerial.increment, TopologyChangeOp.Replace)
              case None =>
                (okm, previousSerial.increment, TopologyChangeOp.Remove)
            }
        }
      }

      propose(
        proposedMapping,
        serial,
        ops,
        signedBy,
        TopologyStoreId.Authorized,
        synchronize,
        mustFullyAuthorize,
        force,
      ).discard
    }

    def propose(
        proposedMapping: OwnerToKeyMapping,
        serial: RequireTypes.PositiveNumeric[Int],
        ops: TopologyChangeOp = TopologyChangeOp.Replace,
        signedBy: Seq[Fingerprint] = Seq.empty,
        store: TopologyStoreId = TopologyStoreId.Authorized,
        synchronize: Option[config.NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        // configurable in case of a key under a decentralized namespace
        mustFullyAuthorize: Boolean = true,
        force: ForceFlags = ForceFlags.none,
    ): SignedTopologyTransaction[TopologyChangeOp, OwnerToKeyMapping] =
      runAdminCommand(
        TopologyAdminCommands.Write.Propose(
          mapping = proposedMapping,
          signedBy = signedBy,
          store = store,
          change = ops,
          serial = Some(serial),
          mustFullyAuthorize = mustFullyAuthorize,
          forceChanges = force,
          waitToBecomeEffective = synchronize,
        )
      )
  }

  @Help.Summary("Manage party to key mappings")
  @Help.Group("Party to key mappings")
  object party_to_key_mappings extends Helpful {

    @Help.Summary("List party to key mapping transactions")
    def list(
        store: TopologyStoreId,
        proposals: Boolean = false,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = Some(TopologyChangeOp.Replace),
        filterParty: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListPartyToKeyMappingResult] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Read.ListPartyToKeyMapping(
            BaseQuery(
              store,
              proposals,
              timeQuery,
              operation,
              filterSigningKey,
              protocolVersion.map(ProtocolVersion.tryCreate),
            ),
            filterParty,
          )
        )
      }

    @Help.Summary("Propose a party to key mapping")
    def propose(
        proposedMapping: PartyToKeyMapping,
        serial: RequireTypes.PositiveNumeric[Int],
        ops: TopologyChangeOp = TopologyChangeOp.Replace,
        signedBy: Option[Fingerprint] = None,
        store: TopologyStoreId = TopologyStoreId.Authorized,
        synchronize: Option[config.NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        // configurable in case of a key under a decentralized namespace
        mustFullyAuthorize: Boolean = true,
        force: ForceFlags = ForceFlags.none,
    ): SignedTopologyTransaction[TopologyChangeOp, PartyToKeyMapping] =
      runAdminCommand(
        TopologyAdminCommands.Write.Propose(
          mapping = proposedMapping,
          signedBy = signedBy.toList,
          store = store,
          change = ops,
          serial = Some(serial),
          mustFullyAuthorize = mustFullyAuthorize,
          forceChanges = force,
          waitToBecomeEffective = synchronize,
        )
      )
  }

  @Help.Summary("Manage party to participant mappings")
  @Help.Group("Party to participant mappings")
  object party_to_participant_mappings extends Helpful {

    private def findCurrent(party: PartyId, store: TopologyStoreId) =
      store match {
        case TopologyStoreId.Synchronizer(synchronizerId) =>
          expectAtMostOneResult(
            list(
              synchronizerId,
              filterParty = party.filterString,
              // fetch both REPLACE and REMOVE to correctly determine the next serial
              operation = None,
            )
          )
        case TopologyStoreId.Temporary(temp) =>
          // TODO(#20978) make it work with temporary stores
          ???

        case TopologyStoreId.Authorized =>
          expectAtMostOneResult(
            list_from_authorized(
              filterParty = party.filterString,
              // fetch both REPLACE and REMOVE to correctly determine the next serial
              operation = None,
            )
          )
      }

    @Help.Summary("Change party to participant mapping")
    @Help.Description("""Change the association of a party to hosting participants.
      party: The unique identifier of the party whose set of participants or permission to modify.
      adds: The unique identifiers of the participants to host the party each specifying the participant's permissions
            (submission, confirmation, observation). If the participant already hosts the specified party, update the
            participant's permissions.
      removes: The unique identifiers of the participants that should no longer host the party.
      signedBy: Refers to the optional fingerprint of the authorizing key which in turn refers to a specific, locally existing certificate.
      synchronize: Synchronize timeout can be used to ensure that the state has been propagated into the node
      mustFullyAuthorize: When set to true, the proposal's previously received signatures and the signature of this node must be
                          sufficient to fully authorize the topology transaction. If this is not the case, the request fails.
                          When set to false, the proposal retains the proposal status until enough signatures are accumulated to
                          satisfy the mapping's authorization requirements.
      store: - "Authorized": The topology transaction will be stored in the node's authorized store and automatically
                             propagated to connected synchronizers, if applicable.
             - "<synchronizer id>": The topology transaction will be directly submitted to the specified synchronizer without
                              storing it locally first. This also means it will _not_ be synchronized to other synchronizers
                              automatically.
      force: must be set when disabling a party with active contracts
      """)
    def propose_delta(
        party: PartyId,
        adds: Seq[(ParticipantId, ParticipantPermission)] = Nil,
        removes: Seq[ParticipantId] = Nil,
        signedBy: Option[Fingerprint] = None,
        synchronize: Option[config.NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        mustFullyAuthorize: Boolean = false,
        store: TopologyStoreId = TopologyStoreId.Authorized,
        forceFlags: ForceFlags = ForceFlags.none,
    ): SignedTopologyTransaction[TopologyChangeOp, PartyToParticipant] = {

      val currentO = findCurrent(party, store)
      val (existingPermissions, newSerial, threshold) = currentO match {
        case Some(current) if current.context.operation == TopologyChangeOp.Remove =>
          (
            // if the existing mapping was REMOVEd, we start from scratch
            Map.empty[ParticipantId, ParticipantPermission],
            Some(current.context.serial.increment),
            current.item.threshold,
          )
        case Some(current) =>
          (
            current.item.participants.map(p => p.participantId -> p.permission).toMap,
            Some(current.context.serial.increment),
            current.item.threshold,
          )
        case None =>
          (
            Map.empty[ParticipantId, ParticipantPermission],
            Some(PositiveInt.one),
            PositiveInt.one,
          )
      }

      val newPermissions = new PartyToParticipantComputations(loggerFactory)
        .computeNewPermissions(
          existingPermissions = existingPermissions,
          adds = adds,
          removes = removes,
        )
        .valueOr(err => throw new IllegalArgumentException(err))

      if (newPermissions.nonEmpty) {
        // issue a REPLACE
        propose(
          party = party,
          newParticipants = newPermissions.toSeq,
          threshold = threshold,
          signedBy = signedBy.toList,
          operation = TopologyChangeOp.Replace,
          serial = newSerial,
          synchronize = synchronize,
          mustFullyAuthorize = mustFullyAuthorize,
          store = store,
          forceFlags = forceFlags,
        )
      } else {
        // we would remove the last participant, therefore we issue a REMOVE
        // with the same mapping values as the existing serial
        propose(
          party = party,
          newParticipants = existingPermissions.toSeq,
          threshold = threshold,
          signedBy = signedBy.toList,
          operation = TopologyChangeOp.Remove,
          serial = newSerial,
          synchronize = synchronize,
          mustFullyAuthorize = mustFullyAuthorize,
          store = store,
          forceFlags = forceFlags,
        )

      }
    }

    @Help.Summary("Replace party to participant mapping")
    @Help.Description("""Replace the association of a party to hosting participants.
      party: The unique identifier of the party whose set of participant permissions to modify.
      newParticipants: The unique identifier of the participants to host the party. Each participant entry specifies
                       the participant's permissions (submission, confirmation, observation).
      threshold: The threshold is `1` for regular parties and larger than `1` for "consortium parties". The threshold
                 indicates how many participant confirmations are needed in order to confirm a Daml transaction on
                 behalf the party.
      signedBy: Refers to the optional fingerprint of the authorizing key which in turn refers to a specific, locally existing certificate.
      serial: The expected serial this topology transaction should have. Serials must be contiguous and start at 1.
              This transaction will be rejected if another fully authorized transaction with the same serial already
              exists, or if there is a gap between this serial and the most recently used serial.
              If None, the serial will be automatically selected by the node.
      operation: The operation to use. When adding a mapping or making changes, use TopologyChangeOp.Replace.
                 When removing a mapping, use TopologyChangeOp.Remove and pass the same values as the currently effective mapping.
                 The default value is TopologyChangeOp.Replace.
      synchronize: Synchronize timeout can be used to ensure that the state has been propagated into the node
      mustFullyAuthorize: When set to true, the proposal's previously received signatures and the signature of this node must be
                          sufficient to fully authorize the topology transaction. If this is not the case, the request fails.
                          When set to false, the proposal retains the proposal status until enough signatures are accumulated to
                          satisfy the mapping's authorization requirements.
      store: - "Authorized": The topology transaction will be stored in the node's authorized store and automatically
                             propagated to connected synchronizers, if applicable.
             - "<synchronizer id>": The topology transaction will be directly submitted to the specified synchronizer without
                              storing it locally first. This also means it will _not_ be synchronized to other synchronizers
                              automatically.
      """)
    def propose(
        party: PartyId,
        newParticipants: Seq[(ParticipantId, ParticipantPermission)],
        threshold: PositiveInt = PositiveInt.one,
        serial: Option[PositiveInt] = None,
        signedBy: Seq[Fingerprint] = Seq.empty,
        operation: TopologyChangeOp = TopologyChangeOp.Replace,
        synchronize: Option[config.NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        mustFullyAuthorize: Boolean = false,
        store: TopologyStoreId = TopologyStoreId.Authorized,
        forceFlags: ForceFlags = ForceFlags.none,
    ): SignedTopologyTransaction[TopologyChangeOp, PartyToParticipant] = {
      val command = TopologyAdminCommands.Write.Propose(
        mapping = PartyToParticipant.create(
          partyId = party,
          threshold = threshold,
          participants = newParticipants.map((HostingParticipant.apply _) tupled),
        ),
        signedBy = signedBy,
        serial = serial,
        change = operation,
        mustFullyAuthorize = mustFullyAuthorize,
        store = store,
        forceChanges = forceFlags,
        waitToBecomeEffective = synchronize,
      )

      runAdminCommand(command)
    }

    @Help.Summary("List party to participant mapping transactions from synchronizer store")
    @Help.Description(
      """List the party to participant mapping transactions present in the stores. Party to participant mappings
        |are topology transactions used to allocate a party to certain participants. The same party can be allocated
        |on several participants with different privileges.

        synchronizerId: Synchronizer to be considered
        proposals: Whether to query proposals instead of authorized transactions.
        timeQuery: The time query allows to customize the query by time. The following options are supported:
                   TimeQuery.HeadState (default): The most recent known state.
                   TimeQuery.Snapshot(ts): The state at a certain point in time.
                   TimeQuery.Range(fromO, toO): Time-range of when the transaction was added to the store
        operation: Optionally, what type of operation the transaction should have.
        filterParty: Filter for parties starting with the given filter string.
        filterParticipant: If non-empty, returns only parties that are hosted on this participant.
        filterSigningKey: Filter for transactions that are authorized with a key that starts with the given filter string.
        protocolVersion: Export the topology transactions in the optional protocol version.
        |"""
    )
    def list(
        synchronizerId: SynchronizerId,
        proposals: Boolean = false,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = Some(TopologyChangeOp.Replace),
        filterParty: String = "",
        filterParticipant: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListPartyToParticipantResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommands.Read.ListPartyToParticipant(
          BaseQuery(
            store = TopologyStoreId.Synchronizer(synchronizerId),
            proposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterParty,
          filterParticipant,
        )
      )
    }

    /** Check whether the node knows about `party` being hosted on `hostingParticipants` and
      * synchronizer `synchronizerId`, optionally the specified expected permission and threshold.
      * @param synchronizerId
      *   Synchronizer on which the party should be hosted
      * @param party
      *   The party which needs to be hosted
      * @param hostingParticipants
      *   Expected hosting participants
      * @param permission
      *   If specified, the expected permission
      * @param threshold
      *   If specified, the expected threshold
      */
    def is_known(
        synchronizerId: SynchronizerId,
        party: PartyId,
        hostingParticipants: Seq[ParticipantId],
        permission: Option[ParticipantPermission] = None,
        threshold: Option[PositiveInt] = None,
    ): Boolean = {

      val permissions: Map[ParticipantId, (PositiveInt, ParticipantPermission)] = list(
        synchronizerId,
        filterParty = party.toProtoPrimitive,
      )
        .map(_.item)
        .flatMap(mapping =>
          mapping.participants.map(p => p.participantId -> (mapping.threshold, p.permission))
        )
        .toMap

      val participantsError = hostingParticipants.mapFilter { hostingParticipant =>
        (permissions.get(hostingParticipant), threshold, permission) match {
          case (None, _, _) => Some(s"for $hostingParticipant: not hosted")
          case (Some((foundThreshold, _)), Some(threshold), _) if threshold != foundThreshold =>
            Some(
              s"for $hostingParticipant: expected threshold $threshold but found $foundThreshold"
            )
          case (Some((_, foundPermission)), _, Some(permission)) if permission != foundPermission =>
            Some(
              s"for $hostingParticipant: expected permission $permission but found $foundPermission"
            )
          case _ => None // All good
        }
      }

      val result = if (participantsError.isEmpty) "yes" else participantsError.mkString(", ")

      logger.debug(
        s"Checking whether node knows $party being hosted on $hostingParticipants with threshold=$threshold and permission=$permission: $result"
      )(TraceContext.empty)

      participantsError.isEmpty
    }

    /** Check whether the node knows about `parties` being hosted on `hostingParticipants` and
      * synchronizer `synchronizerId`.
      * @param synchronizerId
      *   Synchronizer on which the party should be hosted
      * @param parties
      *   The parties which needs to be hosted
      * @param hostingParticipants
      *   Expected hosting participants
      */
    def are_known(
        synchronizerId: SynchronizerId,
        parties: Seq[PartyId],
        hostingParticipants: Seq[ParticipantId],
    ): Boolean = {
      val partyToParticipants: Map[PartyId, Seq[ParticipantId]] = list(synchronizerId)
        .map(_.item)
        .map(mapping => mapping.partyId -> mapping.participants.map(p => p.participantId))
        .toMap

      parties.forall { party =>
        val missingParticipants =
          hostingParticipants.diff(partyToParticipants.getOrElse(party, Seq.empty))

        if (missingParticipants.isEmpty)
          logger.debug(
            s"Node knows about $party being hosted on $hostingParticipants"
          )(TraceContext.empty)
        else
          logger.debug(
            s"Node knows that $party is hosted on ${hostingParticipants.diff(missingParticipants)} but not on $missingParticipants"
          )(TraceContext.empty)

        missingParticipants.isEmpty
      }
    }

    @Help.Summary("List party to participant mapping transactions from the authorized store")
    @Help.Description(
      """List the party to participant mapping transactions present in the stores. Party to participant mappings
        |are topology transactions used to allocate a party to certain participants. The same party can be allocated
        |on several participants with different privileges.

        proposals: Whether to query proposals instead of authorized transactions.
        timeQuery: The time query allows to customize the query by time. The following options are supported:
                   TimeQuery.HeadState (default): The most recent known state.
                   TimeQuery.Snapshot(ts): The state at a certain point in time.
                   TimeQuery.Range(fromO, toO): Time-range of when the transaction was added to the store
        operation: Optionally, what type of operation the transaction should have.
        filterParty: Filter for parties starting with the given filter string.
        filterParticipant: Filter for participants starting with the given filter string.
        filterSigningKey: Filter for transactions that are authorized with a key that starts with the given filter string.
        protocolVersion: Export the topology transactions in the optional protocol version.
        |"""
    )
    def list_from_authorized(
        proposals: Boolean = false,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = Some(TopologyChangeOp.Replace),
        filterParty: String = "",
        filterParticipant: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListPartyToParticipantResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommands.Read.ListPartyToParticipant(
          BaseQuery(
            store = TopologyStoreId.Authorized,
            proposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterParty,
          filterParticipant,
        )
      )
    }

    @Help.Summary("List party to participant mapping transactions from all stores")
    @Help.Description(
      """List the party to participant mapping transactions present in the stores. Party to participant mappings
        |are topology transactions used to allocate a party to certain participants. The same party can be allocated
        |on several participants with different privileges.

        proposals: Whether to query proposals instead of authorized transactions.
        timeQuery: The time query allows to customize the query by time. The following options are supported:
                   TimeQuery.HeadState (default): The most recent known state.
                   TimeQuery.Snapshot(ts): The state at a certain point in time.
                   TimeQuery.Range(fromO, toO): Time-range of when the transaction was added to the store
        operation: Optionally, what type of operation the transaction should have.
        filterParty: Filter for parties starting with the given filter string.
        filterParticipant: Filter for participants starting with the given filter string.
        filterSigningKey: Filter for transactions that are authorized with a key that starts with the given filter string.
        protocolVersion: Export the topology transactions in the optional protocol version.
        |"""
    )
    def list_from_all(
        proposals: Boolean = false,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = Some(TopologyChangeOp.Replace),
        filterParty: String = "",
        filterParticipant: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListPartyToParticipantResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommands.Read.ListPartyToParticipant(
          BaseQuery(
            store = None,
            proposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterParty,
          filterParticipant,
        )
      )
    }
  }

  @Help.Summary("Manage synchronizer trust certificates")
  @Help.Group("Synchronizer trust certificates")
  object synchronizer_trust_certificates extends Helpful {
    def list(
        store: Option[TopologyStoreId] = None,
        proposals: Boolean = false,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = Some(TopologyChangeOp.Replace),
        filterUid: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListSynchronizerTrustCertificateResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommands.Read.ListSynchronizerTrustCertificate(
          BaseQuery(
            store,
            proposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterUid,
        )
      )
    }

    // TODO(#14057) document console command
    def active(synchronizerId: SynchronizerId, participantId: ParticipantId): Boolean =
      list(
        store = Some(TopologyStoreId.Synchronizer(synchronizerId)),
        filterUid = participantId.filterString,
        operation = Some(TopologyChangeOp.Replace),
      ).exists { x =>
        x.item.synchronizerId == synchronizerId && x.item.participantId == participantId
      }

    @Help.Summary("Propose a change to a participant's synchronizer trust certificate.")
    @Help.Description(
      """A participant's synchronizer trust certificate signals to the synchronizer that the participant would like to act on the synchronizer.

        participantId: the identifier of the trust certificate's target participant
        synchronizerId: the identifier of the synchronizer on which the participant would like to act

        store: - "Authorized": the topology transaction will be stored in the node's authorized store and automatically
                               propagated to connected synchronizers, if applicable.
               - "<synchronizer id>": the topology transaction will be directly submitted to the specified synchronizer without
                                storing it locally first. This also means it will _not_ be synchronized to other synchronizers
                                automatically.
        mustFullyAuthorize: when set to true, the proposal's previously received signatures and the signature of this node must be
                            sufficient to fully authorize the topology transaction. if this is not the case, the request fails.
                            when set to false, the proposal retains the proposal status until enough signatures are accumulated to
                            satisfy the mapping's authorization requirements.
        serial: the expected serial this topology transaction should have. Serials must be contiguous and start at 1.
                This transaction will be rejected if another fully authorized transaction with the same serial already
                exists, or if there is a gap between this serial and the most recently used serial.
                If None, the serial will be automatically selected by the node.
        """
    )
    def propose(
        participantId: ParticipantId,
        synchronizerId: SynchronizerId,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        // Using the authorized store by default, because the trust cert upon connecting to a synchronizer is also stored in the authorized store
        store: Option[TopologyStoreId] = Some(TopologyStoreId.Authorized),
        mustFullyAuthorize: Boolean = true,
        serial: Option[PositiveInt] = None,
        change: TopologyChangeOp = TopologyChangeOp.Replace,
    ): SignedTopologyTransaction[TopologyChangeOp, SynchronizerTrustCertificate] = {
      val cmd = TopologyAdminCommands.Write.Propose(
        mapping = SynchronizerTrustCertificate(
          participantId,
          synchronizerId,
        ),
        signedBy = Seq.empty,
        store = store.getOrElse(TopologyStoreId.Synchronizer(synchronizerId)),
        serial = serial,
        mustFullyAuthorize = mustFullyAuthorize,
        change = change,
        waitToBecomeEffective = synchronize,
      )
      runAdminCommand(cmd)
    }

  }

  @Help.Summary("Inspect participant synchronizer permissions")
  @Help.Group("Participant Synchronizer Permissions")
  object participant_synchronizer_permissions extends Helpful {
    @Help.Summary("Propose changes to the synchronizer permissions of participants.")
    @Help.Description(
      """Synchronizer operators may use this command to change a participant's permissions on a synchronizer.

        synchronizerId: the target synchronizer
        participantId: the participant whose permissions should be changed
        permission: the participant's permission
        loginAfter: the earliest time a participant may connect to the synchronizer
        limits: synchronizer limits for this participant

        store: - "Authorized": the topology transaction will be stored in the node's authorized store and automatically
                               propagated to connected synchronizers, if applicable.
               - "<synchronizer id>": the topology transaction will be directly submitted to the specified synchronizer without
                                storing it locally first. This also means it will _not_ be synchronized to other synchronizers
                                automatically.
        mustFullyAuthorize: when set to true, the proposal's previously received signatures and the signature of this node must be
                            sufficient to fully authorize the topology transaction. if this is not the case, the request fails.
                            when set to false, the proposal retains the proposal status until enough signatures are accumulated to
                            satisfy the mapping's authorization requirements.
        serial: the expected serial this topology transaction should have. Serials must be contiguous and start at 1.
                This transaction will be rejected if another fully authorized transaction with the same serial already
                exists, or if there is a gap between this serial and the most recently used serial.
                If None, the serial will be automatically selected by the node."""
    )
    def propose(
        synchronizerId: SynchronizerId,
        participantId: ParticipantId,
        permission: ParticipantPermission,
        loginAfter: Option[CantonTimestamp] = None,
        limits: Option[ParticipantSynchronizerLimits] = None,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        store: Option[TopologyStoreId] = None,
        mustFullyAuthorize: Boolean = false,
        serial: Option[PositiveInt] = None,
        change: TopologyChangeOp = TopologyChangeOp.Replace,
    ): SignedTopologyTransaction[TopologyChangeOp, ParticipantSynchronizerPermission] = {
      val cmd = TopologyAdminCommands.Write.Propose(
        mapping = ParticipantSynchronizerPermission(
          synchronizerId = synchronizerId,
          participantId = participantId,
          permission = permission,
          limits = limits,
          loginAfter = loginAfter,
        ),
        signedBy = Seq.empty,
        serial = serial,
        store = store.getOrElse(TopologyStoreId.Synchronizer(synchronizerId)),
        mustFullyAuthorize = mustFullyAuthorize,
        change = change,
        waitToBecomeEffective = synchronize,
      )

      runAdminCommand(cmd)
    }

    @Help.Summary("Revokes the synchronizer permissions of a participant.")
    @Help.Description(
      """Synchronizer operators may use this command to revoke a participant's permissions on a synchronizer.

        synchronizerId: the target synchronizer
        participantId: the participant whose permissions should be revoked

        store: - "Authorized": the topology transaction will be stored in the node's authorized store and automatically
                               propagated to connected synchronizers, if applicable.
               - "<synchronizer id>": the topology transaction will be directly submitted to the specified synchronizer without
                                storing it locally first. This also means it will _not_ be synchronized to other synchronizers
                                automatically.
        mustFullyAuthorize: when set to true, the proposal's previously received signatures and the signature of this node must be
                            sufficient to fully authorize the topology transaction. if this is not the case, the request fails.
                            when set to false, the proposal retains the proposal status until enough signatures are accumulated to
                            satisfy the mapping's authorization requirements."""
    )
    def revoke(
        synchronizerId: SynchronizerId,
        participantId: ParticipantId,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        mustFullyAuthorize: Boolean = false,
        store: Option[TopologyStoreId] = None,
    ): SignedTopologyTransaction[TopologyChangeOp, ParticipantSynchronizerPermission] =
      list(
        store = store.getOrElse(TopologyStoreId.Synchronizer(synchronizerId)),
        filterUid = participantId.filterString,
      ) match {
        case Seq() =>
          throw new IllegalStateException(
            s"No ParticipantSynchronizerPermission found for participant $participantId."
          ) with NoStackTrace
        case Seq(result) =>
          val item = result.item
          propose(
            item.synchronizerId,
            item.participantId,
            item.permission,
            item.loginAfter,
            item.limits,
            synchronize,
            store = store,
            serial = Some(result.context.serial.increment),
            mustFullyAuthorize = mustFullyAuthorize,
            change = TopologyChangeOp.Remove,
          )
        case otherwise =>
          throw new IllegalStateException(
            s"Found more than one ParticipantSynchronizerPermission for participant $participantId on synchronizer $synchronizerId"
          ) with NoStackTrace
      }

    def list(
        store: TopologyStoreId,
        proposals: Boolean = false,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = Some(TopologyChangeOp.Replace),
        filterUid: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListParticipantSynchronizerPermissionResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommands.Read.ListParticipantSynchronizerPermission(
          BaseQuery(
            store,
            proposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterUid,
        )
      )
    }

    @Help.Summary("Looks up the participant permission for a participant on a synchronizer")
    @Help.Description("""Returns the optional participant synchronizer permission.""")
    def find(
        synchronizerId: SynchronizerId,
        participantId: ParticipantId,
    ): Option[ListParticipantSynchronizerPermissionResult] =
      expectAtMostOneResult(
        list(
          store = TopologyStoreId.Synchronizer(synchronizerId),
          filterUid = participantId.filterString,
        )
      ).filter(p =>
        p.item.participantId == participantId && p.item.synchronizerId == synchronizerId
      )
  }

  @Help.Summary("Inspect participant synchronizer states")
  @Help.Group("Participant Synchronizer States")
  object participant_synchronizer_states extends Helpful {
    @Help.Summary(
      "Returns true if the given participant is currently active on the given synchronizer"
    )
    @Help.Description(
      """Active means that the participant has been granted at least observation rights on the synchronizer
         |and that the participant has registered a synchronizer trust certificate"""
    )
    def active(synchronizerId: SynchronizerId, participantId: ParticipantId): Boolean = {
      lazy val participantHasPermission = participant_synchronizer_permissions
        .find(synchronizerId, participantId)
        .exists(res => res.context.operation == Replace)
      val dynParams = synchronizer_parameters.get_dynamic_synchronizer_parameters(synchronizerId)
      synchronizer_trust_certificates.active(
        synchronizerId,
        participantId,
      ) && (dynParams.onboardingRestriction.isOpen || participantHasPermission)
    }
  }

  @Help.Summary("Manage party hosting limits")
  @Help.Group("Party hosting limits")
  object party_hosting_limits extends Helpful {
    def list(
        store: Option[TopologyStoreId] = None,
        proposals: Boolean = false,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = Some(TopologyChangeOp.Replace),
        filterUid: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListPartyHostingLimitsResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommands.Read.ListPartyHostingLimits(
          BaseQuery(
            store,
            proposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterUid,
        )
      )
    }

    // When we removed the field maxNumHostingParticipants from the PartyHostingLimits, this method did not make sense anymore.
    // We keep it here for now, because it's already implemented and might be useful in the future.
    // Look at the history if you need the summary and description of this method.
    def propose(
        synchronizerId: SynchronizerId,
        partyId: PartyId,
        store: Option[TopologyStoreId] = None,
        mustFullyAuthorize: Boolean = false,
        signedBy: Seq[Fingerprint] = Seq.empty,
        serial: Option[PositiveInt] = None,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): SignedTopologyTransaction[TopologyChangeOp, PartyHostingLimits] =
      runAdminCommand(
        TopologyAdminCommands.Write.Propose(
          PartyHostingLimits(synchronizerId, partyId),
          signedBy = signedBy,
          store = store.getOrElse(TopologyStoreId.Synchronizer(synchronizerId)),
          serial = serial,
          change = TopologyChangeOp.Replace,
          mustFullyAuthorize = mustFullyAuthorize,
          waitToBecomeEffective = synchronize,
        )
      )
  }

  @Help.Summary("Manage package vettings")
  @Help.Group("Vetted Packages")
  object vetted_packages extends Helpful {

    @Help.Summary("Change package vettings")
    @Help.Description(
      """A participant will only process transactions that reference packages that all involved participants have
         |vetted previously. Vetting is done by registering a respective topology transaction with the synchronizer,
         |which can then be used by other participants to verify that a transaction is only using
         |vetted packages.
         |Note that all referenced and dependent packages must exist in the package store.

         participantId: the identifier of the participant vetting the packages
         adds: The lf-package ids to be vetted.
         removes: The lf-package ids to be unvetted.
         store: - "Authorized": the topology transaction will be stored in the node's authorized store and automatically
                             propagated to connected synchronizers, if applicable.
                - "<synchronizer id>": the topology transaction will be directly submitted to the specified synchronizer without
                              storing it locally first. This also means it will _not_ be synchronized to other synchronizers
                              automatically.
         filterParticipant: Filter for participants starting with the given filter string.
         mustFullyAuthorize: when set to true, the proposal's previously received signatures and the signature of this node must be
                          sufficient to fully authorize the topology transaction. if this is not the case, the request fails.
                          when set to false, the proposal retains the proposal status until enough signatures are accumulated to
                          satisfy the mapping's authorization requirements.
         signedBy: the fingerprint of the key to be used to sign this proposal
         force: must be set when revoking the vetting of packagesIds
         |"""
    )
    def propose_delta(
        participant: ParticipantId,
        adds: Seq[VettedPackage] = Nil,
        removes: Seq[PackageId] = Nil,
        store: TopologyStoreId = TopologyStoreId.Authorized,
        mustFullyAuthorize: Boolean = false,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        signedBy: Option[Fingerprint] = None,
        force: ForceFlags = ForceFlags.none,
    ): Unit = {

      val duplicatePackageIds = adds.map(_.packageId).intersect(removes)
      if (duplicatePackageIds.nonEmpty) {
        throw new IllegalArgumentException(
          s"Cannot both add and remove a packageId: $duplicatePackageIds"
        ) with NoStackTrace
      }
      // compute the diff and then call the propose method
      val current0 = expectAtMostOneResult(
        list(
          store = Some(store),
          filterParticipant = participant.filterString,
          operation = None,
        )
      )

      (adds, removes) match {
        case (Nil, Nil) =>
          throw new IllegalArgumentException(
            "Ensure that at least one of the two parameters (adds or removes) is not empty."
          ) with NoStackTrace
        case (_, _) =>
          val allChangedPackageIds = (adds.map(_.packageId) ++ removes).toSet

          val (newSerial, newDiffPackageIds) = current0 match {
            case Some(
                  ListVettedPackagesResult(
                    BaseResult(_, _, _, _, TopologyChangeOp.Replace, _, serial, _),
                    item,
                  )
                ) =>
              (
                serial.increment,
                // first filter out all existing packages that either get re-added (i.e. modified) or removed
                item.packages.filter(vp => !allChangedPackageIds.contains(vp.packageId))
                // now we can add all the adds the also haven't been in the remove set
                  ++ adds,
              )
            case Some(
                  ListVettedPackagesResult(
                    BaseResult(_, _, _, _, TopologyChangeOp.Remove, _, serial, _),
                    _,
                  )
                ) =>
              (serial.increment, adds)
            case None =>
              (PositiveInt.one, adds)
          }

          if (current0.exists(_.item.packages.toSet == newDiffPackageIds.toSet))
            () // means no change
          else
            propose(
              participant = participant,
              packages = newDiffPackageIds,
              store,
              mustFullyAuthorize,
              synchronize,
              Some(newSerial),
              signedBy,
              force,
            )
      }
    }
    @Help.Summary("Replace package vettings")
    @Help.Description("""A participant will only process transactions that reference packages that all involved participants have
        |vetted previously. Vetting is done by registering a respective topology transaction with the synchronizer,
        |which can then be used by other participants to verify that a transaction is only using
        |vetted packages.
        |Note that all referenced and dependent packages must exist in the package store.

        participantId: the identifier of the participant vetting the packages
        packages: The lf-package ids with validity boundaries to be vetted that will replace the previous vetted packages.
        store: - "Authorized": the topology transaction will be stored in the node's authorized store and automatically
                              propagated to connected synchronizers, if applicable.
               - "<synchronizer id>": the topology transaction will be directly submitted to the specified synchronizer without
                              storing it locally first. This also means it will _not_ be synchronized to other synchronizers automatically.
        mustFullyAuthorize: when set to true, the proposal's previously received signatures and the signature of this node must be
                              sufficient to fully authorize the topology transaction. if this is not the case, the request fails.
                              when set to false, the proposal retains the proposal status until enough signatures are accumulated to
                              satisfy the mapping's authorization requirements.
        serial: ted serial this topology transaction should have. Serials must be contiguous and start at 1.
                    This transaction will be rejected if another fully authorized transaction with the same serial already
                    exists, or if there is a gap between this serial and the most recently used serial.
                    If None, the serial will be automatically selected by the node.
        signedBy: the fingerprint of the key to be used to sign this proposal
        force: must be set when revoking the vetting of packagesIds""")
    def propose(
        participant: ParticipantId,
        packages: Seq[VettedPackage],
        store: TopologyStoreId = TopologyStoreId.Authorized,
        mustFullyAuthorize: Boolean = false,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        serial: Option[PositiveInt] = None,
        signedBy: Option[Fingerprint] = None,
        force: ForceFlags = ForceFlags.none,
    ): Unit = {

      val topologyChangeOp =
        if (packages.isEmpty) TopologyChangeOp.Remove else TopologyChangeOp.Replace

      val command = TopologyAdminCommands.Write.Propose(
        mapping = VettedPackages.create(
          participantId = participant,
          packages = packages,
        ),
        signedBy = signedBy.toList,
        serial = serial,
        change = topologyChangeOp,
        mustFullyAuthorize = mustFullyAuthorize,
        store = store,
        forceChanges = force,
        waitToBecomeEffective = synchronize,
      )

      runAdminCommand(command).discard
    }

    def list(
        store: Option[TopologyStoreId] = None,
        proposals: Boolean = false,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = Some(TopologyChangeOp.Replace),
        filterParticipant: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListVettedPackagesResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommands.Read.ListVettedPackages(
          BaseQuery(
            store,
            proposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterParticipant,
        )
      )
    }
  }

  @Help.Summary("Inspect mediator synchronizer state")
  @Help.Group("Mediator Synchronizer State")
  object mediators extends Helpful {
    def list(
        synchronizerId: Option[SynchronizerId] = None,
        proposals: Boolean = false,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = Some(TopologyChangeOp.Replace),
        filterSynchronizer: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
        group: Option[NonNegativeInt] = None,
    ): Seq[ListMediatorSynchronizerStateResult] = {
      def predicate(res: ListMediatorSynchronizerStateResult): Boolean =
        group.forall(_ == res.item.group)

      consoleEnvironment
        .run {
          adminCommand(
            TopologyAdminCommands.Read.MediatorSynchronizerState(
              BaseQuery(
                synchronizerId,
                proposals,
                timeQuery,
                operation,
                filterSigningKey,
                protocolVersion.map(ProtocolVersion.tryCreate),
              ),
              filterSynchronizer,
            )
          )
        }
        .filter(predicate)
    }

    @Help.Summary("Propose changes to the mediator topology")
    @Help.Description(
      """
     synchronizerId: the target synchronizer
     group: the mediator group identifier
     adds: The unique identifiers of the active mediators to add.
     removes: The unique identifiers of the mediators that should no longer be active mediators.
     observerAdds: The unique identifiers of the observer mediators to add.
     observerRemoves: The unique identifiers of the mediators that should no longer be observer mediators.
     updateThreshold: Optionally an updated value for the threshold of the mediator group.
     await: optional timeout to wait for the proposal to be persisted in the specified topology store
     mustFullyAuthorize: when set to true, the proposal's previously received signatures and the signature of this node must be
                         sufficient to fully authorize the topology transaction. if this is not the case, the request fails.
                         when set to false, the proposal retains the proposal status until enough signatures are accumulated to
                         satisfy the mapping's authorization requirements.
     signedBy: the fingerprint of the key to be used to sign this proposal"""
    )
    def propose_delta(
        synchronizerId: SynchronizerId,
        group: NonNegativeInt,
        adds: List[MediatorId] = Nil,
        removes: List[MediatorId] = Nil,
        observerAdds: List[MediatorId] = Nil,
        observerRemoves: List[MediatorId] = Nil,
        updateThreshold: Option[PositiveInt] = None,
        synchronize: Option[config.NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        mustFullyAuthorize: Boolean = false,
        signedBy: Option[Fingerprint] = None,
    ): Unit = {

      MediatorGroupDeltaComputations
        .verifyProposalConsistency(adds, removes, observerAdds, observerRemoves, updateThreshold)
        .valueOr(err => throw new IllegalArgumentException(err))

      def queryStore(proposals: Boolean): Option[(PositiveInt, MediatorSynchronizerState)] =
        expectAtMostOneResult(
          list(
            Some(synchronizerId),
            group = Some(group),
            operation = Some(TopologyChangeOp.Replace),
            proposals = proposals,
          )
        ).map(result => (result.context.serial, result.item))

      val maybeSerialAndMediatorSynchronizerState = queryStore(proposals = false)

      MediatorGroupDeltaComputations
        .verifyProposalAgainstCurrentState(
          maybeSerialAndMediatorSynchronizerState.map(_._2),
          adds,
          removes,
          observerAdds,
          observerRemoves,
          updateThreshold,
        )
        .valueOr(err => throw new IllegalArgumentException(err))

      val (serial, threshold, active, observers) = maybeSerialAndMediatorSynchronizerState match {
        case Some((currentSerial, mds)) =>
          (
            currentSerial.increment,
            mds.threshold,
            mds.active.forgetNE.concat(adds).diff(removes),
            mds.observers.concat(observerAdds).diff(observerRemoves),
          )
        case None =>
          (PositiveInt.one, PositiveInt.one, adds, observerAdds)
      }

      propose(
        synchronizerId,
        updateThreshold.getOrElse(threshold),
        active,
        observers,
        group,
        store = None,
        synchronize = synchronize,
        mustFullyAuthorize = mustFullyAuthorize,
        signedBy = signedBy,
        serial = Some(serial),
      ).discard
    }

    @Help.Summary("Replace the mediator topology")
    @Help.Description("""
         synchronizerId: the target synchronizer
         threshold: the minimum number of mediators that need to come to a consensus for a message to be sent to other members.
         active: the list of mediators that will take part in the mediator consensus in this mediator group
         passive: the mediators that will receive all messages but will not participate in mediator consensus
         group: the mediator group identifier
         store: - "Authorized": the topology transaction will be stored in the node's authorized store and automatically
                                propagated to connected synchronizers, if applicable.
                - "<synchronizer id>": the topology transaction will be directly submitted to the specified synchronizer without
                                 storing it locally first. This also means it will _not_ be synchronized to other synchronizers
                                 automatically.
         mustFullyAuthorize: when set to true, the proposal's previously received signatures and the signature of this node must be
                             sufficient to fully authorize the topology transaction. if this is not the case, the request fails.
                             when set to false, the proposal retains the proposal status until enough signatures are accumulated to
                             satisfy the mapping's authorization requirements.
         signedBy: the fingerprint of the key to be used to sign this proposal
         serial: the expected serial this topology transaction should have. Serials must be contiguous and start at 1.
                 This transaction will be rejected if another fully authorized transaction with the same serial already
                 exists, or if there is a gap between this serial and the most recently used serial.
                 If None, the serial will be automatically selected by the node.""")
    def propose(
        synchronizerId: SynchronizerId,
        threshold: PositiveInt,
        active: Seq[MediatorId],
        observers: Seq[MediatorId] = Seq.empty,
        group: NonNegativeInt,
        store: Option[TopologyStoreId] = None,
        synchronize: Option[config.NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        mustFullyAuthorize: Boolean = false,
        signedBy: Option[Fingerprint] = None,
        serial: Option[PositiveInt] = None,
    ): SignedTopologyTransaction[TopologyChangeOp, MediatorSynchronizerState] = {
      val command = TopologyAdminCommands.Write.Propose(
        mapping = MediatorSynchronizerState
          .create(synchronizerId, group, threshold, active, observers),
        signedBy = signedBy.toList,
        serial = serial,
        change = TopologyChangeOp.Replace,
        mustFullyAuthorize = mustFullyAuthorize,
        forceChanges = ForceFlags.none,
        store = store.getOrElse(TopologyStoreId.Synchronizer(synchronizerId)),
        waitToBecomeEffective = synchronize,
      )

      runAdminCommand(command)
    }

    @Help.Summary("Propose to remove a mediator group")
    @Help.Description("""
         synchronizerId: the target synchronizer
         group: the mediator group identifier

         store: - "Authorized": the topology transaction will be stored in the node's authorized store and automatically
                                propagated to connected synchronizers, if applicable.
                - "<synchronizer id>": the topology transaction will be directly submitted to the specified synchronizer without
                                 storing it locally first. This also means it will _not_ be synchronized to other synchronizers
                                 automatically.
         mustFullyAuthorize: when set to true, the proposal's previously received signatures and the signature of this node must be
                             sufficient to fully authorize the topology transaction. if this is not the case, the request fails.
                             when set to false, the proposal retains the proposal status until enough signatures are accumulated to
                             satisfy the mapping's authorization requirements.""")
    def remove_group(
        synchronizerId: SynchronizerId,
        group: NonNegativeInt,
        store: Option[TopologyStoreId] = None,
        synchronize: Option[config.NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        mustFullyAuthorize: Boolean = false,
    ): SignedTopologyTransaction[TopologyChangeOp, MediatorSynchronizerState] = {

      val mediatorStateResult = list(synchronizerId = synchronizerId, group = Some(group))
        .maxByOption(_.context.serial)
        .getOrElse(throw new IllegalArgumentException(s"Unknown mediator group $group"))

      val command = TopologyAdminCommands.Write.Propose(
        mapping = mediatorStateResult.item,
        signedBy = Seq.empty,
        serial = Some(mediatorStateResult.context.serial.increment),
        change = TopologyChangeOp.Remove,
        mustFullyAuthorize = mustFullyAuthorize,
        forceChanges = ForceFlags.none,
        store = store.getOrElse(TopologyStoreId.Synchronizer(synchronizerId)),
        waitToBecomeEffective = synchronize,
      )

      runAdminCommand(command)
    }
  }

  @Help.Summary("Inspect sequencer synchronizer state")
  @Help.Group("Sequencer Synchronizer State")
  object sequencers extends Helpful {
    def list(
        store: Option[TopologyStoreId] = None,
        proposals: Boolean = false,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = Some(TopologyChangeOp.Replace),
        filterSynchronizer: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListSequencerSynchronizerStateResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommands.Read.SequencerSynchronizerState(
          BaseQuery(
            store,
            proposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterSynchronizer,
        )
      )
    }

    @Help.Summary("Propose changes to the sequencer topology")
    @Help.Description(
      """
         synchronizerId: the target synchronizer
         active: the list of active sequencers
         passive: sequencers that receive messages but are not available for members to connect to

         store: - "Authorized": the topology transaction will be stored in the node's authorized store and automatically
                                propagated to connected synchronizers, if applicable.
                - "<synchronizer id>": the topology transaction will be directly submitted to the specified synchronizer without
                                 storing it locally first. This also means it will _not_ be synchronized to other synchronizers
                                 automatically.
         mustFullyAuthorize: when set to true, the proposal's previously received signatures and the signature of this node must be
                             sufficient to fully authorize the topology transaction. if this is not the case, the request fails.
                             when set to false, the proposal retains the proposal status until enough signatures are accumulated to
                             satisfy the mapping's authorization requirements.
         signedBy: the fingerprint of the key to be used to sign this proposal
         serial: the expected serial this topology transaction should have. Serials must be contiguous and start at 1.
                 This transaction will be rejected if another fully authorized transaction with the same serial already
                 exists, or if there is a gap between this serial and the most recently used serial.
                 If None, the serial will be automatically selected by the node.
         synchronize: Synchronization timeout to wait until the proposal has been observed on the synchronizer."""
    )
    def propose(
        synchronizerId: SynchronizerId,
        threshold: PositiveInt,
        active: Seq[SequencerId],
        passive: Seq[SequencerId] = Seq.empty,
        store: Option[TopologyStoreId] = None,
        mustFullyAuthorize: Boolean = false,
        signedBy: Option[Fingerprint] = None,
        serial: Option[PositiveInt] = None,
        synchronize: Option[config.NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.unbounded
        ),
    ): SignedTopologyTransaction[TopologyChangeOp, SequencerSynchronizerState] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Write.Propose(
            mapping = SequencerSynchronizerState.create(synchronizerId, threshold, active, passive),
            signedBy = signedBy.toList,
            serial = serial,
            change = TopologyChangeOp.Replace,
            mustFullyAuthorize = mustFullyAuthorize,
            forceChanges = ForceFlags.none,
            store = store.getOrElse(TopologyStoreId.Synchronizer(synchronizerId)),
            waitToBecomeEffective = synchronize,
          )
        )
      }
  }

  @Help.Summary("Manage synchronizer parameters state", FeatureFlag.Preview)
  @Help.Group("Synchronizer Parameters State")
  object synchronizer_parameters extends Helpful {
    @Help.Summary("List dynamic synchronizer parameters")
    def list(
        store: TopologyStoreId,
        proposals: Boolean = false,
        timeQuery: TimeQuery = TimeQuery.HeadState,
        operation: Option[TopologyChangeOp] = Some(TopologyChangeOp.Replace),
        filterSynchronizer: String = "",
        filterSigningKey: String = "",
        protocolVersion: Option[String] = None,
    ): Seq[ListSynchronizerParametersStateResult] = consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommands.Read.SynchronizerParametersState(
          BaseQuery(
            store,
            proposals,
            timeQuery,
            operation,
            filterSigningKey,
            protocolVersion.map(ProtocolVersion.tryCreate),
          ),
          filterSynchronizer,
        )
      )
    }

    @Help.Summary("Get the configured dynamic synchronizer parameters")
    def get_dynamic_synchronizer_parameters(
        synchronizerId: SynchronizerId
    ): ConsoleDynamicSynchronizerParameters =
      ConsoleDynamicSynchronizerParameters(
        expectExactlyOneResult(
          list(
            store = TopologyStoreId.Synchronizer(synchronizerId),
            proposals = false,
            timeQuery = TimeQuery.HeadState,
            operation = Some(TopologyChangeOp.Replace),
            filterSynchronizer = synchronizerId.filterString,
          )
        ).item
      )

    @Help.Summary("Propose changes to dynamic synchronizer parameters")
    @Help.Description(
      """
       synchronizerId: the target synchronizer
       parameters: the new dynamic synchronizer parameters to be used on the synchronizer

       store: - "Authorized": the topology transaction will be stored in the node's authorized store and automatically
                              propagated to connected synchronizers, if applicable.
              - "<synchronizer id>": the topology transaction will be directly submitted to the specified synchronizer without
                               storing it locally first. This also means it will _not_ be synchronized to other synchronizers
                               automatically.
       mustFullyAuthorize: when set to true, the proposal's previously received signatures and the signature of this node must be
                           sufficient to fully authorize the topology transaction. if this is not the case, the request fails.
                           when set to false, the proposal retains the proposal status until enough signatures are accumulated to
                           satisfy the mapping's authorization requirements.
       signedBy: the fingerprint of the key to be used to sign this proposal
       serial: the expected serial this topology transaction should have. Serials must be contiguous and start at 1.
               This transaction will be rejected if another fully authorized transaction with the same serial already
               exists, or if there is a gap between this serial and the most recently used serial.
               If None, the serial will be automatically selected by the node.
       synchronize: Synchronize timeout can be used to ensure that the state has been propagated into the node
       waitForParticipants: if synchronize is defined, the command will also wait until parameters have been propagated
                            to the listed participants
       force: must be set to true when performing a dangerous operation, such as increasing the submissionTimeRecordTimeTolerance"""
    )
    def propose(
        synchronizerId: SynchronizerId,
        parameters: ConsoleDynamicSynchronizerParameters,
        store: Option[TopologyStoreId] = None,
        mustFullyAuthorize: Boolean = false,
        signedBy: Option[Fingerprint] = None,
        serial: Option[PositiveInt] = None,
        synchronize: Option[config.NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        force: ForceFlags = ForceFlags.none,
    ): SignedTopologyTransaction[TopologyChangeOp, SynchronizerParametersState] = { // TODO(#15815): Don't expose internal TopologyMapping and TopologyChangeOp classes

      val parametersInternal =
        parameters.toInternal.valueOr(err => throw new IllegalArgumentException(err))

      runAdminCommand(
        TopologyAdminCommands.Write.Propose(
          SynchronizerParametersState(
            synchronizerId,
            parametersInternal,
          ),
          signedBy.toList,
          serial = serial,
          mustFullyAuthorize = mustFullyAuthorize,
          store = store.getOrElse(TopologyStoreId.Synchronizer(synchronizerId)),
          forceChanges = force,
          waitToBecomeEffective = synchronize,
        )
      )
    }

    @Help.Summary("Propose an update to dynamic synchronizer parameters")
    @Help.Description(
      """
       synchronizerId: the target synchronizer
       update: the new dynamic synchronizer parameters to be used on the synchronizer
       mustFullyAuthorize: when set to true, the proposal's previously received signatures and the signature of this node must be
                           sufficient to fully authorize the topology transaction. if this is not the case, the request fails.
                           when set to false, the proposal retains the proposal status until enough signatures are accumulated to
                           satisfy the mapping's authorization requirements.
       signedBy: the fingerprint of the key to be used to sign this proposal
       synchronize: Synchronize timeout can be used to ensure that the state has been propagated into the node
       waitForParticipants: if synchronize is defined, the command will also wait until the update has been propagated
                            to the listed participants
       force: must be set to true when performing a dangerous operation, such as increasing the submissionTimeRecordTimeTolerance"""
    )
    def propose_update(
        synchronizerId: SynchronizerId,
        update: ConsoleDynamicSynchronizerParameters => ConsoleDynamicSynchronizerParameters,
        mustFullyAuthorize: Boolean = false,
        signedBy: Option[Fingerprint] = None,
        synchronize: Option[config.NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
        force: ForceFlags = ForceFlags.none,
    ): Unit = {
      val synchronizerStore = TopologyStoreId.Synchronizer(synchronizerId)
      val previousParameters = expectExactlyOneResult(
        list(
          filterSynchronizer = synchronizerId.filterString,
          store = synchronizerStore,
          operation = Some(TopologyChangeOp.Replace),
        )
      )
      val newParameters = update(ConsoleDynamicSynchronizerParameters(previousParameters.item))

      // Avoid topology manager ALREADY_EXISTS error by not submitting a no-op proposal.
      if (ConsoleDynamicSynchronizerParameters(previousParameters.item) != newParameters) {
        propose(
          synchronizerId,
          newParameters,
          Some(synchronizerStore),
          mustFullyAuthorize,
          signedBy,
          Some(previousParameters.context.serial.increment),
          synchronize,
          force,
        ).discard
      }
    }

    @Help.Summary(
      "Update the ledger time record time tolerance in the dynamic synchronizer parameters"
    )
    @Help.Description(
      """
        synchronizerId: the target synchronizer
        newLedgerTimeRecordTimeTolerance: the new ledgerTimeRecordTimeTolerance value to apply to the synchronizer"""
    )
    def set_ledger_time_record_time_tolerance(
        synchronizerId: SynchronizerId,
        newLedgerTimeRecordTimeTolerance: config.NonNegativeFiniteDuration,
    ): Unit = setLedgerTimeRecordTimeTolerance(synchronizerId, newLedgerTimeRecordTimeTolerance)

    private def setLedgerTimeRecordTimeTolerance(
        synchronizerId: SynchronizerId,
        newLedgerTimeRecordTimeTolerance: config.NonNegativeFiniteDuration,
    ): Unit =
      TraceContext.withNewTraceContext { implicit tc =>
        logger.info(
          s"Immediately updating ledgerTimeRecordTimeTolerance to $newLedgerTimeRecordTimeTolerance..."
        )
        propose_update(
          synchronizerId,
          _.update(ledgerTimeRecordTimeTolerance = newLedgerTimeRecordTimeTolerance),
        )
      }

    @Help.Summary(
      "Update the submission time record time tolerance in the dynamic synchronizer parameters"
    )
    @Help.Description(
      """If it would be insecure to perform the change immediately,
        |the command will block and wait until it is secure to perform the change.
        |The command will block for at most twice of ``newSubmissionTimeRecordTimeTolerance``.
        |
        |The method will fail if ``mediatorDeduplicationTimeout`` is less than twice of ``newSubmissionTimeRecordTimeTolerance``.
        |
        |Do not modify synchronizer parameters concurrently while running this command,
        |because the command may override concurrent changes.
        |
        |force: update ``newSubmissionTimeRecordTimeTolerance`` immediately without blocking.
        |This is safe to do during synchronizer bootstrapping and in test environments, but should not be done in operational production systems."""
    )
    def set_submission_time_record_time_tolerance(
        synchronizerId: SynchronizerId,
        newSubmissionTimeRecordTimeTolerance: config.NonNegativeFiniteDuration,
        force: Boolean = false,
    ): Unit =
      TraceContext.withNewTraceContext { implicit tc =>
        if (!force) {
          securely_set_submission_time_record_time_tolerance(
            synchronizerId,
            newSubmissionTimeRecordTimeTolerance,
          )
        } else {
          logger.info(
            s"Immediately updating submissionTimeRecordTimeTolerance to $newSubmissionTimeRecordTimeTolerance..."
          )
          propose_update(
            synchronizerId,
            _.update(submissionTimeRecordTimeTolerance = newSubmissionTimeRecordTimeTolerance),
            force = ForceFlags(ForceFlag.SubmissionTimeRecordTimeToleranceIncrease),
          )
        }
      }

    private def securely_set_submission_time_record_time_tolerance(
        synchronizerId: SynchronizerId,
        newSubmissionTimeRecordTimeTolerance: config.NonNegativeFiniteDuration,
    )(implicit traceContext: TraceContext): Unit = {

      // See i9028 for a detailed design.
      // https://docs.google.com/document/d/1tpPbzv2s6bjbekVGBn6X5VZuw0oOTHek5c30CBo4UkI/edit#bookmark=id.1dzc6dxxlpca
      // We wait until the antecedent of Lemma 2 Item 2 is falsified for all changes that violate the conclusion.
      // Note: This validation was originally designed for ledgerTimeRecordTimeTolerance. With the introduction of
      // submissionTimeRecordTimeTolerance, the validation was moved to it instead of ledgerTimeRecordTimeTolerance

      // Compute new parameters
      val oldSynchronizerParameters = get_dynamic_synchronizer_parameters(synchronizerId)
      val oldSubmissionTimeRecordTimeTolerance =
        oldSynchronizerParameters.submissionTimeRecordTimeTolerance

      val minMediatorDeduplicationTimeout = newSubmissionTimeRecordTimeTolerance * 2

      if (
        oldSynchronizerParameters.mediatorDeduplicationTimeout < minMediatorDeduplicationTimeout
      ) {
        val err: RpcError = TopologyManagerError.IncreaseOfSubmissionTimeRecordTimeTolerance
          .PermanentlyInsecure(
            newSubmissionTimeRecordTimeTolerance.toInternal,
            oldSynchronizerParameters.mediatorDeduplicationTimeout.toInternal,
          )
        val msg = CantonError.stringFromContext(err)
        consoleEnvironment.run(GenericCommandError(msg))
      }

      logger.info(
        s"Securely updating submissionTimeRecordTimeTolerance to $newSubmissionTimeRecordTimeTolerance..."
      )

      // Poll until it is safe to increase submissionTimeRecordTimeTolerance
      def checkPreconditions(): Future[Unit] = {
        val startTs = consoleEnvironment.environment.clock.now

        // Doing a no-op update so that the resulting topology transaction gives us a meaningful lower bound on the sequencer clock
        logger.info(
          s"Do a no-op update of submissionTimeRecordTimeTolerance to $oldSubmissionTimeRecordTimeTolerance..."
        )
        propose_update(
          synchronizerId,
          _.copy(submissionTimeRecordTimeTolerance = oldSubmissionTimeRecordTimeTolerance),
        )

        logger.debug("Check for incompatible past synchronizer parameters...")

        val allTransactions = list(
          TopologyStoreId.Synchronizer(synchronizerId),
          // We can't specify a lower bound in range because that would be compared against validFrom.
          // (But we need to compare to validUntil).
          timeQuery = TimeQuery.Range(None, None),
        )

        // This serves as a lower bound of validFrom for the next topology transaction.
        val lastSequencerTs =
          allTransactions
            .map(_.context.validFrom)
            .maxOption
            .getOrElse(throw new NoSuchElementException("Missing synchronizer parameters!"))

        logger.debug(s"Last sequencer timestamp is $lastSequencerTs.")

        // Determine how long we need to wait until all incompatible synchronizerParameters have become
        // invalid for at least minMediatorDeduplicationTimeout.
        val waitDuration = allTransactions
          .filterNot(tx =>
            ConsoleDynamicSynchronizerParameters(tx.item)
              .compatibleWithNewSubmissionTimeRecordTimeTolerance(
                newSubmissionTimeRecordTimeTolerance
              )
          )
          .map { tx =>
            val elapsedForAtLeast = tx.context.validUntil match {
              case Some(validUntil) => Duration.between(validUntil, lastSequencerTs)
              case None => Duration.ZERO
            }
            minMediatorDeduplicationTimeout.asJava minus elapsedForAtLeast
          }
          .maxOption
          .getOrElse(Duration.ZERO)

        if (waitDuration > Duration.ZERO) {
          logger.info(
            show"Found incompatible past synchronizer parameters. Waiting for $waitDuration..."
          )

          // Use the clock instead of Threading.sleep to support sim clock based tests.
          val delayF = consoleEnvironment.environment.clock
            .scheduleAt(
              _ => (),
              startTs.plus(waitDuration),
            ) // avoid scheduleAfter, because that causes a race condition in integration tests
            .onShutdown(
              throw new IllegalStateException(
                "Update of submissionTimeRecordTimeTolerance interrupted due to shutdown."
              )
            )
          // Do not submit checkPreconditions() to the clock because it is blocking and would therefore block the clock.
          delayF.flatMap(_ => checkPreconditions())
        } else {
          Future.unit
        }
      }

      consoleEnvironment.commandTimeouts.unbounded.await(
        "Wait until submissionTimeRecordTimeTolerance can be increased."
      )(
        checkPreconditions()
      )

      // Now that past values of mediatorDeduplicationTimeout have been large enough,
      // we can change submissionTimeRecordTimeTolerance.
      logger.info(
        s"Now changing submissionTimeRecordTimeTolerance to $newSubmissionTimeRecordTimeTolerance..."
      )
      propose_update(
        synchronizerId,
        _.copy(submissionTimeRecordTimeTolerance = newSubmissionTimeRecordTimeTolerance),
        force = ForceFlags(ForceFlag.SubmissionTimeRecordTimeToleranceIncrease),
      )
    }
  }

  @Help.Summary("Inspect topology stores")
  @Help.Group("Topology stores")
  object stores extends Helpful {
    @Help.Summary("List available topology stores")
    def list(): Seq[TopologyStoreId] =
      consoleEnvironment.run {
        adminCommand(
          TopologyAdminCommands.Read.ListStores()
        )
      }

    @Help.Summary("Creates a temporary topology store.")
    @Help.Description(
      """A temporary topology store is useful for orchestrating the synchronizer founding ceremony or importing a topology snapshot for later inspection.
        |Temporary topology stores are not persisted and all transactions are kept in memory only, which means restarting the node causes the loss of all
        |transactions in that store.
        |Additionally, temporary topology stores are not connected to any synchronizer, so there is no automatic propagation of topology transactions
        |from the temporary store to connected synchronizers."""
    )
    def create_temporary_topology_store(
        name: String,
        protocolVersion: ProtocolVersion,
    ): TopologyStoreId.Temporary =
      consoleEnvironment.run {
        adminCommand(TopologyAdminCommands.Write.CreateTemporaryStore(name, protocolVersion))
      }

    @Help.Summary(
      "This command drops a temporary topology store and all transactions contained in it."
    )
    @Help.Description(
      """Dropping a temporary topology store is not reversible and all topology transactions in the store will
        |be permanently dropped.
        |It's not possible to delete the authorized store or any synchronizer store with this command."""
    )
    def drop_temporary_topology_store(temporaryStoreId: TopologyStoreId.Temporary): Unit =
      consoleEnvironment.run {
        adminCommand(TopologyAdminCommands.Write.DropTemporaryStore(temporaryStoreId))
      }

  }

  private def expectAtMostOneResult[R](seq: Seq[R]): Option[R] = seq match {
    case Nil => None
    case res +: Nil => Some(res)
    case multipleResults =>
      throw new IllegalStateException(
        s"Found ${multipleResults.size} results, but expect at most one."
      )
  }

  private def expectExactlyOneResult[R](seq: Seq[R]): R = expectAtMostOneResult(seq).getOrElse(
    throw new IllegalStateException(s"Expected exactly one result, but found none")
  )
}
