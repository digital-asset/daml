// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import better.files.File
import cats.Applicative
import cats.syntax.either.*
import cats.syntax.foldable.*
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.{
  TopologyTransactionWrapper,
  UpdateWrapper,
}
import com.digitalasset.canton.admin.api.client.commands.{
  ParticipantAdminCommands,
  TopologyAdminCommands,
}
import com.digitalasset.canton.admin.api.client.data.{
  AddPartyStatus,
  ListPartiesResult,
  PartyOnboardingFlagStatus,
}
import com.digitalasset.canton.admin.participant.v30.ExportPartyAcsResponse
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.config.{ConsoleCommandTimeout, NonNegativeDuration}
import com.digitalasset.canton.console.commands.TopologyTxFiltering.{AddedFilter, RevokedFilter}
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  ConsoleCommandResult,
  ConsoleEnvironment,
  FeatureFlag,
  FeatureFlagFilter,
  Help,
  Helpful,
  ParticipantReference,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.grpc.FileStreamObserver
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.data.{
  ContractImportMode,
  RepresentativePackageIdOverride,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{LedgerParticipantId, SynchronizerAlias, config}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import io.grpc.Context

import java.time.Instant
import java.util.UUID

class PartiesAdministrationGroup(
    runner: AdminCommandRunner,
    consoleEnvironment: ConsoleEnvironment,
) extends Helpful {

  protected def defaultLimit: PositiveInt =
    consoleEnvironment.environment.config.parameters.console.defaultLimit

  import runner.*

  @Help.Summary(
    "List active parties, their active participants, and the participants' permissions on synchronizers."
  )
  @Help.Description(
    """Inspect the parties known by this participant as used for synchronisation.
      |The response is built from the timestamped topology transactions of each synchronizer, excluding the
      |authorized store of the given node. For each known party, the list of active
      |participants and their permission on the synchronizer for that party is given.
      |
      filterParty: Filter by parties starting with the given string.
      filterParticipant: Filter for parties that are hosted by a participant with an id starting with the given string
      filterSynchronizerId: Filter by synchronizers whose id starts with the given string.
      asOf: Optional timestamp to inspect the topology state at a given point in time.
      limit: Limit on the number of parties fetched (defaults to canton.parameters.console.default-limit).

      Example: participant1.parties.list(filterParty="alice")
      """
  )
  def list(
      filterParty: String = "",
      filterParticipant: String = "",
      synchronizerIds: Set[SynchronizerId] = Set.empty,
      asOf: Option[Instant] = None,
      limit: PositiveInt = defaultLimit,
  ): Seq[ListPartiesResult] =
    consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommands.Aggregation.ListParties(
          synchronizerIds = synchronizerIds,
          filterParty = filterParty,
          filterParticipant = filterParticipant,
          asOf = asOf,
          limit = limit,
        )
      )
    }
}

class ParticipantPartiesAdministrationGroup(
    participantId: => ParticipantId,
    reference: ParticipantReference,
    override protected val consoleEnvironment: ConsoleEnvironment,
    override protected val loggerFactory: NamedLoggerFactory,
) extends PartiesAdministrationGroup(reference, consoleEnvironment)
    with FeatureFlagFilter {

  private def timeouts: ConsoleCommandTimeout = consoleEnvironment.commandTimeouts

  @Help.Summary("List parties hosted by this participant")
  @Help.Description("""Inspect the parties hosted by this participant as used for synchronisation.
      |The response is built from the timestamped topology transactions of each synchronizer, excluding the
      |authorized store of the given node. The search will include all hosted parties and is equivalent
      |to running the `list` method using the participant id of the invoking participant.
      |
      filterParty: Filter by parties starting with the given string.
      filterSynchronizerId: Filter by synchronizers whose id starts with the given string.
      asOf: Optional timestamp to inspect the topology state at a given point in time.
      limit: How many items to return (defaults to canton.parameters.console.default-limit)

      Example: participant1.parties.hosted(filterParty="alice")""")
  def hosted(
      filterParty: String = "",
      synchronizerIds: Set[SynchronizerId] = Set.empty,
      asOf: Option[Instant] = None,
      limit: PositiveInt = defaultLimit,
  ): Seq[ListPartiesResult] =
    list(
      filterParty,
      filterParticipant = participantId.filterString,
      synchronizerIds = synchronizerIds,
      asOf = asOf,
      limit = limit,
    )

  @Help.Summary("Find a party from a filter string")
  @Help.Description(
    """Will search for all parties that match this filter string. If it finds exactly one party, it
      |will return that one. Otherwise, the function will throw."""
  )
  def find(filterParty: String): PartyId =
    list(filterParty).map(_.party).distinct.toList match {
      case one :: Nil => one
      case Nil => throw new IllegalArgumentException(s"No party matching $filterParty")
      case more =>
        throw new IllegalArgumentException(s"Multiple parties match $filterParty: $more")
    }

  @Help.Summary("Enable/add party to participant")
  @Help.Description("""This function registers a new party on a synchronizer with the current participant within the
      |participants namespace. The function fails if the participant does not have appropriate signing keys
      |to issue the corresponding PartyToParticipant topology transaction, or if the participant is not connected to any
      |synchronizers.
      |The synchronizer parameter does not have to be specified if the participant is connected only to one synchronizer.
      |If the participant is connected to multiple synchronizers, the party needs to be enabled on each synchronizer explicitly.
      |Additionally, a sequence of additional participants can be added to be synchronized to
      |ensure that the party is known to these participants as well before the function terminates.
      |""")
  def enable(
      name: String,
      namespace: Namespace = participantId.namespace,
      synchronizer: Option[SynchronizerAlias] = None,
      synchronizeParticipants: Seq[ParticipantReference] = consoleEnvironment.participants.all,
      synchronize: Option[config.NonNegativeDuration] = Some(
        consoleEnvironment.commandTimeouts.unbounded
      ),
  ): PartyId =
    consoleEnvironment.run {
      ConsoleCommandResult.fromEither {
        for {
          // assert that name is valid ParticipantId
          _ <- LedgerParticipantId.fromString(name)
          partyId <- UniqueIdentifier.create(name, namespace).map(PartyId(_))
          // find the synchronizer id
          synchronizerId <- lookupOrDetectSynchronizerId(synchronizer)
          _ <- runPartyCommand(
            partyId,
            synchronizerId,
            synchronize,
          ).toEither

          _ <- Applicative[Either[String, *]].whenA(synchronize.nonEmpty)(
            PartiesAdministration.Allocation.waitForPartyKnown(
              partyId = partyId,
              hostingParticipant = reference,
              synchronizeParticipants = synchronizeParticipants,
              synchronizerId = synchronizerId.logical,
            )(consoleEnvironment)
          )
        } yield partyId
      }
    }

  @VisibleForTesting
  private[canton] object testing {

    @VisibleForTesting
    private[canton] val external = new ExternalPartiesTestingAdministration(
      reference,
      consoleEnvironment,
      loggerFactory,
    )

    @Help.Summary("Find a party from a filter string")
    @Help.Description(
      """Will search for all parties that match this filter string. If it finds exactly one party, it
        |will return that one. Otherwise, the function will throw."""
    )
    @VisibleForTesting
    private[canton] def find(filterParty: String)(implicit partyKind: PartyKind): Party =
      list(filterParty).map(_.partyResult).distinct.toList match {
        case one :: Nil => one
        case Nil => throw new IllegalArgumentException(s"No party matching $filterParty")
        case more =>
          throw new IllegalArgumentException(s"Multiple parties match $filterParty: $more")
      }

    @Help.Summary(
      "List active parties, their active participants, and the participants' permissions on synchronizers."
    )
    @Help.Description(
      """Inspect the parties known by this participant as used for synchronisation.
      |The response is built from the timestamped topology transactions of each synchronizer, excluding the
      |authorized store of the given node. For each known party, the list of active
      |participants and their permission on the synchronizer for that party is given.
      |
      filterParty: Filter by parties starting with the given string.
      filterParticipant: Filter for parties that are hosted by a participant with an id starting with the given string
      filterSynchronizerId: Filter by synchronizers whose id starts with the given string.
      asOf: Optional timestamp to inspect the topology state at a given point in time.
      limit: Limit on the number of parties fetched (defaults to canton.parameters.console.default-limit).

      Example: participant1.parties.list(filterParty="alice")
      """
    )
    @VisibleForTesting
    private[canton] def list(
        filterParty: String = "",
        filterParticipant: String = "",
        synchronizerIds: Set[SynchronizerId] = Set.empty,
        asOf: Option[Instant] = None,
        limit: PositiveInt = defaultLimit,
    )(implicit partyKind: PartyKind): Seq[ListPartiesResult] = partyKind match {
      case PartyKind.Local =>
        reference.parties
          .list(filterParty, filterParticipant, synchronizerIds, asOf, limit)
      case PartyKind.External =>
        external.list(
          filterParty,
          filterParticipant,
          synchronizerIds,
          asOf,
          limit,
        )
    }

    /** Enable a party hosted on `reference` with either submission permission (for local parties),
      * or confirmation permission (for external parties).
      * @param name
      *   Name of the party to be enabled
      * @param synchronizer
      *   Synchronizer
      * @param synchronizeParticipants
      *   Participants that need to see activation of the party
      */
    @VisibleForTesting
    private[canton] def enable(
        name: String,
        synchronizer: Option[SynchronizerAlias] = None,
        synchronizeParticipants: Seq[ParticipantReference] = consoleEnvironment.participants.all,
        synchronize: Option[config.NonNegativeDuration] = Some(timeouts.unbounded),
    )(implicit partyKind: PartyKind): Party = partyKind match {
      case PartyKind.Local =>
        reference.parties.enable(
          name,
          synchronizer = synchronizer,
          synchronizeParticipants = synchronizeParticipants,
          synchronize = synchronize,
        )
      case PartyKind.External =>
        external.enable(
          name,
          synchronizer = synchronizer,
          synchronizeParticipants = synchronizeParticipants,
          synchronize = synchronize,
        )
    }

    /** Enable an existing party hosted on `reference`. Unlike `enable`, this command assumes the
      * party already exists on a different synchronizer.
      *
      * Note: For external parties the keys (and threshold) will be the same as on the synchronizer
      * on which the party is already hosted.
      *
      * @param party
      *   Name of the party to be enabled
      * @param synchronizer
      *   Synchronizer
      * @param synchronizeParticipants
      *   Participants that need to see activation of the party
      */
    @VisibleForTesting
    private[canton] def also_enable(
        party: Party,
        synchronizer: SynchronizerAlias,
        synchronizeParticipants: Seq[ParticipantReference] = consoleEnvironment.participants.all,
        synchronize: Option[config.NonNegativeDuration] = Some(timeouts.unbounded),
    ): Unit = party match {
      case partyId: PartyId =>
        reference.parties
          .enable(
            partyId.uid.identifier.str,
            synchronizer = Some(synchronizer),
            synchronizeParticipants = synchronizeParticipants,
            synchronize = synchronize,
          )
          .discard[PartyId]
      case externalParty: ExternalParty =>
        external.also_enable(externalParty, synchronizer, synchronizeParticipants, synchronize)
    }
  }

  /** @return
    *   if SynchronizerAlias is set, the SynchronizerId that corresponds to the alias. if
    *   SynchronizerAlias is not set, the synchronizer id of the only connected synchronizer. if the
    *   participant is connected to multiple synchronizers, it returns an error.
    */
  private def lookupOrDetectSynchronizerId(
      alias: Option[SynchronizerAlias]
  ): Either[String, PhysicalSynchronizerId] = {
    lazy val singleConnectedSynchronizer = reference.synchronizers.list_connected() match {
      case Seq() =>
        Left("not connected to any synchronizer")
      case Seq(onlyOneSynchronizer) => Right(onlyOneSynchronizer.physicalSynchronizerId)
      case multiple =>
        val psids = multiple.map(_.physicalSynchronizerId)

        Left(
          s"cannot automatically determine synchronizer, because participant is connected to more than 1 synchronizer: $psids"
        )
    }
    alias
      .map(a => Right(reference.synchronizers.physical_id_of(a)))
      .getOrElse(singleConnectedSynchronizer)
  }

  private def runPartyCommand(
      partyId: PartyId,
      synchronizerId: PhysicalSynchronizerId,
      synchronize: Option[config.NonNegativeDuration],
  ): ConsoleCommandResult[SignedTopologyTransaction[TopologyChangeOp, PartyToParticipant]] = {
    // determine the next serial
    val nextSerial = reference.topology.party_to_participant_mappings
      .list(synchronizerId, filterParty = partyId.filterString)
      .maxByOption(_.context.serial)
      .map(_.context.serial.increment)

    reference
      .adminCommand(
        TopologyAdminCommands.Write.Propose(
          mapping = PartyToParticipant.create(
            partyId,
            PositiveInt.one,
            Seq(
              HostingParticipant(
                participantId,
                ParticipantPermission.Submission,
              )
            ),
          ),
          // let the topology service determine the appropriate keys to use
          signedBy = Seq.empty,
          serial = nextSerial,
          store = synchronizerId,
          mustFullyAuthorize = true,
          change = TopologyChangeOp.Replace,
          forceChanges = ForceFlags.none,
          waitToBecomeEffective = synchronize,
        )
      )
  }

  @Help.Summary("Disable party on participant")
  def disable(
      party: PartyId,
      forceFlags: ForceFlags = ForceFlags.none,
      synchronizer: Option[SynchronizerAlias] = None,
  ): Unit = {
    val synchronizerId = consoleEnvironment.runE(lookupOrDetectSynchronizerId(synchronizer))
    reference.topology.party_to_participant_mappings
      .propose_delta(
        party,
        removes = List(this.participantId),
        forceFlags = forceFlags,
        store = synchronizerId,
      )
      .discard
  }

  @Help.Summary("Add a previously existing party to the local participant", FeatureFlag.Preview)
  @Help.Description(
    """Initiate adding a previously existing party to this participant on the specified synchronizer.
      |Performs some checks synchronously and then initiates party replication asynchronously. The returned `addPartyRequestId`
      |parameter allows identifying asynchronous progress and errors."""
  )
  def add_party_async(
      party: PartyId,
      synchronizerId: SynchronizerId,
      sourceParticipant: ParticipantId,
      serial: PositiveInt,
      participantPermission: ParticipantPermission,
  ): String = check(FeatureFlag.Preview) {
    consoleEnvironment.run {
      reference.adminCommand(
        ParticipantAdminCommands.PartyManagement.AddPartyAsync(
          party,
          synchronizerId,
          sourceParticipant,
          serial,
          participantPermission,
        )
      )
    }
  }

  @Help.Summary("Obtain status on a pending `add_party_async` call", FeatureFlag.Preview)
  @Help.Description(
    """Retrieve status information on a party previously added via the `add_party_async` endpoint
      |by specifying the previously returned `addPartyRequestId` parameter."""
  )
  def get_add_party_status(addPartyRequestId: String): AddPartyStatus = check(FeatureFlag.Preview) {
    consoleEnvironment.run {
      reference.adminCommand(
        ParticipantAdminCommands.PartyManagement.GetAddPartyStatus(addPartyRequestId)
      )
    }
  }

  @Help.Summary("Finds a party's highest activation offset.")
  @Help.Description(
    """This command locates the highest ledger offset where a party's activation matches
      |specified criteria.
      |
      |It searches the ledger for topology transactions, sequenced by the given synchronizer
      |(`synchronizerId`), that result in the party (`partyId`) being newly hosted on the
      |participant (`participantId`). An optional `validFrom` timestamp filters the topology
      |transactions for their effective time.
      |
      |The ledger search occurs within the specified offset range, targeting a specific number
      |of topology transactions (`completeAfter`).
      |
      |The search begins at the ledger start if `beginOffsetExclusive` is default. If the
      |participant was pruned and `beginOffsetExclusive` is below the pruning offset, a
      |`NOT_FOUND` error occurs. Use an `beginOffsetExclusive` near, but before, the desired
      |topology transactions.
      |
      |If `endOffsetInclusive` is not set (`None`), the search continues until `completeAfter`
      |number of transactions are found or the `timeout` expires. Otherwise, the ledger search
      |ends at the specified offset.
      |
      |This command is useful for creating ACS snapshots with `export_acs`, which requires the
      |party activation ledger offset.
      |
      |
      |The arguments are:
      |- partyId: The party to find activations for.
      |- participantId: The participant hosting the new party.
      |- synchronizerId: The synchronizer sequencing the activations.
      |- validFrom: The activation's effective time (default: None).
      |- beginOffsetExclusive: Starting ledger offset (default: 0).
      |- endOffsetInclusive: Ending ledger offset (default: None = trailing search).
      |- completeAfter: Number of transactions to find (default: Maximum = no limit).
      |- timeout: Search timeout (default: 1 minute).
      |"""
  )
  def find_party_max_activation_offset(
      partyId: PartyId,
      participantId: ParticipantId,
      synchronizerId: SynchronizerId,
      validFrom: Option[Instant] = None,
      beginOffsetExclusive: Long = 0L,
      endOffsetInclusive: Option[Long] = None,
      completeAfter: PositiveInt = PositiveInt.MaxValue,
      timeout: NonNegativeDuration = timeouts.bounded,
  ): Long = {
    val filter = TopologyTxFiltering.getTopologyFilter(
      partyId,
      participantId,
      synchronizerId,
      validFrom,
      AddedFilter,
    )(consoleEnvironment)

    findTopologyOffset(
      partyId,
      beginOffsetExclusive,
      endOffsetInclusive,
      completeAfter,
      timeout,
      filter,
    )
  }

  @Help.Summary("Finds a party's highest deactivation offset.")
  @Help.Description(
    """This command locates the highest ledger offset where a party's deactivation matches
      |specified criteria.
      |
      |It searches the ledger for topology transactions, sequenced by the given synchronizer
      |(`synchronizerId`), that result in the party (`partyId`) being revoked on the participant
      |(`participantId`). An optional `validFrom` timestamp filters the topology transactions
      |for their effective time.
      |
      |The ledger search occurs within the specified offset range, targeting a specific number
      |of topology transactions (`completeAfter`).
      |
      |The search begins at the ledger start if `beginOffsetExclusive` is default. If the
      |participant was pruned and `beginOffsetExclusive` is below the pruning offset, a
      |`NOT_FOUND` error occurs. Use an `beginOffsetExclusive` near, but before, the desired
      |topology transactions.
      |
      |If `endOffsetInclusive` is not set (`None`), the search continues until `completeAfter`
      |number of transactions are found or the `timeout` expires. Otherwise, the ledger search
      |ends at the specified offset.
      |
      |This command is useful for finding active contracts at the ledger offset where a party
      |has been off-boarded from a participant.
      |
      |
      |The arguments are:
      |- partyId: The party to find deactivations for.
      |- participantId: The participant hosting the new party.
      |- synchronizerId: The synchronizer sequencing the deactivations.
      |- validFrom: The deactivation's effective time (default: None).
      |- beginOffsetExclusive: Starting ledger offset (default: 0).
      |- endOffsetInclusive: Ending ledger offset (default: None = trailing search).
      |- completeAfter: Number of transactions to find (default: Maximum = no limit).
      |- timeout: Search timeout (default: 1 minute).
      |"""
  )
  def find_party_max_deactivation_offset(
      partyId: PartyId,
      participantId: ParticipantId,
      synchronizerId: SynchronizerId,
      validFrom: Option[Instant] = None,
      beginOffsetExclusive: Long = 0L,
      endOffsetInclusive: Option[Long] = None,
      completeAfter: PositiveInt = PositiveInt.MaxValue,
      timeout: NonNegativeDuration = timeouts.bounded,
  ): Long = {
    val filter = TopologyTxFiltering.getTopologyFilter(
      partyId,
      participantId,
      synchronizerId,
      validFrom,
      RevokedFilter,
    )(consoleEnvironment)

    findTopologyOffset(
      partyId,
      beginOffsetExclusive,
      endOffsetInclusive,
      completeAfter,
      timeout,
      filter,
    )
  }

  private def findTopologyOffset(
      party: PartyId,
      beginOffsetExclusive: Long,
      endOffsetInclusive: Option[Long],
      completeAfter: PositiveInt,
      timeout: NonNegativeDuration,
      filter: UpdateWrapper => Boolean,
  ): Long = {
    val topologyTransactions: Seq[com.daml.ledger.api.v2.topology_transaction.TopologyTransaction] =
      reference.ledger_api.updates
        .topology_transactions(
          partyIds = Seq(party),
          completeAfter = completeAfter,
          timeout = timeout,
          beginOffsetExclusive = beginOffsetExclusive,
          endOffsetInclusive = endOffsetInclusive,
          resultFilter = filter,
        )
        .collect { case TopologyTransactionWrapper(topologyTransaction) => topologyTransaction }

    topologyTransactions
      .map(_.offset)
      .lastOption
      .getOrElse(
        consoleEnvironment.raiseError(
          "Offset not found in topology data. Possible causes: " +
            "1) No topology transaction exists (Solution: Initiate a new topology transaction). " +
            "2) Existing topology transactions do not match the specified search criteria. (Solution: Adjust search criteria). " +
            "3) The ledger has not yet processed the relevant topology transaction. (Solution: Retry after delay, ensuring the ledger (end) has advanced)."
        )
      )
  }

  @Help.Summary("Find highest ledger offset by timestamp.")
  @Help.Description(
    """This command attempts to find the highest ledger offset among all events belonging
      |to a synchronizer that have a record time before or at the given timestamp.
      |
      |Returns the highest ledger offset, or an error.
      |
      |Possible failure causes:
      |- The requested timestamp is too far in the past for which no events exist anymore.
      |- There are no events for the given synchronizer.
      |- Not all events have been processed fully and/or published to the Ledger API DB
      |  until the requested timestamp.
      |
      |Depending on the failure cause, this command can be tried to get a ledger offset.
      |For example, if not all events have been processed fully and/or published to the
      |Ledger API DB, a retry makes sense.
      |
      |The arguments are:
      |- synchronizerId: Restricts the query to a particular synchronizer.
      |- timestamp: A point in time.
      |- force: Defaults to false. If true, returns the highest currently known ledger offset
      |  with a record time before or at the given timestamp.
      |"""
  )
  def find_highest_offset_by_timestamp(
      synchronizerId: SynchronizerId,
      timestamp: Instant,
      force: Boolean = false,
  ): Long = consoleEnvironment.run {
    reference.adminCommand(
      ParticipantAdminCommands.PartyManagement
        .GetHighestOffsetByTimestamp(synchronizerId, timestamp, force)
    )
  }

  @Help.Summary(
    "Export active contracts for a given party to replicate it."
  )
  @Help.Description(
    """This command exports the current Active Contract Set (ACS) for a given
      |party to facilitate its replication from a source to a target participant.
      |
      |It uses the party's most recent activation on the target participant to
      |determine the precise historical state of the ACS to export from the
      |source participant.
      |
      |"Activation" on the target participant means the new hosting arrangement
      |has been authorized by both the party itself and the target participant
      |via party-to-participant topology transactions.
      |
      |This command will fail if the party has not yet been activated on the
      |target participant.
      |
      |Upon successful completion, the command writes a GZIP-compressed ACS
      |snapshot file. This file should then be imported into the target participant's
      |ACS using the `import_party_acs` command.
      |
      |The arguments are:
      |- party: The party being replicated, it must already be active on the target participant.
      |- synchronizerId: Restricts the export to the given synchronizer.
      |- targetParticipantId: Unique identifier of the target participant where the party
      |                       will be replicated.
      |- beginOffsetExclusive: Exclusive ledger offset used as starting point fo find the party's
      |                        activation on the target participant.
      |- exportFilePath: The path denoting the file where the ACS snapshot will be stored.
      |- waitForActivationTimeout: The maximum duration the service will wait to find the topology
      |                            transaction that activates the party on the target participant.
      |- timeout: A timeout for this operation to complete.
      """
  )
  def export_party_acs(
      party: PartyId,
      synchronizerId: SynchronizerId,
      targetParticipantId: ParticipantId,
      beginOffsetExclusive: Long,
      exportFilePath: String = "canton-acs-export.gz",
      waitForActivationTimeout: Option[config.NonNegativeFiniteDuration] = Some(
        config.NonNegativeFiniteDuration.ofMinutes(2)
      ),
      timeout: config.NonNegativeDuration = timeouts.unbounded,
  ): Unit =
    consoleEnvironment.run {
      val file = File(exportFilePath)
      val responseObserver = new FileStreamObserver[ExportPartyAcsResponse](file, _.chunk)

      def call: ConsoleCommandResult[Context.CancellableContext] =
        reference.adminCommand(
          ParticipantAdminCommands.PartyManagement.ExportPartyAcs(
            party,
            synchronizerId,
            targetParticipantId,
            beginOffsetExclusive,
            waitForActivationTimeout,
            responseObserver,
          )
        )

      processResult(
        call,
        responseObserver.result,
        timeout,
        request = "exporting party acs",
        cleanupOnError = () => file.delete(),
      )
    }

  @Help.Summary(
    "Import active contracts from a snapshot file to replicate a party."
  )
  @Help.Description(
    """This command imports contracts from an Active Contract Set (ACS) snapshot
      |file into the participant's ACS. It expects the given ACS snapshot file to
      |be the result of a previous `export_party_acs` command invocation.
      |
      |The argument is:
      |- importFilePath: The path denoting the file from where the ACS snapshot will be read.
      |                  Defaults to "canton-acs-export.gz" when undefined.
      |- workflowIdPrefix: Sets a custom prefix for the workflow ID to easily identify all
      |                  transactions generated by this import.
      |                  Defaults to "import-<random_UUID>" when unspecified.
      |- contractImportMode: Governs contract authentication processing on import. Options include
      |                      Validation (default), [Accept].
      |- representativePackageIdOverride: Defines override mappings for assigning
      |                                   representative package IDs to contracts upon ACS import.
      |                                   Defaults to NoOverride when undefined.
   """
  )
  def import_party_acs(
      importFilePath: String = "canton-acs-export.gz",
      workflowIdPrefix: String = "",
      contractImportMode: ContractImportMode = ContractImportMode.Validation,
      representativePackageIdOverride: RepresentativePackageIdOverride =
        RepresentativePackageIdOverride.NoOverride,
  ): Unit =
    consoleEnvironment.run {
      reference.adminCommand(
        ParticipantAdminCommands.PartyManagement.ImportPartyAcs(
          ByteString.copyFrom(File(importFilePath).loadBytes),
          if (workflowIdPrefix.nonEmpty) workflowIdPrefix else s"import-${UUID.randomUUID}",
          contractImportMode,
          representativePackageIdOverride,
        )
      )
    }

  @Help.Summary(
    "Clears the onboarding flag for a party."
  )
  @Help.Description(
    """Instructs the participant to unilaterally clear the 'onboarding' flag on the
      |party-to-participant topology mapping.
      |
      |This operation is time-sensitive. It will first attempt to authorize the
      |flag's clearance immediately.
      |
      |If the flag cannot be safely cleared, this command schedules an idempotent
      |background task to propose the clearance at the appropriate "safe time".
      |This safe time (the "latest decision deadline") is computed from the
      |synchronizer's parameter history to ensure all prior in-flight
      |transactions are finalized.
      |
      |The command is idempotent and designed to be polled. You can re-run
      |it safely to check the status.
      |
      |Prerequisite:
      |- A prior party-to-participant mapping must exist that activates the party
      |  on this participant with the `onboarding` flag set to `true`.
      |
      |Returns the current the current status as `PartyOnboardingFlagStatus`:
      |- `FlagNotSet`: The flag is successfully cleared (or was already clear).
      |- `FlagSet`: The flag is still set. A clearance task is pending and scheduled
      |             to run at or after the given timestamp.
      |
      |The arguments are:
      |- party: The party being onboarded. It must already be active on the participant.
      |- synchronizerId: Restricts the operation to the given synchronizer.
      |- beginOffsetExclusive: Exclusive ledger offset used as a starting point to find the
      |                        party's activation.
      |- waitForActivationTimeout: Max duration to wait to find the party's activation
      |                            topology transaction.
    """
  )
  def clear_party_onboarding_flag(
      party: PartyId,
      synchronizerId: SynchronizerId,
      beginOffsetExclusive: Long,
      waitForActivationTimeout: Option[config.NonNegativeFiniteDuration] = Some(
        config.NonNegativeFiniteDuration.ofMinutes(2)
      ),
  ): PartyOnboardingFlagStatus =
    consoleEnvironment.run {
      reference.adminCommand(
        ParticipantAdminCommands.PartyManagement.ClearPartyOnboardingFlag(
          party,
          synchronizerId,
          NonNegativeLong.tryCreate(beginOffsetExclusive),
          waitForActivationTimeout,
        )
      )
    }

}

private[canton] object PartiesAdministration {
  object Allocation {

    /** Ensure a new party is known by some participants
      * @param partyId
      *   Party to be known
      * @param hostingParticipant
      *   The party hosting the patry
      * @param synchronizeParticipants
      *   All the participants that need to know the party
      * @param synchronizerId
      *   Synchronizer
      */
    def waitForPartyKnown(
        partyId: PartyId,
        hostingParticipant: ParticipantReference,
        synchronizeParticipants: Seq[ParticipantReference],
        synchronizerId: SynchronizerId,
    )(implicit consoleEnvironment: ConsoleEnvironment): Either[String, Unit] =
      for {
        _ <- retryE(
          hostingParticipant.ledger_api.parties.list().map(_.party).contains(partyId),
          show"The party $partyId never appeared on the ledger API server",
        )

        // Party is known on relevant participants
        otherParticipants = synchronizeParticipants.filter(
          _.synchronizers.is_connected(synchronizerId)
        )
        _ <- (hostingParticipant +: otherParticipants).traverse_(p =>
          waitForParty(
            partyId,
            synchronizerId,
            hostingParticipant = hostingParticipant.id,
            queriedParticipant = p,
          )
        )
      } yield ()

    private def synchronizersPartyIsRegisteredOn(
        hostingParticipant: ParticipantId,
        participant: ParticipantReference,
        partyId: PartyId,
    ): Set[SynchronizerId] =
      participant.parties
        .list(
          filterParty = partyId.filterString,
          filterParticipant = hostingParticipant.filterString,
        )
        .flatMap(_.participants.flatMap(_.synchronizers))
        .map(_.synchronizerId)
        .toSet

    private def waitForParty(
        partyId: PartyId,
        synchronizerId: SynchronizerId,
        hostingParticipant: ParticipantId,
        queriedParticipant: ParticipantReference,
    )(implicit consoleEnvironment: ConsoleEnvironment): Either[String, Unit] =
      retryE(
        synchronizersPartyIsRegisteredOn(hostingParticipant, queriedParticipant, partyId).contains(
          synchronizerId
        ),
        show"Party $partyId did not appear for $queriedParticipant on synchronizer $synchronizerId}",
      )
  }

  private def retryE(condition: => Boolean, message: => String)(implicit
      consoleEnvironment: ConsoleEnvironment
  ): Either[String, Unit] =
    AdminCommandRunner
      .retryUntilTrue(consoleEnvironment.commandTimeouts.ledgerCommand)(condition)
      .toEither
      .leftMap(_ => message)
}

private object TopologyTxFiltering {
  sealed trait AuthorizationFilterKind
  case object AddedFilter extends AuthorizationFilterKind
  case object RevokedFilter extends AuthorizationFilterKind

  def getTopologyFilter(
      partyId: PartyId,
      participantId: ParticipantId,
      synchronizerId: SynchronizerId,
      validFrom: Option[Instant],
      filterType: AuthorizationFilterKind,
  )(consoleEnvironment: ConsoleEnvironment): UpdateWrapper => Boolean = {
    def filterOnEffectiveTime(
        tx: com.daml.ledger.api.v2.topology_transaction.TopologyTransaction,
        recordTime: Option[Instant],
    ): Boolean =
      recordTime.forall { instant =>
        tx.recordTime match {
          case Some(ts) =>
            ProtoConverter.InstantConverter
              .fromProtoPrimitive(ts)
              .valueOr(err =>
                consoleEnvironment.raiseError(
                  s"Failed record time timestamp conversion for $ts: $err"
                )
              ) == instant
          case None => false
        }
      }

    def filter(wrapper: UpdateWrapper): Boolean =
      wrapper match {
        case TopologyTransactionWrapper(tx) =>
          synchronizerId.toProtoPrimitive == wrapper.synchronizerId &&
          tx.events.exists { tx =>
            filterType match {
              case AddedFilter =>
                val added = tx.getParticipantAuthorizationAdded
                added.partyId == partyId.toLf && added.participantId == participantId.toLf
              case RevokedFilter =>
                val revoked = tx.getParticipantAuthorizationRevoked
                revoked.partyId == partyId.toLf && revoked.participantId == participantId.toLf
            }
          } &&
          filterOnEffectiveTime(tx, validFrom)
        case _ => false
      }

    filter
  }
}
