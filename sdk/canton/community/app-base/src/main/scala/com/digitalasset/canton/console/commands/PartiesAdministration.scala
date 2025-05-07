// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import better.files.File
import cats.Applicative
import cats.syntax.either.*
import cats.syntax.foldable.*
import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction
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
  PartyDetails,
}
import com.digitalasset.canton.admin.participant.v30.{
  ExportAcsAtTimestampResponse,
  ExportAcsResponse,
}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.config.{ConsoleCommandTimeout, NonNegativeDuration}
import com.digitalasset.canton.console.commands.TopologyTxFiltering.{AddedFilter, RevokedFilter}
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  ConsoleCommandResult,
  ConsoleEnvironment,
  ConsoleMacros,
  FeatureFlag,
  FeatureFlagFilter,
  Help,
  Helpful,
  ParticipantReference,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.grpc.FileStreamObserver
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{LedgerParticipantId, SynchronizerAlias}
import io.grpc.Context

import java.time.Instant

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
      participants: Seq[ParticipantId] = Seq(participantId),
      threshold: PositiveInt = PositiveInt.one,
      synchronizer: Option[SynchronizerAlias] = None,
      synchronizeParticipants: Seq[ParticipantReference] = consoleEnvironment.participants.all,
      mustFullyAuthorize: Boolean = true,
      synchronize: Option[NonNegativeDuration] = Some(
        consoleEnvironment.commandTimeouts.unbounded
      ),
  ): PartyId = {
    def synchronizersPartyIsRegisteredOn(participant: ParticipantReference, partyId: PartyId) =
      participant.parties
        .list(filterParty = partyId.filterString, filterParticipant = participantId.filterString)
        .flatMap(_.participants.flatMap(_.synchronizers))
        .map(_.synchronizerId)
        .toSet

    def retryE(condition: => Boolean, message: => String): Either[String, Unit] =
      AdminCommandRunner
        .retryUntilTrue(consoleEnvironment.commandTimeouts.ledgerCommand)(condition)
        .toEither
        .leftMap(_ => message)

    def waitForParty(
        partyId: PartyId,
        synchronizerId: SynchronizerId,
        queriedParticipant: ParticipantReference,
    ): Either[String, Unit] =
      retryE(
        synchronizersPartyIsRegisteredOn(queriedParticipant, partyId).contains(synchronizerId),
        show"Party $partyId did not appear for $queriedParticipant on synchronizer $synchronizerId}",
      )

    def waitForPartyAndSyncWithParticipants(partyId: PartyId, synchronizerId: SynchronizerId) =
      for {
        _ <- waitForParty(partyId, synchronizerId, reference)
        _ <- retryE(
          reference.ledger_api.parties.list().map(_.party).contains(partyId),
          show"The party $partyId never appeared on the ledger API server",
        )
        // find the additional participants that are connected to the same synchronizer
        additionalSync = synchronizeParticipants.filter(
          _.synchronizers.is_connected(synchronizerId)
        )
        _ <- additionalSync.traverse_ { p =>
          waitForParty(
            partyId,
            synchronizerId,
            p,
          )
        }
      } yield ()

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
            participants,
            threshold,
            synchronizerId,
            mustFullyAuthorize,
            synchronize,
          ).toEither

          _ <- Applicative[Either[String, *]].whenA(synchronize.nonEmpty)(
            waitForPartyAndSyncWithParticipants(partyId, synchronizerId)
          )
        } yield partyId
      }
    }
  }

  /** @return
    *   if SynchronizerAlias is set, the SynchronizerId that corresponds to the alias. if
    *   SynchronizerAlias is not set, the synchronizer id of the only connected synchronizer. if the
    *   participant is connected to multiple synchronizers, it returns an error.
    */
  private def lookupOrDetectSynchronizerId(
      alias: Option[SynchronizerAlias]
  ): Either[String, SynchronizerId] = {
    lazy val singleConnectedSynchronizer = reference.synchronizers.list_connected() match {
      case Seq() =>
        Left("not connected to any synchronizer")
      case Seq(onlyOneSynchronizer) => Right(onlyOneSynchronizer.synchronizerId)
      case _multiple =>
        Left(
          "cannot automatically determine synchronizer, because participant is connected to more than 1 synchronizer"
        )
    }
    alias.map(a => Right(reference.synchronizers.id_of(a))).getOrElse(singleConnectedSynchronizer)
  }

  private def runPartyCommand(
      partyId: PartyId,
      participants: Seq[ParticipantId],
      threshold: PositiveInt,
      synchronizerId: SynchronizerId,
      mustFullyAuthorize: Boolean,
      synchronize: Option[NonNegativeDuration],
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
            threshold,
            participants.map(pid =>
              HostingParticipant(
                pid,
                if (threshold.value > 1) ParticipantPermission.Confirmation
                else ParticipantPermission.Submission,
              )
            ),
          ),
          // let the topology service determine the appropriate keys to use
          signedBy = Seq.empty,
          serial = nextSerial,
          store = TopologyStoreId.Synchronizer(synchronizerId),
          mustFullyAuthorize = mustFullyAuthorize,
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
        store = TopologyStoreId.Synchronizer(synchronizerId),
      )
      .discard
  }

  @Help.Summary("Update participant-local party details")
  @Help.Description(
    """Currently you can update only the annotations.
           |You cannot update other user attributes.
          party: party to be updated,
          modifier: a function to modify the party details, e.g.: `partyDetails => { partyDetails.copy(annotations = partyDetails.annotations.updated("a", "b").removed("c")) }`"""
  )
  def update(
      party: PartyId,
      modifier: PartyDetails => PartyDetails,
  ): PartyDetails =
    reference.ledger_api.parties.update(
      party = party,
      modifier = modifier,
    )

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
  ): String = check(FeatureFlag.Preview) {
    consoleEnvironment.run {
      reference.adminCommand(
        ParticipantAdminCommands.PartyManagement.AddPartyAsync(
          party,
          synchronizerId,
          sourceParticipant,
          serial,
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

  @Help.Summary("Waits for any topology changes to be observed", FeatureFlag.Preview)
  @Help.Description(
    "Will throw an exception if the given topology has not been observed within the given timeout."
  )
  def await_topology_observed[T <: ParticipantReference](
      partyAssignment: Set[(PartyId, T)],
      timeout: NonNegativeDuration = consoleEnvironment.commandTimeouts.bounded,
  )(implicit env: ConsoleEnvironment): Unit =
    check(FeatureFlag.Preview) {
      reference.health.wait_for_initialized()
      TopologySynchronisation.awaitTopologyObserved(reference, partyAssignment, timeout)
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
  ): NonNegativeLong = {
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
  ): NonNegativeLong = {
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
  ): NonNegativeLong = {
    val topologyTransactions: Seq[TopologyTransaction] = reference.ledger_api.updates
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
      .map(NonNegativeLong.tryCreate)
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

  @Help.Summary("Export active contracts for the given set of parties to a file.")
  @Help.Description(
    """This command exports the current Active Contract Set (ACS) of a given set of parties to a
      |GZIP compressed ACS snapshot file. Afterwards, the `import_acs` repair command imports it
      |into a participant's ACS again.
      |
      |The arguments are:
      |- parties: Identifying contracts having at least one stakeholder from the given set.
      |- synchronizerId: When defined, restricts the export to the given synchronizer.
      |- exportFilePath: The path denoting the file where the ACS snapshot will be stored.
      |- ledgerOffset: The offset at which the ACS snapshot is exported.
      |- contractSynchronizerRenames: Changes the associated synchronizer id of contracts from
      |                               one synchronizer to another based on the mapping.
      |- timeout: A timeout for this operation to complete.
      """
  )
  def export_acs(
      parties: Set[PartyId],
      synchronizerId: Option[SynchronizerId] = None,
      exportFilePath: String = "canton-acs-export.gz",
      ledgerOffset: NonNegativeLong,
      contractSynchronizerRenames: Map[SynchronizerId, SynchronizerId] = Map.empty,
      timeout: NonNegativeDuration = timeouts.unbounded,
  ): Unit =
    consoleEnvironment.run {
      val file = File(exportFilePath)
      val responseObserver = new FileStreamObserver[ExportAcsResponse](file, _.chunk)

      def call: ConsoleCommandResult[Context.CancellableContext] =
        reference.adminCommand(
          ParticipantAdminCommands.PartyManagement.ExportAcs(
            parties,
            synchronizerId,
            ledgerOffset.unwrap,
            responseObserver,
            contractSynchronizerRenames,
          )
        )

      processResult(
        call,
        responseObserver.result,
        timeout,
        request = "exporting acs",
        cleanupOnError = () => file.delete(),
      )
    }

  @Help.Summary("Export active contracts for the given set of parties to a file.")
  @Help.Description(
    """This command exports the current Active Contract Set (ACS) of a given set of parties to a
      |GZIP compressed ACS snapshot file. Afterwards, the `import_acs` repair command imports it
      |into a participant's ACS again.
      |
      |This command attempts to resolve the given instant (`topologyTransactionEffectiveTime`)
      |to a ledger offset internally. Such offset exists only after the corresponding topology
      |transaction has been recorded on the ledger.
      |This command returns an error when no offset has been found. Possible causes:
      |1. No topology transaction. Solution: Issue a topology transaction.
      |2. Topology transaction exists. Solution: Retry the command.
      |
      |The arguments are:
      |- parties: Identifying contracts having at least one stakeholder from the given set.
      |- synchronizerId: Restricts the export to the given synchronizer.
      |- topologyTransactionEffectiveTime: The effective time of a topology transaction at which
      |                                    the ACS snapshot is exported.
      |- exportFilePath: The path denoting the file where the ACS snapshot will be stored.
      |- timeout: A timeout for this operation to complete.
      """
  )
  def export_acs_at_timestamp(
      parties: Set[PartyId],
      synchronizerId: SynchronizerId,
      topologyTransactionEffectiveTime: Instant,
      exportFilePath: String = "canton-acs-export.gz",
      timeout: NonNegativeDuration = timeouts.unbounded,
  ): Unit =
    consoleEnvironment.run {
      val file = File(exportFilePath)
      val responseObserver = new FileStreamObserver[ExportAcsAtTimestampResponse](file, _.chunk)

      def call: ConsoleCommandResult[Context.CancellableContext] =
        reference.adminCommand(
          ParticipantAdminCommands.PartyManagement.ExportAcsAtTimestamp(
            parties,
            synchronizerId,
            topologyTransactionEffectiveTime,
            responseObserver,
          )
        )

      processResult(
        call,
        responseObserver.result,
        timeout,
        request = "exporting acs at timestamp",
        cleanupOnError = () => file.delete(),
      )
    }
}

object TopologySynchronisation {

  def awaitTopologyObserved[T <: ParticipantReference](
      participant: ParticipantReference,
      partyAssignment: Set[(PartyId, T)],
      timeout: NonNegativeDuration,
  )(implicit env: ConsoleEnvironment): Unit =
    TraceContext.withNewTraceContext { _ =>
      ConsoleMacros.utils.retry_until_true(timeout) {
        val partiesWithId = partyAssignment.map { case (party, participantRef) =>
          (party, participantRef.id)
        }
        env.sequencers.all.map(_.synchronizer_id).distinct.forall { synchronizerId =>
          !participant.synchronizers.is_connected(synchronizerId) || {
            val timestamp = participant.testing.fetch_synchronizer_time(synchronizerId)
            partiesWithId.subsetOf(
              participant.parties
                .list(asOf = Some(timestamp.toInstant))
                .flatMap(res => res.participants.map(par => (res.party, par.participant)))
                .toSet
            )
          }
        }
      }
    }
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
    def filterOnEffectiveTime(tx: TopologyTransaction, recordTime: Option[Instant]): Boolean =
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
