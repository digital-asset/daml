// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.traverse.*
import com.digitalasset.canton.LedgerParticipantId
import com.digitalasset.canton.admin.api.client.commands.{
  ParticipantAdminCommands,
  TopologyAdminCommands,
}
import com.digitalasset.canton.admin.api.client.data.{
  ListConnectedSynchronizersResult,
  ListPartiesResult,
  PartyDetails,
}
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  CantonInternalError,
  CommandFailure,
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
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*

import java.time.Instant
import scala.util.Try

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
  @Help.Description("""This function registers a new party with the current participant within the participants
      |namespace. The function fails if the participant does not have appropriate signing keys
      |to issue the corresponding PartyToParticipant topology transaction.
      |Specifying a set of synchronizers via the `waitForSynchronizer` parameter ensures that the synchronizers have
      |enabled/added a party by the time the call returns, but other participants connected to the same synchronizers may not
      |yet be aware of the party.
      |Additionally, a sequence of additional participants can be added to be synchronized to
      |ensure that the party is known to these participants as well before the function terminates.
      |""")
  def enable(
      name: String,
      namespace: Namespace = participantId.namespace,
      participants: Seq[ParticipantId] = Seq(participantId),
      threshold: PositiveInt = PositiveInt.one,
      // TODO(i10809) replace wait for synchronizer for a clean topology synchronisation using the dispatcher info
      waitForSynchronizer: SynchronizerChoice = SynchronizerChoice.Only(Seq()),
      synchronizeParticipants: Seq[ParticipantReference] = Seq(),
      mustFullyAuthorize: Boolean = true,
      synchronize: Option[NonNegativeDuration] = Some(
        consoleEnvironment.commandTimeouts.unbounded
      ),
  ): PartyId = {

    def registered(lst: => Seq[ListPartiesResult]): Set[SynchronizerId] =
      lst
        .flatMap(_.participants.flatMap(_.synchronizers))
        .map(_.synchronizerId)
        .toSet
    def primaryRegistered(partyId: PartyId) =
      registered(
        list(filterParty = partyId.filterString, filterParticipant = participantId.filterString)
      )

    def primaryConnected: Either[String, Seq[ListConnectedSynchronizersResult]] =
      reference
        .adminCommand(
          ParticipantAdminCommands.SynchronizerConnectivity.ListConnectedSynchronizers()
        )
        .toEither

    def findSynchronizerIds(
        name: String,
        connected: Either[String, Seq[ListConnectedSynchronizersResult]],
    ): Either[String, Set[SynchronizerId]] =
      for {
        synchronizerIds <- waitForSynchronizer match {
          case SynchronizerChoice.All =>
            connected.map(_.map(_.synchronizerId))
          case SynchronizerChoice.Only(Seq()) =>
            Right(Seq())
          case SynchronizerChoice.Only(aliases) =>
            connected.flatMap { res =>
              val connectedM = res.map(x => (x.synchronizerAlias, x.synchronizerId)).toMap
              aliases.traverse(alias => connectedM.get(alias).toRight(s"Unknown: $alias for $name"))
            }
        }
      } yield synchronizerIds.toSet
    def retryE(condition: => Boolean, message: => String): Either[String, Unit] =
      AdminCommandRunner
        .retryUntilTrue(consoleEnvironment.commandTimeouts.ledgerCommand)(condition)
        .toEither
        .leftMap(_ => message)
    def waitForParty(
        partyId: PartyId,
        synchronizerIds: Set[SynchronizerId],
        registered: => Set[SynchronizerId],
        queriedParticipant: ParticipantId = participantId,
    ): Either[String, Unit] =
      if (synchronizerIds.nonEmpty) {
        retryE(
          synchronizerIds subsetOf registered,
          show"Party $partyId did not appear for $queriedParticipant on synchronizer ${synchronizerIds
              .diff(registered)}",
        )
      } else Either.unit
    val syncLedgerApi = waitForSynchronizer match {
      case SynchronizerChoice.All => true
      case SynchronizerChoice.Only(aliases) => aliases.nonEmpty
    }
    consoleEnvironment.run {
      ConsoleCommandResult.fromEither {
        for {
          // assert that name is valid ParticipantId
          _ <- Either
            .catchOnly[IllegalArgumentException](LedgerParticipantId.assertFromString(name))
            .leftMap(_.getMessage)
          partyId <- UniqueIdentifier.create(name, namespace).map(PartyId(_))
          // find the synchronizer ids
          synchronizerIds <- findSynchronizerIds(
            this.participantId.identifier.unwrap,
            primaryConnected,
          )
          // find the synchronizer ids the additional participants are connected to
          additionalSync <- synchronizeParticipants.traverse { p =>
            findSynchronizerIds(
              p.name,
              Try(p.synchronizers.list_connected()).toEither.leftMap {
                case exception @ (_: CommandFailure | _: CantonInternalError) =>
                  exception.getMessage
                case exception => throw exception
              },
            )
              .map(synchronizers => (p, synchronizers.intersect(synchronizerIds)))
          }
          _ <- runPartyCommand(
            partyId,
            participants,
            threshold,
            mustFullyAuthorize,
            synchronize,
          ).toEither
          _ <- waitForParty(partyId, synchronizerIds, primaryRegistered(partyId))
          _ <-
            // sync with ledger-api server if this node is connected to at least one synchronizer
            if (syncLedgerApi && primaryConnected.exists(_.nonEmpty))
              retryE(
                reference.ledger_api.parties.list().map(_.party).contains(partyId),
                show"The party $partyId never appeared on the ledger API server",
              )
            else Either.unit
          _ <- additionalSync.traverse_ { case (p, synchronizers) =>
            waitForParty(
              partyId,
              synchronizers,
              registered(
                p.parties.list(
                  filterParty = partyId.filterString,
                  filterParticipant = participantId.filterString,
                )
              ),
              p.id,
            )
          }
        } yield partyId
      }
    }

  }

  private def runPartyCommand(
      partyId: PartyId,
      participants: Seq[ParticipantId],
      threshold: PositiveInt,
      mustFullyAuthorize: Boolean,
      synchronize: Option[NonNegativeDuration],
  ): ConsoleCommandResult[SignedTopologyTransaction[TopologyChangeOp, PartyToParticipant]] = {
    // determine the next serial
    val nextSerial = reference.topology.party_to_participant_mappings
      .list_from_authorized(filterParty = partyId.filterString)
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
          signedBy = Seq(this.participantId.fingerprint),
          serial = nextSerial,
          store = TopologyStoreId.Authorized,
          mustFullyAuthorize = mustFullyAuthorize,
          change = TopologyChangeOp.Replace,
          forceChanges = ForceFlags.none,
          waitToBecomeEffective = synchronize,
        )
      )
  }

  @Help.Summary("Disable party on participant")
  def disable(party: PartyId, forceFlags: ForceFlags = ForceFlags.none): Unit =
    reference.topology.party_to_participant_mappings
      .propose_delta(
        party,
        removes = List(this.participantId),
        forceFlags = forceFlags,
      )
      .discard

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
      |Performs some checks synchronously and then initiates party replication asynchronously. The returned `id`
      |parameter allows identifying asynchronous progress and errors."""
  )
  def add_party_async(
      party: PartyId,
      synchronizerId: SynchronizerId,
      sourceParticipant: Option[ParticipantId],
      serial: Option[PositiveInt],
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
