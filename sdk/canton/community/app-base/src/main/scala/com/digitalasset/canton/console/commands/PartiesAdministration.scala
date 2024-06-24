// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  ListConnectedDomainsResult,
  ListPartiesResult,
  PartyDetails,
}
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  BaseInspection,
  CantonInternalError,
  CommandFailure,
  ConsoleCommandResult,
  ConsoleEnvironment,
  ConsoleMacros,
  FeatureFlag,
  FeatureFlagFilter,
  Help,
  Helpful,
  InstanceReference,
  LocalParticipantReference,
  ParticipantReference,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.ParticipantNode
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
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
    "List active parties, their active participants, and the participants' permissions on domains."
  )
  @Help.Description(
    """Inspect the parties known by this participant as used for synchronisation.
      |The response is built from the timestamped topology transactions of each domain, excluding the
      |authorized store of the given node. For each known party, the list of active
      |participants and their permission on the domain for that party is given.
      |
      filterParty: Filter by parties starting with the given string.
      filterParticipant: Filter for parties that are hosted by a participant with an id starting with the given string
      filterDomain: Filter by domains whose id starts with the given string.
      asOf: Optional timestamp to inspect the topology state at a given point in time.
      limit: Limit on the number of parties fetched (defaults to canton.parameters.console.default-limit).

      Example: participant1.parties.list(filterParty="alice")
      """
  )
  def list(
      filterParty: String = "",
      filterParticipant: String = "",
      filterDomain: String = "",
      asOf: Option[Instant] = None,
      limit: PositiveInt = defaultLimit,
  ): Seq[ListPartiesResult] =
    consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommands.Aggregation.ListParties(
          filterDomain = filterDomain,
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
    runner: AdminCommandRunner
      & ParticipantAdministration
      & BaseLedgerApiAdministration
      & InstanceReference,
    consoleEnvironment: ConsoleEnvironment,
) extends PartiesAdministrationGroup(runner, consoleEnvironment) {

  @Help.Summary("List parties hosted by this participant")
  @Help.Description("""Inspect the parties hosted by this participant as used for synchronisation.
      |The response is built from the timestamped topology transactions of each domain, excluding the
      |authorized store of the given node. The search will include all hosted parties and is equivalent
      |to running the `list` method using the participant id of the invoking participant.
      |
      filterParty: Filter by parties starting with the given string.
      filterDomain: Filter by domains whose id starts with the given string.
      asOf: Optional timestamp to inspect the topology state at a given point in time.
      limit: How many items to return (defaults to canton.parameters.console.default-limit)

      Example: participant1.parties.hosted(filterParty="alice")""")
  def hosted(
      filterParty: String = "",
      filterDomain: String = "",
      asOf: Option[Instant] = None,
      limit: PositiveInt = defaultLimit,
  ): Seq[ListPartiesResult] = {
    list(
      filterParty,
      filterParticipant = participantId.filterString,
      filterDomain = filterDomain,
      asOf = asOf,
      limit = limit,
    )
  }

  @Help.Summary("Find a party from a filter string")
  @Help.Description(
    """Will search for all parties that match this filter string. If it finds exactly one party, it
      |will return that one. Otherwise, the function will throw."""
  )
  def find(filterParty: String): PartyId = {
    list(filterParty).map(_.party).distinct.toList match {
      case one :: Nil => one
      case Nil => throw new IllegalArgumentException(s"No party matching $filterParty")
      case more =>
        throw new IllegalArgumentException(s"Multiple parties match $filterParty: $more")
    }
  }

  @Help.Summary("Enable/add party to participant")
  @Help.Description("""This function registers a new party with the current participant within the participants
      |namespace. The function fails if the participant does not have appropriate signing keys
      |to issue the corresponding PartyToParticipant topology transaction.
      |Optionally, a local display name can be added. This display name will be exposed on the
      |ledger API party management endpoint.
      |Specifying a set of domains via the `WaitForDomain` parameter ensures that the domains have
      |enabled/added a party by the time the call returns, but other participants connected to the same domains may not
      |yet be aware of the party.
      |Additionally, a sequence of additional participants can be added to be synchronized to
      |ensure that the party is known to these participants as well before the function terminates.
      |""")
  def enable(
      name: String,
      namespace: Namespace = participantId.namespace,
      participants: Seq[ParticipantId] = Seq(participantId),
      threshold: PositiveInt = PositiveInt.one,
      displayName: Option[String] = None,
      // TODO(i10809) replace wait for domain for a clean topology synchronisation using the dispatcher info
      waitForDomain: DomainChoice = DomainChoice.Only(Seq()),
      synchronizeParticipants: Seq[ParticipantReference] = Seq(),
      groupAddressing: Boolean = false,
      mustFullyAuthorize: Boolean = true,
  ): PartyId = {

    def registered(lst: => Seq[ListPartiesResult]): Set[DomainId] = {
      lst
        .flatMap(_.participants.flatMap(_.domains))
        .map(_.domain)
        .toSet
    }
    def primaryRegistered(partyId: PartyId) =
      registered(
        list(filterParty = partyId.filterString, filterParticipant = participantId.filterString)
      )

    def primaryConnected: Either[String, Seq[ListConnectedDomainsResult]] =
      runner
        .adminCommand(ParticipantAdminCommands.DomainConnectivity.ListConnectedDomains())
        .toEither

    def findDomainIds(
        name: String,
        connected: Either[String, Seq[ListConnectedDomainsResult]],
    ): Either[String, Set[DomainId]] = {
      for {
        domainIds <- waitForDomain match {
          case DomainChoice.All =>
            connected.map(_.map(_.domainId))
          case DomainChoice.Only(Seq()) =>
            Right(Seq())
          case DomainChoice.Only(aliases) =>
            connected.flatMap { res =>
              val connectedM = res.map(x => (x.domainAlias, x.domainId)).toMap
              aliases.traverse(alias => connectedM.get(alias).toRight(s"Unknown: $alias for $name"))
            }
        }
      } yield domainIds.toSet
    }
    def retryE(condition: => Boolean, message: => String): Either[String, Unit] = {
      AdminCommandRunner
        .retryUntilTrue(consoleEnvironment.commandTimeouts.ledgerCommand)(condition)
        .toEither
        .leftMap(_ => message)
    }
    def waitForParty(
        partyId: PartyId,
        domainIds: Set[DomainId],
        registered: => Set[DomainId],
        queriedParticipant: ParticipantId = participantId,
    ): Either[String, Unit] = {
      if (domainIds.nonEmpty) {
        retryE(
          domainIds subsetOf registered,
          show"Party $partyId did not appear for $queriedParticipant on domain ${domainIds.diff(registered)}",
        )
      } else Right(())
    }
    val syncLedgerApi = waitForDomain match {
      case DomainChoice.All => true
      case DomainChoice.Only(aliases) => aliases.nonEmpty
    }
    consoleEnvironment.run {
      ConsoleCommandResult.fromEither {
        for {
          // validating party and display name here to prevent, e.g., a party being registered despite it having an invalid display name
          // assert that name is valid ParticipantId
          partyId <- UniqueIdentifier.create(name, namespace).map(PartyId(_))
          _ <- Either
            .catchOnly[IllegalArgumentException](LedgerParticipantId.assertFromString(name))
            .leftMap(_.getMessage)
          validDisplayName <- displayName.map(String255.create(_, Some("display name"))).sequence
          // find the domain ids
          domainIds <- findDomainIds(this.participantId.identifier.unwrap, primaryConnected)
          // find the domain ids the additional participants are connected to
          additionalSync <- synchronizeParticipants.traverse { p =>
            findDomainIds(
              p.name,
              Try(p.domains.list_connected()).toEither.leftMap {
                case exception @ (_: CommandFailure | _: CantonInternalError) =>
                  exception.getMessage
                case exception => throw exception
              },
            )
              .map(domains => (p, domains intersect domainIds))
          }
          _ <- runPartyCommand(
            partyId,
            participants,
            threshold,
            groupAddressing,
            mustFullyAuthorize,
          ).toEither
          _ <- validDisplayName match {
            case None => Right(())
            case Some(name) =>
              runner
                .adminCommand(
                  ParticipantAdminCommands.PartyNameManagement
                    .SetPartyDisplayName(partyId, name.unwrap)
                )
                .toEither
          }
          _ <- waitForParty(partyId, domainIds, primaryRegistered(partyId))
          _ <-
            // sync with ledger-api server if this node is connected to at least one domain
            if (syncLedgerApi && primaryConnected.exists(_.nonEmpty))
              retryE(
                runner.ledger_api.parties.list().map(_.party).contains(partyId),
                show"The party $partyId never appeared on the ledger API server",
              )
            else Right(())
          _ <- additionalSync.traverse_ { case (p, domains) =>
            waitForParty(
              partyId,
              domains,
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
      groupAddressing: Boolean,
      mustFullyAuthorize: Boolean,
  ): ConsoleCommandResult[SignedTopologyTransaction[TopologyChangeOp, PartyToParticipant]] = {

    runner
      .adminCommand(
        TopologyAdminCommands.Write.Propose(
          // TODO(#14048) properly set the serial or introduce auto-detection so we don't
          //              have to set it on the client side
          mapping = PartyToParticipant.create(
            partyId,
            None,
            threshold,
            participants.map(pid =>
              HostingParticipant(
                pid,
                if (threshold.value > 1) ParticipantPermission.Confirmation
                else ParticipantPermission.Submission,
              )
            ),
            groupAddressing,
          ),
          signedBy = Seq(this.participantId.fingerprint),
          serial = None,
          store = AuthorizedStore.filterName,
          mustFullyAuthorize = mustFullyAuthorize,
          change = TopologyChangeOp.Replace,
          forceChanges = ForceFlags.none,
        )
      )
  }

  @Help.Summary("Disable party on participant")
  // TODO(#14067): reintroduce `force` once it is implemented on the server side and threaded through properly.
  def disable(name: String /*, force: Boolean = false*/ ): Unit = {
    runner.topology.party_to_participant_mappings
      .propose_delta(
        PartyId(runner.id.member.uid.tryChangeId(name)),
        removes = List(this.participantId),
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
  ): PartyDetails = {
    runner.ledger_api.parties.update(
      party = party,
      modifier = modifier,
    )
  }

  @Help.Summary("Set party display name")
  @Help.Description(
    "Locally set the party display name (shown on the ledger-api) to the given value"
  )
  def set_display_name(party: PartyId, displayName: String): Unit = consoleEnvironment.run {
    // takes displayName as String argument which is validated at GrpcPartyNameManagementService
    runner.adminCommand(
      ParticipantAdminCommands.PartyNameManagement.SetPartyDisplayName(party, displayName)
    )
  }
}

class LocalParticipantPartiesAdministrationGroup(
    reference: LocalParticipantReference,
    runner: AdminCommandRunner
      & BaseInspection[ParticipantNode]
      & ParticipantAdministration
      & BaseLedgerApiAdministration
      & InstanceReference,
    val consoleEnvironment: ConsoleEnvironment,
    val loggerFactory: NamedLoggerFactory,
) extends ParticipantPartiesAdministrationGroup(reference.id, runner, consoleEnvironment)
    with FeatureFlagFilter {

  import runner.*

  @Help.Summary("Waits for any topology changes to be observed", FeatureFlag.Preview)
  @Help.Description(
    "Will throw an exception if the given topology has not been observed within the given timeout."
  )
  def await_topology_observed[T <: ParticipantReference](
      partyAssignment: Set[(PartyId, T)],
      timeout: NonNegativeDuration = consoleEnvironment.commandTimeouts.bounded,
  )(implicit env: ConsoleEnvironment): Unit =
    check(FeatureFlag.Preview) {
      access(node =>
        TopologySynchronisation.awaitTopologyObserved(reference, partyAssignment, timeout)
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
        env.sequencers.all.map(_.domain_id).distinct.forall { domainId =>
          !participant.domains.is_connected(domainId) || {
            val timestamp = participant.testing.fetch_domain_time(domainId)
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
