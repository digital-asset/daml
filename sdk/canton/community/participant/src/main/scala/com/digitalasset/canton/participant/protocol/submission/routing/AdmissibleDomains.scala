// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import cats.syntax.applicativeError.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.sync.TransactionRoutingError.{
  TopologyErrors,
  UnableToQueryTopologySnapshot,
}
import com.digitalasset.canton.participant.sync.{ConnectedDomainsLookup, TransactionRoutingError}
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.transaction.ParticipantAttributes
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*

import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordered.orderingToOrdered

private[routing] final class AdmissibleDomains(
    localParticipantId: ParticipantId,
    connectedDomains: ConnectedDomainsLookup,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  /** Domains that host both submitters and informees of the transaction:
    * - submitters have to be hosted on the local participant
    * - informees have to be hosted on some participant
    * It is assumed that the participant is connected to all domains in `connectedDomains`
    */
  def forParties(submitters: Set[LfPartyId], informees: Set[LfPartyId])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, TransactionRoutingError, NonEmpty[Set[DomainId]]] = {

    def queryPartyTopologySnapshotClient(
        domainPartyTopologySnapshotClient: (DomainId, PartyTopologySnapshotClient)
    ): EitherT[Future, TransactionRoutingError, Option[(DomainId, Map[LfPartyId, PartyInfo])]] = {
      val (domainId, partyTopologySnapshotClient) = domainPartyTopologySnapshotClient
      val allParties = submitters.view ++ informees.view
      partyTopologySnapshotClient
        .activeParticipantsOfPartiesWithInfo(allParties.toSeq)
        .attemptT
        .map { partyTopology =>
          val partyTopologyWithThresholds = partyTopology
            .filter { case (_, partyInfo) => partyInfo.participants.nonEmpty }

          Option.when(partyTopologyWithThresholds.nonEmpty) {
            domainId -> partyTopologyWithThresholds
          }
        }
        .leftMap { throwable =>
          logger.warn("Unable to query the topology information", throwable)
          UnableToQueryTopologySnapshot.Failed(domainId)
        }
    }

    def queryTopology()
        : EitherT[Future, TransactionRoutingError, Map[DomainId, Map[LfPartyId, PartyInfo]]] =
      connectedDomains.snapshot.view
        .mapValues(_.topologyClient.currentSnapshotApproximation)
        .toVector
        .parTraverseFilter(queryPartyTopologySnapshotClient)
        .map(_.toMap)

    def ensureAllKnown[A, E](
        required: Set[A],
        known: Set[A],
        ifUnknown: Set[A] => E,
    ): EitherT[Future, E, Unit] = {
      val unknown = required -- known
      EitherT.cond[Future](
        unknown.isEmpty,
        (),
        ifUnknown(unknown),
      )
    }

    def ensureAllSubmittersAreKnown(
        knownParties: Set[LfPartyId]
    ): EitherT[Future, TransactionRoutingError, Unit] =
      ensureAllKnown(
        required = submitters,
        known = knownParties,
        ifUnknown = TopologyErrors.UnknownSubmitters.Error.apply,
      )

    def ensureAllInformeesAreKnown(
        knownParties: Set[LfPartyId]
    ): EitherT[Future, TransactionRoutingError, Unit] =
      ensureAllKnown(
        required = informees,
        known = knownParties,
        ifUnknown = TopologyErrors.UnknownInformees.Error.apply,
      )

    def ensureNonEmpty[I[_] <: collection.immutable.Iterable[?], A, E](
        iterable: I[A],
        ifEmpty: => E,
    ): EitherT[Future, E, NonEmpty[I[A]]] =
      EitherT.fromEither[Future](NonEmpty.from(iterable).toRight(ifEmpty))

    def domainWithAll(parties: Set[LfPartyId])(
        topology: (DomainId, Map[LfPartyId, PartyInfo])
    ): Boolean =
      parties.subsetOf(topology._2.keySet)

    def domainsWithAll(
        parties: Set[LfPartyId],
        topology: Map[DomainId, Map[LfPartyId, PartyInfo]],
        ifEmpty: Set[DomainId] => TransactionRoutingError,
    ): EitherT[Future, TransactionRoutingError, NonEmpty[
      Map[DomainId, Map[LfPartyId, PartyInfo]]
    ]] = {
      val domainsWithAllParties = topology.filter(domainWithAll(parties))
      ensureNonEmpty(domainsWithAllParties, ifEmpty(topology.keySet))
    }

    def domainsWithAllSubmitters(
        topology: Map[DomainId, Map[LfPartyId, PartyInfo]]
    ): EitherT[Future, TransactionRoutingError, NonEmpty[
      Map[DomainId, Map[LfPartyId, PartyInfo]]
    ]] =
      domainsWithAll(
        parties = submitters,
        topology = topology,
        ifEmpty = TopologyErrors.SubmittersNotActive.Error(_, submitters),
      )

    def domainsWithAllInformees(
        topology: Map[DomainId, Map[LfPartyId, PartyInfo]]
    ): EitherT[Future, TransactionRoutingError, NonEmpty[
      Map[DomainId, Map[LfPartyId, PartyInfo]]
    ]] =
      domainsWithAll(
        parties = informees,
        topology = topology,
        ifEmpty = TopologyErrors.InformeesNotActive.Error(_, informees),
      )

    def suitableDomains(
        domainsWithAllSubmitters: NonEmpty[Map[DomainId, Map[LfPartyId, PartyInfo]]]
    ): EitherT[Future, TransactionRoutingError, NonEmpty[Set[DomainId]]] = {
      logger.debug(
        s"Checking whether one domain in ${domainsWithAllSubmitters.keys} is suitable for submission"
      )

      // Return true if all submitters are locally hosted with correct permissions
      def canUseDomain(domainId: DomainId, parties: Map[LfPartyId, PartyInfo]): Boolean = {
        // We keep only the relevant topology (submitter on the local participant)
        val locallyHostedSubmitters: Map[LfPartyId, (ParticipantAttributes, PositiveInt)] =
          parties.toSeq.mapFilter { case (party, partyInfo) =>
            for {
              permissions <- partyInfo.participants.get(localParticipantId)
              _ <- Option.when(submitters.contains(party))(())
            } yield (party, (permissions, partyInfo.threshold))
          }.toMap

        val unknownSubmitters: Set[LfPartyId] = submitters.diff(locallyHostedSubmitters.keySet)

        val incorrectPermissionSubmitters = locallyHostedSubmitters.toSeq.flatMap {
          case (party, (permissions, threshold)) =>
            if (permissions.permission < Submission)
              List(s"submitter $party has permissions=${permissions.permission}")
            else if (threshold > PositiveInt.one)
              List(s"submitter $party has threshold=$threshold")
            else Nil
        }

        val canUseDomain = unknownSubmitters.isEmpty && incorrectPermissionSubmitters.isEmpty

        if (!canUseDomain) {
          val context = Map(
            "unknown submitters" -> unknownSubmitters,
            "incorrect permissions" -> incorrectPermissionSubmitters,
          )
          logger.debug(s"Cannot use domain $domainId: $context")
        }

        canUseDomain
      }

      val suitableDomains = for {
        (domainId, topology) <- domainsWithAllSubmitters
        if canUseDomain(domainId, topology)
      } yield domainId

      ensureNonEmpty(suitableDomains.toSet, noDomainWhereAllSubmittersCanSubmit)
    }

    def commonDomainIds(
        submittersDomainIds: Set[DomainId],
        informeesDomainIds: Set[DomainId],
    ): EitherT[Future, TransactionRoutingError, NonEmpty[Set[DomainId]]] =
      ensureNonEmpty(
        submittersDomainIds.intersect(informeesDomainIds),
        TopologyErrors.NoCommonDomain.Error(submitters, informees),
      )

    def noDomainWhereAllSubmittersCanSubmit: TransactionRoutingError =
      submitters.toSeq match {
        case Seq(one) => TopologyErrors.NoDomainOnWhichAllSubmittersCanSubmit.NotAllowed(one)
        case some => TopologyErrors.NoDomainOnWhichAllSubmittersCanSubmit.NoSuitableDomain(some)
      }

    for {
      topology <- queryTopology()
      _ = logger.debug(s"Topology queried for the following domains: ${topology.keySet}")
      knownParties = topology.view.values.map(_.keySet).fold(Set.empty)(_ ++ _)
      _ <- ensureAllSubmittersAreKnown(knownParties)
      _ <- ensureAllInformeesAreKnown(knownParties)

      domainsWithAllSubmitters <- domainsWithAllSubmitters(topology)
      _ = logger.debug(s"Domains with all submitters: ${domainsWithAllSubmitters.keySet}")

      domainsWithAllInformees <- domainsWithAllInformees(topology)
      _ = logger.debug(s"Domains with all informees: ${domainsWithAllInformees.keySet}")

      submittersDomainIds <- suitableDomains(domainsWithAllSubmitters)
      informeesDomainIds = domainsWithAllInformees.keySet
      commonDomainIds <- commonDomainIds(submittersDomainIds, informeesDomainIds)
    } yield commonDomainIds

  }
}
