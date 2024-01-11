// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import cats.syntax.applicativeError.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.sync.TransactionRoutingError.{
  TopologyErrors,
  UnableToQueryTopologySnapshot,
}
import com.digitalasset.canton.participant.sync.{ConnectedDomainsLookup, TransactionRoutingError}
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient
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

  // Not meant for public consumption, it's just for conciseness in this class
  // A `PartyTopology` is something gathered for each domain which maps, for
  // every party of the domain, on which participant it's hosted and with what
  // permissions and trust level (i.e. the `ParticipantAttributes`)
  private type PartyTopology = Map[LfPartyId, Map[ParticipantId, ParticipantAttributes]]

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
    ): EitherT[Future, TransactionRoutingError, Option[(DomainId, PartyTopology)]] = {
      val (domainId, partyTopologySnapshotClient) = domainPartyTopologySnapshotClient
      val allParties = submitters.view ++ informees.view
      partyTopologySnapshotClient
        .activeParticipantsOfPartiesWithAttributes(allParties.toSeq)
        .attemptT
        .map(_.filter { case (_, attributes) => attributes.nonEmpty })
        .map(partyTopology =>
          if (partyTopology.isEmpty) None
          else Some(domainId -> partyTopology)
        )
        .leftMap { throwable =>
          logger.warn("Unable to query the topology information", throwable)
          UnableToQueryTopologySnapshot.Failed(domainId)
        }
    }

    def queryTopology(): EitherT[Future, TransactionRoutingError, Map[DomainId, PartyTopology]] =
      connectedDomains.snapshot.view
        .mapValues(_.topologyClient.currentSnapshotApproximation)
        .toVector
        .parTraverseFilter(queryPartyTopologySnapshotClient)
        .map(_.toMap)

    def ensureAllKnown[A, E](
        required: Set[A],
        known: Set[A],
        ifUnknown: Set[A] => E,
    ): EitherT[Future, E, Unit] =
      EitherT.fromEither[Future] {
        val unknown = required -- known
        if (unknown.isEmpty) {
          Right(())
        } else {
          Left(ifUnknown(unknown))
        }
      }

    def ensureAllSubmittersAreKnown(
        knownParties: Set[LfPartyId]
    ): EitherT[Future, TransactionRoutingError, Unit] =
      ensureAllKnown(
        required = submitters,
        known = knownParties,
        ifUnknown = TopologyErrors.UnknownSubmitters.Error,
      )

    def ensureAllInformeesAreKnown(
        knownParties: Set[LfPartyId]
    ): EitherT[Future, TransactionRoutingError, Unit] =
      ensureAllKnown(
        required = informees,
        known = knownParties,
        ifUnknown = TopologyErrors.UnknownInformees.Error,
      )

    def ensureNonEmpty[I[_] <: collection.immutable.Iterable[?], A, E](
        iterable: I[A],
        ifEmpty: => E,
    ): EitherT[Future, E, NonEmpty[I[A]]] =
      EitherT.fromEither[Future](NonEmpty.from(iterable).toRight(ifEmpty))

    def domainWithAll(parties: Set[LfPartyId])(topology: (DomainId, PartyTopology)): Boolean =
      parties.subsetOf(topology._2.keySet)

    def domainsWithAll(
        parties: Set[LfPartyId],
        topology: Map[DomainId, PartyTopology],
        ifEmpty: Set[DomainId] => TransactionRoutingError,
    ): EitherT[Future, TransactionRoutingError, NonEmpty[Map[DomainId, PartyTopology]]] = {
      val domainsWithAllParties = topology.filter(domainWithAll(parties))
      ensureNonEmpty(domainsWithAllParties, ifEmpty(topology.keySet))
    }

    def domainsWithAllSubmitters(
        topology: Map[DomainId, PartyTopology]
    ): EitherT[Future, TransactionRoutingError, NonEmpty[Map[DomainId, PartyTopology]]] =
      domainsWithAll(
        parties = submitters,
        topology = topology,
        ifEmpty = TopologyErrors.SubmittersNotActive.Error(_, submitters),
      )

    def domainsWithAllInformees(
        topology: Map[DomainId, PartyTopology]
    ): EitherT[Future, TransactionRoutingError, NonEmpty[Map[DomainId, PartyTopology]]] =
      domainsWithAll(
        parties = informees,
        topology = topology,
        ifEmpty = TopologyErrors.InformeesNotActive.Error(_, informees),
      )

    def suitableDomains(
        domainsWithAllSubmitters: NonEmpty[Map[DomainId, PartyTopology]]
    ): EitherT[Future, TransactionRoutingError, NonEmpty[Set[DomainId]]] = {
      val suitableDomains =
        for {
          (domainId, topology) <- domainsWithAllSubmitters
          (partyId, participants) <- topology
          if submitters.contains(partyId)
          (participantId, attributes) <- participants
          if participantId == localParticipantId && attributes.permission >= Submission
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
      knownParties = topology.view.values.map(_.keySet).fold(Set.empty)(_ ++ _)
      _ <- ensureAllSubmittersAreKnown(knownParties)
      _ <- ensureAllInformeesAreKnown(knownParties)
      domainsWithAllSubmitters <- domainsWithAllSubmitters(topology)
      domainsWithAllInformees <- domainsWithAllInformees(topology)
      submittersDomainIds <- suitableDomains(domainsWithAllSubmitters)
      informeesDomainIds = domainsWithAllInformees.keySet
      commonDomainIds <- commonDomainIds(submittersDomainIds, informeesDomainIds)
    } yield commonDomainIds

  }
}
