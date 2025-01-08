// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import cats.syntax.applicativeError.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
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
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext
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
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, NonEmpty[Set[SynchronizerId]]] = {

    def queryPartyTopologySnapshotClient(
        domainPartyTopologySnapshotClient: (SynchronizerId, PartyTopologySnapshotClient)
    ): EitherT[FutureUnlessShutdown, TransactionRoutingError, Option[
      (SynchronizerId, Map[LfPartyId, PartyInfo])
    ]] = {
      val (synchronizerId, partyTopologySnapshotClient) = domainPartyTopologySnapshotClient
      val allParties = submitters.view ++ informees.view
      partyTopologySnapshotClient
        .activeParticipantsOfPartiesWithInfo(allParties.toSeq)
        .attemptT
        .map { partyTopology =>
          val partyTopologyWithThresholds = partyTopology
            .filter { case (_, partyInfo) => partyInfo.participants.nonEmpty }

          Option.when(partyTopologyWithThresholds.nonEmpty) {
            synchronizerId -> partyTopologyWithThresholds
          }
        }
        .leftMap { throwable =>
          logger.warn("Unable to query the topology information", throwable)
          UnableToQueryTopologySnapshot.Failed(synchronizerId)
        }
    }

    def queryTopology(): EitherT[FutureUnlessShutdown, TransactionRoutingError, Map[
      SynchronizerId,
      Map[LfPartyId, PartyInfo],
    ]] =
      connectedDomains.snapshot.view
        .mapValues(_.topologyClient.currentSnapshotApproximation)
        .toVector
        .parTraverseFilter(queryPartyTopologySnapshotClient)
        .map(_.toMap)

    def ensureAllKnown[A, E](
        required: Set[A],
        known: Set[A],
        ifUnknown: Set[A] => E,
    ): EitherT[FutureUnlessShutdown, E, Unit] = {
      val unknown = required -- known
      EitherT.cond[FutureUnlessShutdown](
        unknown.isEmpty,
        (),
        ifUnknown(unknown),
      )
    }

    def ensureAllSubmittersAreKnown(
        knownParties: Set[LfPartyId]
    ): EitherT[FutureUnlessShutdown, TransactionRoutingError, Unit] =
      ensureAllKnown(
        required = submitters,
        known = knownParties,
        ifUnknown = TopologyErrors.UnknownSubmitters.Error.apply,
      )

    def ensureAllInformeesAreKnown(
        knownParties: Set[LfPartyId]
    ): EitherT[FutureUnlessShutdown, TransactionRoutingError, Unit] =
      ensureAllKnown(
        required = informees,
        known = knownParties,
        ifUnknown = TopologyErrors.UnknownInformees.Error.apply,
      )

    def ensureNonEmpty[I[_] <: collection.immutable.Iterable[?], A, E](
        iterable: I[A],
        ifEmpty: => E,
    ): EitherT[FutureUnlessShutdown, E, NonEmpty[I[A]]] =
      EitherT.fromEither[FutureUnlessShutdown](NonEmpty.from(iterable).toRight(ifEmpty))

    def domainWithAll(parties: Set[LfPartyId])(
        topology: (SynchronizerId, Map[LfPartyId, PartyInfo])
    ): Boolean =
      parties.subsetOf(topology._2.keySet)

    def domainsWithAll(
        parties: Set[LfPartyId],
        topology: Map[SynchronizerId, Map[LfPartyId, PartyInfo]],
        ifEmpty: Set[SynchronizerId] => TransactionRoutingError,
    ): EitherT[FutureUnlessShutdown, TransactionRoutingError, NonEmpty[
      Map[SynchronizerId, Map[LfPartyId, PartyInfo]]
    ]] = {
      val domainsWithAllParties = topology.filter(domainWithAll(parties))
      ensureNonEmpty(domainsWithAllParties, ifEmpty(topology.keySet))
    }

    def domainsWithAllSubmitters(
        topology: Map[SynchronizerId, Map[LfPartyId, PartyInfo]]
    ): EitherT[FutureUnlessShutdown, TransactionRoutingError, NonEmpty[
      Map[SynchronizerId, Map[LfPartyId, PartyInfo]]
    ]] =
      domainsWithAll(
        parties = submitters,
        topology = topology,
        ifEmpty = TopologyErrors.SubmittersNotActive.Error(_, submitters),
      )

    def domainsWithAllInformees(
        topology: Map[SynchronizerId, Map[LfPartyId, PartyInfo]]
    ): EitherT[FutureUnlessShutdown, TransactionRoutingError, NonEmpty[
      Map[SynchronizerId, Map[LfPartyId, PartyInfo]]
    ]] =
      domainsWithAll(
        parties = informees,
        topology = topology,
        ifEmpty = TopologyErrors.InformeesNotActive.Error(_, informees),
      )

    def suitableDomains(
        domainsWithAllSubmitters: NonEmpty[Map[SynchronizerId, Map[LfPartyId, PartyInfo]]]
    ): EitherT[FutureUnlessShutdown, TransactionRoutingError, NonEmpty[Set[SynchronizerId]]] = {
      logger.debug(
        s"Checking whether one domain in ${domainsWithAllSubmitters.keys} is suitable for submission"
      )

      // Return true if all submitters are locally hosted with correct permissions
      def canUseDomain(
          synchronizerId: SynchronizerId,
          parties: Map[LfPartyId, PartyInfo],
      ): Boolean = {
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
          logger.debug(s"Cannot use domain $synchronizerId: $context")
        }

        canUseDomain
      }

      val suitableDomains = for {
        (synchronizerId, topology) <- domainsWithAllSubmitters
        if canUseDomain(synchronizerId, topology)
      } yield synchronizerId

      ensureNonEmpty(suitableDomains.toSet, noDomainWhereAllSubmittersCanSubmit)
    }

    def commonsynchronizerIds(
        submitterssynchronizerIds: Set[SynchronizerId],
        informeessynchronizerIds: Set[SynchronizerId],
    ): EitherT[FutureUnlessShutdown, TransactionRoutingError, NonEmpty[Set[SynchronizerId]]] =
      ensureNonEmpty(
        submitterssynchronizerIds.intersect(informeessynchronizerIds),
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

      submitterssynchronizerIds <- suitableDomains(domainsWithAllSubmitters)
      informeessynchronizerIds = domainsWithAllInformees.keySet
      commonsynchronizerIds <- commonsynchronizerIds(
        submitterssynchronizerIds,
        informeessynchronizerIds,
      )
    } yield commonsynchronizerIds

  }
}
