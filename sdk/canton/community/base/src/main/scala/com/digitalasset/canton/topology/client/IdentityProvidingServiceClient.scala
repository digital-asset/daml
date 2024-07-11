// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import cats.Monad
import cats.data.EitherT
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.digitalasset.canton.concurrent.HasFutureSupervision
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{EncryptionPublicKey, SigningPublicKey}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.protocol.{
  DynamicDomainParameters,
  DynamicDomainParametersWithValidity,
  DynamicSequencingParametersWithValidity,
}
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.time.DomainTimeTracker
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.{
  AuthorityOfResponse,
  PartyInfo,
}
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  TopologyTransactionProcessingSubscriber,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.SingleUseCell
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPartyId, checked}
import com.digitalasset.daml.lf.data.Ref.PackageId

import scala.collection.concurrent.TrieMap
import scala.collection.immutable
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

// architecture-handbook-entry-begin: IdentityProvidingServiceClient

/** Client side API for the Identity Providing Service. This API is used to get information about the layout of
  * the domains, such as party-participant relationships, used encryption and signing keys,
  * package information, participant states, domain parameters, and so on.
  */
class IdentityProvidingServiceClient {

  private val domains = TrieMap.empty[DomainId, DomainTopologyClient]

  def add(domainClient: DomainTopologyClient): this.type = {
    domains += (domainClient.domainId -> domainClient)
    this
  }

  def allDomains: Iterable[DomainTopologyClient] = domains.values

  def tryForDomain(domain: DomainId): DomainTopologyClient =
    domains.getOrElse(domain, sys.error("unknown domain " + domain.toString))

  def forDomain(domain: DomainId): Option[DomainTopologyClient] = domains.get(domain)

}

trait TopologyClientApi[+T] { this: HasFutureSupervision =>

  /** The domain this client applies to */
  def domainId: DomainId

  /** Our current snapshot approximation
    *
    * As topology transactions are future dated (to prevent sequential bottlenecks), we do
    * have to "guess" the current state, as time is defined by the sequencer after
    * we've sent the transaction. Therefore, this function will return the
    * best snapshot approximation known.
    *
    * The snapshot returned by this method should be used when preparing a transaction or transfer request (Phase 1).
    * It must not be used when validating a request (Phase 2 - 7); instead, use one of the `snapshot` methods with the request timestamp.
    */
  def currentSnapshotApproximation(implicit traceContext: TraceContext): T

  /** Possibly future dated head snapshot
    *
    * As we future date topology transactions, the head snapshot is our latest knowledge of the topology state,
    * but as it can be still future dated, we need to be careful when actually using it: the state might not
    * yet be active, as the topology transactions are future dated. Therefore, do not act towards the sequencer
    * using this snapshot, but use the currentSnapshotApproximation instead.
    */
  def headSnapshot(implicit traceContext: TraceContext): T = checked(
    trySnapshot(topologyKnownUntilTimestamp)
  )

  /** The approximate timestamp
    *
    * This is either the last observed sequencer timestamp OR the effective timestamp after we observed
    * the time difference of (effective - sequencer = epsilon) to elapse
    */
  def approximateTimestamp: CantonTimestamp

  /** The most recently observed effective timestamp
    *
    * The effective timestamp is sequencer_time + epsilon(sequencer_time), where
    * epsilon is given by the topology change delay time, defined using the domain parameters.
    *
    * This is the highest timestamp for which we can serve snapshots
    */
  def topologyKnownUntilTimestamp: CantonTimestamp

  /** Returns true if the topology information at the passed timestamp is already known */
  def snapshotAvailable(timestamp: CantonTimestamp): Boolean

  /** Returns the topology information at a certain point in time
    *
    * Use this method if you are sure to be synchronized with the topology state updates.
    * The method will block & wait for an update, but emit a warning if it is not available
    *
    * The snapshot returned by this method should be used for validating transaction and transfer requests (Phase 2 - 7).
    * Use the request timestamp as parameter for this method.
    * Do not use a response or result timestamp, because all validation steps must use the same topology snapshot.
    */
  def snapshot(timestamp: CantonTimestamp)(implicit traceContext: TraceContext): Future[T]
  def snapshotUS(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[T]

  /** Waits until a snapshot is available
    *
    * The snapshot returned by this method should be used for validating transaction and transfer requests (Phase 2 - 7).
    * Use the request timestamp as parameter for this method.
    * Do not use a response or result timestamp, because all validation steps must use the same topology snapshot.
    */
  def awaitSnapshot(timestamp: CantonTimestamp)(implicit traceContext: TraceContext): Future[T]

  /** Supervised version of [[awaitSnapshot]] */
  def awaitSnapshotSupervised(description: => String, warnAfter: Duration = 30.seconds)(
      timestamp: CantonTimestamp
  )(implicit
      traceContext: TraceContext
  ): Future[T] = supervised(description, warnAfter)(awaitSnapshot(timestamp))

  /** Shutdown safe version of await snapshot */
  def awaitSnapshotUS(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[T]

  /** Supervised version of [[awaitSnapshotUS]] */
  def awaitSnapshotUSSupervised(description: => String, warnAfter: Duration = 30.seconds)(
      timestamp: CantonTimestamp
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[T] = supervisedUS(description, warnAfter)(awaitSnapshotUS(timestamp))

  /** Returns the topology information at a certain point in time
    *
    * Fails with an exception if the state is not yet known.
    *
    * The snapshot returned by this method should be used for validating transaction and transfer requests (Phase 2 - 7).
    * Use the request timestamp as parameter for this method.
    * Do not use a response or result timestamp, because all validation steps must use the same topology snapshot.
    */
  def trySnapshot(timestamp: CantonTimestamp)(implicit traceContext: TraceContext): T

  /** Returns an optional future which will complete when the timestamp has been observed
    *
    * If the timestamp is already observed, we return None.
    *
    * Note that this function allows to wait for effective time (true) and sequenced time (false).
    * If we wait for effective time, we wait until the topology snapshot for that given
    * point in time is known. As we future date topology transactions (to avoid bottlenecks),
    * this might be before we actually observed a sequencing timestamp.
    */
  def awaitTimestamp(
      timestamp: CantonTimestamp,
      waitForEffectiveTime: Boolean,
  )(implicit traceContext: TraceContext): Option[Future[Unit]]

  def awaitTimestampUS(
      timestamp: CantonTimestamp,
      waitForEffectiveTime: Boolean,
  )(implicit traceContext: TraceContext): Option[FutureUnlessShutdown[Unit]]

}

/** The client that provides the topology information on a per domain basis
  */
trait DomainTopologyClient extends TopologyClientApi[TopologySnapshot] with AutoCloseable {
  this: HasFutureSupervision =>

  /** Wait for a condition to become true according to the current snapshot approximation
    *
    * @return true if the condition became true, false if it timed out
    */
  def await(condition: TopologySnapshot => Future[Boolean], timeout: Duration)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean]

}

trait BaseTopologySnapshotClient {

  protected implicit def executionContext: ExecutionContext

  /** The official timestamp corresponding to this snapshot */
  def timestamp: CantonTimestamp

  /** Internally used reference time (representing when the last change happened that affected this snapshot) */
  def referenceTime: CantonTimestamp = timestamp

}

/** The subset of the topology client providing party to participant mapping information */
trait PartyTopologySnapshotClient {

  this: BaseTopologySnapshotClient =>

  /** Load the set of active participants for the given parties */
  def activeParticipantsOfParties(
      parties: Seq[LfPartyId]
  )(implicit traceContext: TraceContext): Future[Map[LfPartyId, Set[ParticipantId]]]

  def activeParticipantsOfPartiesWithAttributes(
      parties: Seq[LfPartyId]
  )(implicit
      traceContext: TraceContext
  ): Future[Map[LfPartyId, Map[ParticipantId, ParticipantAttributes]]]

  /** Returns the set of active participants the given party is represented by as of the snapshot timestamp
    *
    * Should never return a PartyParticipantRelationship where ParticipantPermission is DISABLED.
    */
  def activeParticipantsOf(
      party: LfPartyId
  )(implicit traceContext: TraceContext): Future[Map[ParticipantId, ParticipantAttributes]]

  /** Returns Right if all parties have at least an active participant passing the check. Otherwise, all parties not passing are passed as Left */
  def allHaveActiveParticipants(
      parties: Set[LfPartyId],
      check: (ParticipantPermission => Boolean) = _ => true,
  )(implicit traceContext: TraceContext): EitherT[Future, Set[LfPartyId], Unit]

  /** Returns the consortium thresholds (how many votes from different participants that host the consortium party
    * are required for the confirmation to become valid). For normal parties returns 1.
    */
  def consortiumThresholds(parties: Set[LfPartyId])(implicit
      traceContext: TraceContext
  ): Future[Map[LfPartyId, PositiveInt]]

  /** Returns the Authority-Of delegations for consortium parties. Non-consortium parties delegate to themselves
    * with threshold one
    */
  def authorityOf(parties: Set[LfPartyId])(implicit
      traceContext: TraceContext
  ): Future[AuthorityOfResponse]

  /** Returns true if there is at least one participant that satisfies the predicate */
  def isHostedByAtLeastOneParticipantF(
      parties: Set[LfPartyId],
      check: (LfPartyId, ParticipantAttributes) => Boolean,
  )(implicit traceContext: TraceContext): Future[Set[LfPartyId]]

  /** Returns the participant permission for that particular participant (if there is one) */
  def hostedOn(
      partyIds: Set[LfPartyId],
      participantId: ParticipantId,
  )(implicit traceContext: TraceContext): Future[Map[LfPartyId, ParticipantAttributes]]

  /** Returns true of all given party ids are hosted on a certain participant */
  def allHostedOn(
      partyIds: Set[LfPartyId],
      participantId: ParticipantId,
      permissionCheck: ParticipantAttributes => Boolean = _ => true,
  )(implicit traceContext: TraceContext): Future[Boolean]

  /** Returns whether a participant can confirm on behalf of a party. */
  def canConfirm(
      participant: ParticipantId,
      parties: Set[LfPartyId],
  )(implicit traceContext: TraceContext): Future[Set[LfPartyId]]

  /** Returns the subset of parties the given participant can NOT submit on behalf of */
  def canNotSubmit(
      participant: ParticipantId,
      parties: Seq[LfPartyId],
  )(implicit traceContext: TraceContext): Future[immutable.Iterable[LfPartyId]]

  /** Returns all active participants of all the given parties. Returns a Left if some of the parties don't have active
    * participants, in which case the parties with missing active participants are returned. Note that it will return
    * an empty set as a Right when given an empty list of parties.
    */
  def activeParticipantsOfAll(
      parties: List[LfPartyId]
  )(implicit traceContext: TraceContext): EitherT[Future, Set[LfPartyId], Set[ParticipantId]]

  def partiesWithGroupAddressing(
      parties: Seq[LfPartyId]
  )(implicit traceContext: TraceContext): Future[Set[LfPartyId]]

  def activeParticipantsOfPartiesWithGroupAddressing(
      parties: Seq[LfPartyId]
  )(implicit traceContext: TraceContext): Future[Map[LfPartyId, Set[ParticipantId]]]

  /** Returns a list of all known parties on this domain */
  def inspectKnownParties(
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): Future[
    Set[PartyId]
  ] // TODO(#14048): Decide on whether to standarize APIs on LfPartyId or PartyId and unify interfaces

}

object PartyTopologySnapshotClient {
  final case class AuthorityOfDelegation(expected: Set[LfPartyId], threshold: PositiveInt)

  def nonConsortiumPartyDelegation(partyId: LfPartyId): AuthorityOfDelegation =
    AuthorityOfDelegation(Set(partyId), PositiveInt.one)

  final case class AuthorityOfResponse(response: Map[LfPartyId, AuthorityOfDelegation])

  final case class PartyInfo(
      groupAddressing: Boolean,
      threshold: PositiveInt, // > 1 for consortium parties
      participants: Map[ParticipantId, ParticipantAttributes],
  )

  object PartyInfo {
    def nonConsortiumPartyInfo(participants: Map[ParticipantId, ParticipantAttributes]): PartyInfo =
      PartyInfo(groupAddressing = false, threshold = PositiveInt.one, participants = participants)

    lazy val EmptyPartyInfo: PartyInfo = nonConsortiumPartyInfo(Map.empty)
  }
}

/** The subset of the topology client, providing signing and encryption key information */
trait KeyTopologySnapshotClient {

  this: BaseTopologySnapshotClient =>

  /** returns newest signing public key */
  def signingKey(owner: Member)(implicit
      traceContext: TraceContext
  ): Future[Option[SigningPublicKey]]

  /** returns all signing keys */
  def signingKeys(owner: Member)(implicit traceContext: TraceContext): Future[Seq[SigningPublicKey]]

  def signingKeysUS(owner: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[SigningPublicKey]]

  def signingKeys(members: Seq[Member])(implicit
      traceContext: TraceContext
  ): Future[Map[Member, Seq[SigningPublicKey]]]

  /** returns newest encryption public key */
  def encryptionKey(owner: Member)(implicit
      traceContext: TraceContext
  ): Future[Option[EncryptionPublicKey]]

  /** returns newest encryption public key */
  def encryptionKey(members: Seq[Member])(implicit
      traceContext: TraceContext
  ): Future[Map[Member, EncryptionPublicKey]]

  /** returns all encryption keys */
  def encryptionKeys(owner: Member)(implicit
      traceContext: TraceContext
  ): Future[Seq[EncryptionPublicKey]]

  def encryptionKeys(members: Seq[Member])(implicit
      traceContext: TraceContext
  ): Future[Map[Member, Seq[EncryptionPublicKey]]]

  /** Returns a list of all known parties on this domain */
  def inspectKeys(
      filterOwner: String,
      filterOwnerType: Option[MemberCode],
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Map[Member, KeyCollection]]

}

/** The subset of the topology client, providing participant state information */
trait ParticipantTopologySnapshotClient {

  this: BaseTopologySnapshotClient =>

  /** Checks whether the provided participant exists and is active */
  def isParticipantActive(participantId: ParticipantId)(implicit
      traceContext: TraceContext
  ): Future[Boolean]

  /** Checks whether the provided participant exists, is active and can login at the given point in time
    *
    * (loginAfter is before timestamp)
    */
  def isParticipantActiveAndCanLoginAt(
      participantId: ParticipantId,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Boolean]

}

/** The subset of the topology client providing mediator state information */
trait MediatorDomainStateClient {
  this: BaseTopologySnapshotClient =>

  def mediatorGroups()(implicit traceContext: TraceContext): Future[Seq[MediatorGroup]]

  def isMediatorActive(mediatorId: MediatorId)(implicit
      traceContext: TraceContext
  ): Future[Boolean] =
    mediatorGroups().map(_.exists { group =>
      // Note: mediator in group.passive should still be able to authenticate and process ConfirmationResponses,
      // only sending the verdicts is disabled and verdicts from a passive mediator should not pass the checks
      group.isActive && (group.active.contains(mediatorId) || group.passive.contains(mediatorId))
    })

  def isMediatorActive(
      mediator: MediatorGroupRecipient
  )(implicit traceContext: TraceContext): Future[Boolean] =
    mediatorGroup(mediator.group).map {
      case Some(group) => group.isActive
      case None => false
    }

  def mediatorGroupsOfAll(
      groups: Seq[MediatorGroupIndex]
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, Seq[MediatorGroupIndex], Seq[MediatorGroup]] =
    if (groups.isEmpty) EitherT.rightT(Seq.empty)
    else
      EitherT(
        mediatorGroups()
          .map { mediatorGroups =>
            val existingGroupIndices = mediatorGroups.map(_.index)
            val nonExisting = groups.filterNot(existingGroupIndices.contains)
            Either.cond(
              nonExisting.isEmpty,
              mediatorGroups.filter(g => groups.contains(g.index)),
              nonExisting,
            )
          }
      )

  def mediatorGroup(
      index: MediatorGroupIndex
  )(implicit traceContext: TraceContext): Future[Option[MediatorGroup]] = {
    mediatorGroups().map(_.find(_.index == index))
  }
}

/** The subset of the topology client providing sequencer state information */
trait SequencerDomainStateClient {
  this: BaseTopologySnapshotClient =>

  /** returns the sequencer group */
  def sequencerGroup()(implicit traceContext: TraceContext): Future[Option[SequencerGroup]]
}

trait VettedPackagesSnapshotClient {

  this: BaseTopologySnapshotClient =>

  /** Returns the set of packages that are not vetted by the given participant
    *
    * @param participantId the participant for which we want to check the package vettings
    * @param packages the set of packages that should be vetted
    * @return Right the set of unvetted packages (which is empty if all packages are vetted)
    *         Left if a package is missing locally such that we can not verify the vetting state of the package dependencies
    */
  def findUnvettedPackagesOrDependencies(
      participantId: ParticipantId,
      packages: Set[PackageId],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, PackageId, Set[PackageId]]
}

trait DomainGovernanceSnapshotClient {
  this: BaseTopologySnapshotClient with NamedLogging =>

  def trafficControlParameters[A](
      protocolVersion: ProtocolVersion,
      warnOnUsingDefault: Boolean = true,
  )(implicit tc: TraceContext): FutureUnlessShutdown[Option[TrafficControlParameters]] =
    FutureUnlessShutdown.outcomeF {
      findDynamicDomainParametersOrDefault(protocolVersion, warnOnUsingDefault = warnOnUsingDefault)
        .map(_.trafficControlParameters)
    }

  def findDynamicDomainParametersOrDefault(
      protocolVersion: ProtocolVersion,
      warnOnUsingDefault: Boolean = true,
  )(implicit traceContext: TraceContext): Future[DynamicDomainParameters] =
    findDynamicDomainParameters().map {
      case Right(value) => value.parameters
      case Left(_) =>
        if (warnOnUsingDefault) {
          logger.warn(s"Unexpectedly using default domain parameters at ${timestamp}")
        }

        DynamicDomainParameters.initialValues(
          // we must use zero as default change delay parameter, as otherwise static time tests will not work
          // however, once the domain has published the initial set of domain parameters, the zero time will be
          // adjusted.
          topologyChangeDelay = DynamicDomainParameters.topologyChangeDelayIfAbsent,
          protocolVersion = protocolVersion,
        )
    }

  def findDynamicDomainParameters()(implicit
      traceContext: TraceContext
  ): Future[Either[String, DynamicDomainParametersWithValidity]]

  def findDynamicSequencingParameters()(implicit
      traceContext: TraceContext
  ): Future[Either[String, DynamicSequencingParametersWithValidity]]

  /** List all the dynamic domain parameters (past and current) */
  def listDynamicDomainParametersChanges()(implicit
      traceContext: TraceContext
  ): Future[Seq[DynamicDomainParametersWithValidity]]
}

trait MembersTopologySnapshotClient {
  this: BaseTopologySnapshotClient =>

  def allMembers()(implicit traceContext: TraceContext): Future[Set[Member]]

  def isMemberKnown(member: Member)(implicit traceContext: TraceContext): Future[Boolean]

  def areMembersKnown(members: Set[Member])(implicit
      traceContext: TraceContext
  ): Future[Set[Member]]

  def memberFirstKnownAt(member: Member)(implicit
      traceContext: TraceContext
  ): Future[Option[(SequencedTime, EffectiveTime)]]
}

trait TopologySnapshot
    extends PartyTopologySnapshotClient
    with BaseTopologySnapshotClient
    with ParticipantTopologySnapshotClient
    with KeyTopologySnapshotClient
    with VettedPackagesSnapshotClient
    with MediatorDomainStateClient
    with SequencerDomainStateClient
    with DomainGovernanceSnapshotClient
    with MembersTopologySnapshotClient { this: BaseTopologySnapshotClient with NamedLogging => }

// architecture-handbook-entry-end: IdentityProvidingServiceClient

/** The internal domain topology client interface used for initialisation and efficient processing */
trait DomainTopologyClientWithInit
    extends DomainTopologyClient
    with TopologyTransactionProcessingSubscriber
    with HasFutureSupervision
    with NamedLogging {

  implicit override protected def executionContext: ExecutionContext

  protected val domainTimeTracker: SingleUseCell[DomainTimeTracker] = new SingleUseCell()

  def setDomainTimeTracker(tracker: DomainTimeTracker): Unit =
    domainTimeTracker.putIfAbsent(tracker).discard

  /** current number of changes waiting to become effective */
  def numPendingChanges: Int

  /** Overloaded recent snapshot returning derived type */
  override def currentSnapshotApproximation(implicit
      traceContext: TraceContext
  ): TopologySnapshotLoader = trySnapshot(approximateTimestamp)

  override def trySnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): TopologySnapshotLoader

  /** Overloaded snapshot returning derived type */
  override def snapshot(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[TopologySnapshotLoader] = {
    snapshotInternal(timestamp)((timestamp, waitForEffectiveTime) =>
      this.awaitTimestamp(timestamp, waitForEffectiveTime)
    )
  }

  /** Overloaded snapshot returning derived type */
  override def snapshotUS(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[TopologySnapshotLoader] = {
    snapshotInternal[FutureUnlessShutdown](timestamp)(
      (timestamp, waitForEffectiveTime) => this.awaitTimestampUS(timestamp, waitForEffectiveTime),
      // Do not log a warning if we get a shutdown future
      logWarning = f => f != FutureUnlessShutdown.abortedDueToShutdown,
    )
  }

  private def snapshotInternal[F[_]](
      timestamp: CantonTimestamp
  )(
      awaitTimestampFn: (CantonTimestamp, Boolean) => Option[F[Unit]],
      logWarning: F[Unit] => Boolean = Function.const(true),
  )(implicit traceContext: TraceContext, monad: Monad[F]): F[TopologySnapshotLoader] = {
    // Keep current value, in case we need it in the log entry below
    val topoKnownUntilTs = topologyKnownUntilTimestamp

    val syncF = awaitTimestampFn(timestamp, true) match {
      case None => monad.unit
      // No need to log a warning if the future we get is due to a shutdown in progress
      case Some(fut) =>
        if (logWarning(fut)) {
          logger.warn(
            s"Unsynchronized access to topology snapshot at $timestamp, topology known until=$topoKnownUntilTs"
          )
          logger.debug(
            s"Stack trace",
            new Exception("Stack trace for unsynchronized topology snapshot access"),
          )
        }
        fut
    }
    syncF.map(_ => trySnapshot(timestamp))
  }

  override def awaitSnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[TopologySnapshot] =
    awaitTimestamp(timestamp, waitForEffectiveTime = true)
      .getOrElse(Future.unit)
      .map(_ => trySnapshot(timestamp))

  override def awaitSnapshotUS(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[TopologySnapshot] =
    awaitTimestampUS(timestamp, waitForEffectiveTime = true)
      .getOrElse(FutureUnlessShutdown.unit)
      .map(_ => trySnapshot(timestamp))

  /** internal await implementation used to schedule state evaluations after topology updates */
  private[topology] def scheduleAwait(
      condition: => Future[Boolean],
      timeout: Duration,
  ): FutureUnlessShutdown[Boolean]

}

/** An internal interface with a simpler lookup function which can be implemented efficiently with caching and reading from a store */
private[client] trait KeyTopologySnapshotClientLoader extends KeyTopologySnapshotClient {
  this: BaseTopologySnapshotClient =>

  /** abstract loading function used to obtain the full key collection for a key owner */
  def allKeys(owner: Member)(implicit traceContext: TraceContext): Future[KeyCollection]

  def allKeys(members: Seq[Member])(implicit
      traceContext: TraceContext
  ): Future[Map[Member, KeyCollection]]

  override def signingKey(owner: Member)(implicit
      traceContext: TraceContext
  ): Future[Option[SigningPublicKey]] =
    allKeys(owner).map(_.signingKeys.lastOption)

  override def signingKeys(members: Seq[Member])(implicit
      traceContext: TraceContext
  ): Future[Map[Member, Seq[SigningPublicKey]]] =
    allKeys(members).map(_.view.mapValues(_.signingKeys).toMap)

  override def signingKeys(owner: Member)(implicit
      traceContext: TraceContext
  ): Future[Seq[SigningPublicKey]] =
    allKeys(owner).map(_.signingKeys)

  override def encryptionKey(owner: Member)(implicit
      traceContext: TraceContext
  ): Future[Option[EncryptionPublicKey]] =
    allKeys(owner).map(_.encryptionKeys.lastOption)

  /** returns newest encryption public key */
  def encryptionKey(members: Seq[Member])(implicit
      traceContext: TraceContext
  ): Future[Map[Member, EncryptionPublicKey]] =
    encryptionKeys(members).map(_.mapFilter(_.lastOption))

  override def encryptionKeys(owner: Member)(implicit
      traceContext: TraceContext
  ): Future[Seq[EncryptionPublicKey]] =
    allKeys(owner).map(_.encryptionKeys)

  override def encryptionKeys(members: Seq[Member])(implicit
      traceContext: TraceContext
  ): Future[Map[Member, Seq[EncryptionPublicKey]]] =
    allKeys(members).map(_.view.mapValues(_.encryptionKeys).toMap)
}

/** An internal interface with a simpler lookup function which can be implemented efficiently with caching and reading from a store */
private[client] trait ParticipantTopologySnapshotLoader extends ParticipantTopologySnapshotClient {

  this: BaseTopologySnapshotClient =>

  override def isParticipantActive(participantId: ParticipantId)(implicit
      traceContext: TraceContext
  ): Future[Boolean] =
    findParticipantState(participantId).map(_.isDefined)

  override def isParticipantActiveAndCanLoginAt(
      participantId: ParticipantId,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Boolean] =
    findParticipantState(participantId).map { attributesO =>
      attributesO.exists(_.loginAfter.forall(_ <= timestamp))
    }

  final def findParticipantState(participantId: ParticipantId)(implicit
      traceContext: TraceContext
  ): Future[Option[ParticipantAttributes]] =
    loadParticipantStates(Seq(participantId)).map(_.get(participantId))

  /** abstract loading function used to load the participant state for the given set of participant-ids */
  def loadParticipantStates(
      participants: Seq[ParticipantId]
  )(implicit traceContext: TraceContext): Future[Map[ParticipantId, ParticipantAttributes]]

}

private[client] trait PartyTopologySnapshotBaseClient {

  this: PartyTopologySnapshotClient & BaseTopologySnapshotClient =>

  override def allHaveActiveParticipants(
      parties: Set[LfPartyId],
      check: (ParticipantPermission => Boolean) = _ => true,
  )(implicit traceContext: TraceContext): EitherT[Future, Set[LfPartyId], Unit] = {
    val fetchedF = activeParticipantsOfPartiesWithAttributes(parties.toSeq)
    EitherT(
      fetchedF
        .map { fetched =>
          fetched.foldLeft(Set.empty[LfPartyId]) { case (acc, (party, relationships)) =>
            if (relationships.exists { case (_, attributes) => check(attributes.permission) })
              acc
            else acc + party
          }
        }
        .map { res =>
          if (res.isEmpty) Right(())
          else Left(res)
        }
    )
  }

  override def isHostedByAtLeastOneParticipantF(
      parties: Set[LfPartyId],
      check: (LfPartyId, ParticipantAttributes) => Boolean,
  )(implicit traceContext: TraceContext): Future[Set[LfPartyId]] =
    activeParticipantsOfPartiesWithAttributes(parties.toSeq).map(partiesWithAttributes =>
      parties.filter(party =>
        partiesWithAttributes.get(party).exists(_.values.exists(check(party, _)))
      )
    )

  override def hostedOn(
      partyIds: Set[LfPartyId],
      participantId: ParticipantId,
  )(implicit traceContext: TraceContext): Future[Map[LfPartyId, ParticipantAttributes]] =
    // TODO(i4930) implement directly, must not return DISABLED
    activeParticipantsOfPartiesWithAttributes(partyIds.toSeq).map(
      _.flatMap { case (party, participants) =>
        participants.get(participantId).map(party -> _)
      }
    )

  override def allHostedOn(
      partyIds: Set[LfPartyId],
      participantId: ParticipantId,
      permissionCheck: ParticipantAttributes => Boolean = _ => true,
  )(implicit traceContext: TraceContext): Future[Boolean] =
    hostedOn(partyIds, participantId).map(partiesWithAttributes =>
      partiesWithAttributes.view
        .filter { case (_, attributes) => permissionCheck(attributes) }
        .sizeCompare(partyIds) == 0
    )

  override def canConfirm(
      participant: ParticipantId,
      parties: Set[LfPartyId],
  )(implicit traceContext: TraceContext): Future[Set[LfPartyId]] =
    hostedOn(parties, participant)
      .map(partiesWithAttributes =>
        parties.toSeq.mapFilter { case party =>
          partiesWithAttributes
            .get(party)
            .filter(_.permission.canConfirm)
            .map(_ => party)
        }.toSet
      )(executionContext)

  override def activeParticipantsOfAll(
      parties: List[LfPartyId]
  )(implicit traceContext: TraceContext): EitherT[Future, Set[LfPartyId], Set[ParticipantId]] =
    EitherT(for {
      withActiveParticipants <- activeParticipantsOfPartiesWithAttributes(parties)
      (noActive, allActive) = withActiveParticipants.foldLeft(
        Set.empty[LfPartyId] -> Set.empty[ParticipantId]
      ) { case ((noActive, allActive), (p, active)) =>
        (if (active.isEmpty) noActive + p else noActive, allActive.union(active.keySet))
      }
    } yield Either.cond(noActive.isEmpty, allActive, noActive))
}

private[client] trait PartyTopologySnapshotLoader
    extends PartyTopologySnapshotClient
    with PartyTopologySnapshotBaseClient {

  this: BaseTopologySnapshotClient & ParticipantTopologySnapshotLoader =>

  final override def activeParticipantsOf(
      party: LfPartyId
  )(implicit traceContext: TraceContext): Future[Map[ParticipantId, ParticipantAttributes]] =
    PartyId
      .fromLfParty(party)
      .map(loadActiveParticipantsOf(_, loadParticipantStates).map(_.participants))
      .getOrElse(Future.successful(Map()))

  private[client] def loadActiveParticipantsOf(
      party: PartyId,
      participantStates: Seq[ParticipantId] => Future[Map[ParticipantId, ParticipantAttributes]],
  )(implicit traceContext: TraceContext): Future[PartyInfo]

  final override def activeParticipantsOfParties(
      parties: Seq[LfPartyId]
  )(implicit traceContext: TraceContext): Future[Map[LfPartyId, Set[ParticipantId]]] =
    loadAndMapPartyInfos(parties, _.participants.keySet)

  final override def activeParticipantsOfPartiesWithAttributes(
      parties: Seq[LfPartyId]
  )(implicit
      traceContext: TraceContext
  ): Future[Map[LfPartyId, Map[ParticipantId, ParticipantAttributes]]] =
    loadAndMapPartyInfos(parties, _.participants)

  final override def partiesWithGroupAddressing(parties: Seq[LfPartyId])(implicit
      traceContext: TraceContext
  ): Future[Set[LfPartyId]] =
    loadAndMapPartyInfos(parties, identity, _.groupAddressing).map(_.keySet)

  final override def activeParticipantsOfPartiesWithGroupAddressing(
      parties: Seq[LfPartyId]
  )(implicit traceContext: TraceContext): Future[Map[LfPartyId, Set[ParticipantId]]] =
    loadAndMapPartyInfos(parties, _.participants.keySet, _.groupAddressing)

  final override def consortiumThresholds(
      parties: Set[LfPartyId]
  )(implicit traceContext: TraceContext): Future[Map[LfPartyId, PositiveInt]] =
    loadAndMapPartyInfos(parties.toSeq, _.threshold)

  final override def canNotSubmit(
      participant: ParticipantId,
      parties: Seq[LfPartyId],
  )(implicit traceContext: TraceContext): Future[immutable.Iterable[LfPartyId]] =
    loadAndMapPartyInfos(
      parties,
      _ => (),
      info =>
        info.threshold > PositiveInt.one ||
          !info.participants
            .get(participant)
            .exists(_.permission == ParticipantPermission.Submission),
    ).map(_.keySet)

  private def loadAndMapPartyInfos[T](
      lfParties: Seq[LfPartyId],
      f: PartyInfo => T,
      filter: PartyInfo => Boolean = _ => true,
  )(implicit traceContext: TraceContext): Future[Map[LfPartyId, T]] =
    loadBatchActiveParticipantsOf(
      lfParties.mapFilter(PartyId.fromLfParty(_).toOption),
      loadParticipantStates,
    ).map(_.collect {
      case (partyId, partyInfo) if filter(partyInfo) => partyId.toLf -> f(partyInfo)
    })

  private[client] def loadBatchActiveParticipantsOf(
      parties: Seq[PartyId],
      loadParticipantStates: Seq[ParticipantId] => Future[Map[ParticipantId, ParticipantAttributes]],
  )(implicit traceContext: TraceContext): Future[Map[PartyId, PartyInfo]]
}

trait VettedPackagesSnapshotLoader extends VettedPackagesSnapshotClient {
  this: BaseTopologySnapshotClient & PartyTopologySnapshotLoader =>

  private[client] def loadUnvettedPackagesOrDependencies(
      participant: ParticipantId,
      packageId: PackageId,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, PackageId, Set[PackageId]]

  protected def findUnvettedPackagesOrDependenciesUsingLoader(
      participantId: ParticipantId,
      packages: Set[PackageId],
      loader: (ParticipantId, PackageId) => EitherT[FutureUnlessShutdown, PackageId, Set[PackageId]],
  ): EitherT[FutureUnlessShutdown, PackageId, Set[PackageId]] =
    packages.toList
      .parTraverse(packageId => loader(participantId, packageId))
      .map(_.flatten.toSet)

  override def findUnvettedPackagesOrDependencies(
      participantId: ParticipantId,
      packages: Set[PackageId],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, PackageId, Set[PackageId]] =
    findUnvettedPackagesOrDependenciesUsingLoader(
      participantId,
      packages,
      (pid, packId) => loadUnvettedPackagesOrDependencies(pid, packId),
    )

}

trait DomainGovernanceSnapshotLoader extends DomainGovernanceSnapshotClient {
  this: BaseTopologySnapshotClient with NamedLogging =>
}

/** Loading interface with a more optimal method to read data from a store
  *
  * The topology information is stored in a particular way. In order to optimise loading and caching
  * of the data, we use such loader interfaces, such that we can optimise caching and loading of the
  * data while still providing a good and convenient access to the topology information.
  */
trait TopologySnapshotLoader
    extends TopologySnapshot
    with PartyTopologySnapshotLoader
    with BaseTopologySnapshotClient
    with ParticipantTopologySnapshotLoader
    with KeyTopologySnapshotClientLoader
    with VettedPackagesSnapshotLoader
    with DomainGovernanceSnapshotLoader
    with NamedLogging
