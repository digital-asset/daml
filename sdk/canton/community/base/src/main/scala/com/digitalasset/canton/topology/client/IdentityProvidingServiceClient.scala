// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import cats.data.EitherT
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.HasFutureSupervision
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.SigningKeyUsage.nonEmptyIntersection
import com.digitalasset.canton.crypto.{EncryptionPublicKey, SigningKeyUsage, SigningPublicKey}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.protocol.{
  DynamicSequencingParametersWithValidity,
  DynamicSynchronizerParameters,
  DynamicSynchronizerParametersWithValidity,
}
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.time.SynchronizerTimeTracker
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.client.PartyKeyTopologySnapshotClient.PartyAuthorizationInfo
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
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
  * the synchronizers, such as party-participant relationships, used encryption and signing keys,
  * package information, participant states, synchronizer parameters, and so on.
  */
class IdentityProvidingServiceClient {

  private val synchronizers = TrieMap.empty[SynchronizerId, SynchronizerTopologyClient]

  def add(synchronizerClient: SynchronizerTopologyClient): this.type = {
    synchronizers += (synchronizerClient.synchronizerId -> synchronizerClient)
    this
  }

  def allSynchronizers: Iterable[SynchronizerTopologyClient] = synchronizers.values

  def tryForSynchronizer(synchronizerId: SynchronizerId): SynchronizerTopologyClient =
    synchronizers.getOrElse(
      synchronizerId,
      sys.error("unknown synchronizer " + synchronizerId.toString),
    )

  def forSynchronizer(synchronizerId: SynchronizerId): Option[SynchronizerTopologyClient] =
    synchronizers.get(synchronizerId)

}

trait TopologyClientApi[+T] { this: HasFutureSupervision =>

  /** The synchronizer this client applies to */
  def synchronizerId: SynchronizerId

  /** Our current snapshot approximation
    *
    * As topology transactions are future dated (to prevent sequential bottlenecks), we do
    * have to "guess" the current state, as time is defined by the sequencer after
    * we've sent the transaction. Therefore, this function will return the
    * best snapshot approximation known.
    *
    * The snapshot returned by this method should be used when preparing a transaction or reassignment request (Phase 1).
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
    * epsilon is given by the topology change delay time, defined using the synchronizer parameters.
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
    * The snapshot returned by this method should be used for validating transaction and reassignment requests (Phase 2 - 7).
    * Use the request timestamp as parameter for this method.
    * Do not use a response or result timestamp, because all validation steps must use the same topology snapshot.
    */
  def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[T]

  /** Waits until a snapshot is available
    *
    * The snapshot returned by this method should be used for validating transaction and reassignment requests (Phase 2 - 7).
    * Use the request timestamp as parameter for this method.
    * Do not use a response or result timestamp, because all validation steps must use the same topology snapshot.
    */
  def awaitSnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[T]

  /** Supervised version of [[awaitSnapshot]] */
  def awaitSnapshotUSSupervised(description: => String, warnAfter: Duration = 30.seconds)(
      timestamp: CantonTimestamp
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[T] = supervisedUS(description, warnAfter)(awaitSnapshot(timestamp))

  /** Returns the topology information at a certain point in time
    *
    * Fails with an exception if the state is not yet known.
    *
    * The snapshot returned by this method should be used for validating transaction and reassignment requests (Phase 2 - 7).
    * Use the request timestamp as parameter for this method.
    * Do not use a response or result timestamp, because all validation steps must use the same topology snapshot.
    */
  def trySnapshot(timestamp: CantonTimestamp)(implicit traceContext: TraceContext): T

  /** Returns an optional future which will complete when the effective timestamp has been observed
    *
    * If the timestamp is already observed, returns None.
    */
  def awaitTimestamp(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Option[FutureUnlessShutdown[Unit]]

  /** Finds the topology transaction with maximum effective time whose effects would be visible,
    * at earliest i.e. if delay is 0, in a topology snapshot at `effectiveTime`, and yields
    * the sequenced and actual effective time of that topology transaction, if necessary after waiting
    * to observe a timestamp at or after sequencing time `effectiveTime.immediatePredecessor`
    * that the topology processor has fully processed.
    */
  def awaitMaxTimestamp(sequencedTime: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]]
}

/** The client that provides the topology information on a per-synchronizer basis
  */
trait SynchronizerTopologyClient extends TopologyClientApi[TopologySnapshot] with AutoCloseable {
  this: HasFutureSupervision =>

  /** Wait for a condition to become true according to the current snapshot approximation
    *
    * @return true if the condition became true, false if it timed out
    */
  def await(condition: TopologySnapshot => Future[Boolean], timeout: Duration)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean]

  def awaitUS(condition: TopologySnapshot => FutureUnlessShutdown[Boolean], timeout: Duration)(
      implicit traceContext: TraceContext
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
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[LfPartyId, Set[ParticipantId]]]

  def activeParticipantsOfPartiesWithInfo(
      parties: Seq[LfPartyId]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfPartyId, PartyInfo]]

  /** Returns the set of active participants the given party is represented by as of the snapshot timestamp
    *
    * Should never return a PartyParticipantRelationship where ParticipantPermission is DISABLED.
    */
  def activeParticipantsOf(
      party: LfPartyId
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[ParticipantId, ParticipantAttributes]]

  /** Returns Right if all parties have at least an active participant passing the check. Otherwise, all parties not passing are passed as Left */
  def allHaveActiveParticipants(
      parties: Set[LfPartyId],
      check: ParticipantPermission => Boolean = _ => true,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, Set[LfPartyId], Unit]

  /** Returns the consortium thresholds (how many votes from different participants that host the consortium party
    * are required for the confirmation to become valid). For normal parties returns 1.
    */
  def consortiumThresholds(parties: Set[LfPartyId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfPartyId, PositiveInt]]

  /** Returns true if there is at least one participant that satisfies the predicate */
  def isHostedByAtLeastOneParticipantF(
      parties: Set[LfPartyId],
      check: (LfPartyId, ParticipantAttributes) => Boolean,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[LfPartyId]]

  /** Returns the participant permission for that particular participant (if there is one) */
  def hostedOn(
      partyIds: Set[LfPartyId],
      participantId: ParticipantId,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfPartyId, ParticipantAttributes]]

  /** Returns true if all given party ids are hosted on a certain participant */
  def allHostedOn(
      partyIds: Set[LfPartyId],
      participantId: ParticipantId,
      permissionCheck: ParticipantAttributes => Boolean = _ => true,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean]

  /** Returns whether a participant can confirm on behalf of a party. */
  def canConfirm(
      participant: ParticipantId,
      parties: Set[LfPartyId],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[LfPartyId]]

  /** Returns parties with no hosting participant that can confirm for them. */
  def hasNoConfirmer(
      parties: Set[LfPartyId]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[LfPartyId]]

  /** Returns the subset of parties the given participant can NOT submit on behalf of */
  def canNotSubmit(
      participant: ParticipantId,
      parties: Seq[LfPartyId],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[immutable.Iterable[LfPartyId]]

  /** Returns all active participants of all the given parties. Returns a Left if some of the parties don't have active
    * participants, in which case the parties with missing active participants are returned. Note that it will return
    * an empty set as a Right when given an empty list of parties.
    */
  def activeParticipantsOfAll(
      parties: List[LfPartyId]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Set[LfPartyId], Set[ParticipantId]]

  /** Returns a list of all known parties on this synchronizer. */
  def inspectKnownParties(
      filterParty: String,
      filterParticipant: String,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PartyId]]
}

object PartyTopologySnapshotClient {
  final case class PartyInfo(
      threshold: PositiveInt, // > 1 for consortium parties
      participants: Map[ParticipantId, ParticipantAttributes],
  )

  object PartyInfo {
    def nonConsortiumPartyInfo(participants: Map[ParticipantId, ParticipantAttributes]): PartyInfo =
      PartyInfo(threshold = PositiveInt.one, participants = participants)

    lazy val EmptyPartyInfo: PartyInfo = nonConsortiumPartyInfo(Map.empty)
  }
}

/** The subset of the topology client, providing the party related key information */
trait PartyKeyTopologySnapshotClient {

  this: BaseTopologySnapshotClient =>

  /** returns authorization information for the party, including signing keys and threshold */
  def partyAuthorization(party: PartyId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[PartyAuthorizationInfo]]

}

object PartyKeyTopologySnapshotClient {

  /** party key information
    *
    * @param threshold how many signatures we require to have for the given party to authorize a transaction
    * @param signingKeys the valid signing keys for the given party
    */
  final case class PartyAuthorizationInfo(
      threshold: PositiveInt,
      signingKeys: NonEmpty[Seq[SigningPublicKey]],
  )
}

/** The subset of the topology client, providing signing and encryption key information */
trait KeyTopologySnapshotClient {

  this: BaseTopologySnapshotClient =>

  /** Retrieves the signing keys for a given owner, filtering them based on the provided usage set.
    *
    * @param owner The owner whose signing keys are being queried.
    * @param filterUsage A non-empty set of usages to filter the signing keys by.
    *              At least one usage from this set must match with the key's usage.
    */
  def signingKeys(
      owner: Member,
      filterUsage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[SigningPublicKey]]

  def signingKeys(
      members: Seq[Member],
      filterUsage: NonEmpty[Set[SigningKeyUsage]] = SigningKeyUsage.All,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[Member, Seq[SigningPublicKey]]]

  /** returns newest encryption public key */
  def encryptionKey(owner: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[EncryptionPublicKey]]

  /** returns newest encryption public key */
  def encryptionKey(members: Seq[Member])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[Member, EncryptionPublicKey]]

  /** returns all encryption keys */
  def encryptionKeys(owner: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[EncryptionPublicKey]]

  def encryptionKeys(members: Seq[Member])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[Member, Seq[EncryptionPublicKey]]]

  /** Returns a list of all known keys on this synchronizer */
  def inspectKeys(
      filterOwner: String,
      filterOwnerType: Option[MemberCode],
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[Member, KeyCollection]]

}

/** The subset of the topology client, providing participant state information */
trait ParticipantTopologySnapshotClient {

  this: BaseTopologySnapshotClient =>

  /** Checks whether the provided participant exists and is active.
    * Active means:
    * 1. The participant has a SynchronizerTrustCertificate.
    * 2. The synchronizer is either unrestricted or there is a ParticipantSynchronizerPermission for the participant.
    * 3. The participant has an OwnerToKeyMapping with signing and encryption keys.
    */
  def isParticipantActive(participantId: ParticipantId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean]

  /** Checks whether the provided participant exists, is active and can login at the given point in time
    *
    * (loginAfter is before timestamp)
    */
  def isParticipantActiveAndCanLoginAt(
      participantId: ParticipantId,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean]

}

/** The subset of the topology client providing mediator state information */
trait MediatorSynchronizerStateClient {
  this: BaseTopologySnapshotClient & KeyTopologySnapshotClient =>

  def mediatorGroups()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[MediatorGroup]]

  /** Returns true if
    * <ul>
    *   <li>the mediator is a member of a mediator group with num_active_sequencers >= threshold</li>
    *   <li>the mediator has an OwnerToKeyMapping with at least 1 signing key</li>
    * </ul>
    */
  def isMediatorActive(mediatorId: MediatorId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    mediatorGroups().map(_.exists { group =>
      // Note: mediator in group.passive should still be able to authenticate and process ConfirmationResponses,
      // only sending the verdicts is disabled and verdicts from a passive mediator should not pass the checks
      group.isActive && (group.active.contains(mediatorId) || group.passive.contains(mediatorId))
    })

  def isMediatorActive(
      mediator: MediatorGroupRecipient
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean] =
    mediatorGroup(mediator.group).map {
      case Some(group) => group.isActive
      case None => false
    }

  def mediatorGroupsOfAll(
      groups: Seq[MediatorGroupIndex]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Seq[MediatorGroupIndex], Seq[MediatorGroup]] =
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
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[MediatorGroup]] =
    mediatorGroups().map(_.find(_.index == index))
}

/** The subset of the topology client providing sequencer state information */
trait SequencerSynchronizerStateClient {
  this: BaseTopologySnapshotClient =>

  /** The returned sequencer group contains all sequencers that
    * <ul>
    *   <li>are mentioned in the SequencerSynchronizerState topology transaction and</li>
    *   <li>have at least 1 signing key</li>
    * </ul>
    */
  def sequencerGroup()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[SequencerGroup]]
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
      ledgerTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PackageId]]

  /** Checks the vetting state for the given packages and returns the packages
    * that have no entry in the participant's VettedPackages topology transactions.
    * Note: this does not check the vetted packages for their validity period, but simply for their
    * existence in the mapping.
    *
    * @param participantId the participant for which we want to check the package vettings
    * @param packageIds the set of packages to check
    * @return the packages that have no entry in the participant's VettedPackages mapping
    */
  def determinePackagesWithNoVettingEntry(
      participantId: ParticipantId,
      packageIds: Set[PackageId],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PackageId]]
}

trait SynchronizerGovernanceSnapshotClient {
  this: BaseTopologySnapshotClient with NamedLogging =>

  def trafficControlParameters(
      protocolVersion: ProtocolVersion,
      warnOnUsingDefault: Boolean = true,
  )(implicit tc: TraceContext): FutureUnlessShutdown[Option[TrafficControlParameters]] =
    findDynamicSynchronizerParametersOrDefault(
      protocolVersion,
      warnOnUsingDefault = warnOnUsingDefault,
    )
      .map(_.trafficControlParameters)

  def findDynamicSynchronizerParametersOrDefault(
      protocolVersion: ProtocolVersion,
      warnOnUsingDefault: Boolean = true,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[DynamicSynchronizerParameters] =
    findDynamicSynchronizerParameters().map {
      case Right(value) => value.parameters
      case Left(_) =>
        if (warnOnUsingDefault) {
          logger.warn(s"Unexpectedly using default synchronizer parameters at $timestamp")
        }

        DynamicSynchronizerParameters.initialValues(
          // we must use zero as default change delay parameter, as otherwise static time tests will not work
          // however, once the synchronizer has published the initial set of synchronizer parameters, the zero time will be
          // adjusted.
          topologyChangeDelay = DynamicSynchronizerParameters.topologyChangeDelayIfAbsent,
          protocolVersion = protocolVersion,
        )
    }

  def findDynamicSynchronizerParameters()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[String, DynamicSynchronizerParametersWithValidity]]

  def findDynamicSequencingParameters()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[String, DynamicSequencingParametersWithValidity]]

  /** List all the dynamic synchronizer parameters (past and current) */
  def listDynamicSynchronizerParametersChanges()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[DynamicSynchronizerParametersWithValidity]]
}

trait MembersTopologySnapshotClient {
  this: BaseTopologySnapshotClient =>

  /** Convenience method to determin all members with `isMemberKnown`. */
  def allMembers()(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[Member]]

  /** Determines if a member is known on the synchronizer (through a SynchronizerTrustCertificate, MediatorSynchronizerState, or SequencerSynchronizerState).
    * Note that a "known" member is not necessarily authorized to use the synchronizer.
    */
  def isMemberKnown(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean]

  /** Convenience method to check `isMemberKnown` for several members. */
  def areMembersKnown(members: Set[Member])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[Member]]

  def memberFirstKnownAt(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]]
}

trait TopologySnapshot
    extends PartyTopologySnapshotClient
    with BaseTopologySnapshotClient
    with ParticipantTopologySnapshotClient
    with KeyTopologySnapshotClient
    with VettedPackagesSnapshotClient
    with MediatorSynchronizerStateClient
    with SequencerSynchronizerStateClient
    with SynchronizerGovernanceSnapshotClient
    with MembersTopologySnapshotClient
    with PartyKeyTopologySnapshotClient { this: BaseTopologySnapshotClient with NamedLogging => }

// architecture-handbook-entry-end: IdentityProvidingServiceClient

/** The internal synchronizer topology client interface used for initialisation and efficient processing */
trait SynchronizerTopologyClientWithInit
    extends SynchronizerTopologyClient
    with TopologyTransactionProcessingSubscriber
    with HasFutureSupervision
    with NamedLogging {

  implicit override protected def executionContext: ExecutionContext

  protected val synchronizerTimeTracker: SingleUseCell[SynchronizerTimeTracker] =
    new SingleUseCell()

  def setSynchronizerTimeTracker(tracker: SynchronizerTimeTracker): Unit =
    synchronizerTimeTracker.putIfAbsent(tracker).discard

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
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[TopologySnapshotLoader] = {
    // Keep current value, in case we need it in the log entry below
    val topoKnownUntilTs = topologyKnownUntilTimestamp

    val syncF = this.awaitTimestamp(timestamp) match {
      case None => FutureUnlessShutdown.unit
      // No need to log a warning if the future we get is due to a shutdown in progress
      case Some(fut) =>
        if (fut != FutureUnlessShutdown.abortedDueToShutdown) {
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
  ): FutureUnlessShutdown[TopologySnapshot] =
    awaitTimestamp(timestamp)
      .getOrElse(FutureUnlessShutdown.unit)
      .map(_ => trySnapshot(timestamp))

  /** internal await implementation used to schedule state evaluations after topology updates */
  private[topology] def scheduleAwait(
      condition: => FutureUnlessShutdown[Boolean],
      timeout: Duration,
  ): FutureUnlessShutdown[Boolean]
}

/** An internal interface with a simpler lookup function which can be implemented efficiently with caching and reading from a store */
private[client] trait KeyTopologySnapshotClientLoader extends KeyTopologySnapshotClient {
  this: BaseTopologySnapshotClient =>

  /** abstract loading function used to obtain the full key collection for a key owner */
  def allKeys(owner: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[KeyCollection]

  def allKeys(members: Seq[Member])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[Member, KeyCollection]]

  override def signingKeys(
      owner: Member,
      filterUsage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[SigningPublicKey]] =
    allKeys(owner).map(keys =>
      keys.signingKeys.filter(key => nonEmptyIntersection(key.usage, filterUsage))
    )

  override def signingKeys(
      members: Seq[Member],
      filterUsage: NonEmpty[Set[SigningKeyUsage]] = SigningKeyUsage.All,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[Member, Seq[SigningPublicKey]]] =
    allKeys(members).map(_.view.mapValues(_.signingKeys).toMap.map { case (member, keys) =>
      member -> keys.filter(key => nonEmptyIntersection(key.usage, filterUsage))
    })

  override def encryptionKey(owner: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[EncryptionPublicKey]] =
    allKeys(owner).map(_.encryptionKeys.lastOption)

  /** returns newest encryption public key */
  def encryptionKey(members: Seq[Member])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[Member, EncryptionPublicKey]] =
    encryptionKeys(members).map(_.mapFilter(_.lastOption))

  override def encryptionKeys(owner: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[EncryptionPublicKey]] =
    allKeys(owner).map(_.encryptionKeys)

  override def encryptionKeys(members: Seq[Member])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[Member, Seq[EncryptionPublicKey]]] =
    allKeys(members).map(_.view.mapValues(_.encryptionKeys).toMap)
}

/** An internal interface with a simpler lookup function which can be implemented efficiently with caching and reading from a store */
private[client] trait ParticipantTopologySnapshotLoader extends ParticipantTopologySnapshotClient {

  this: BaseTopologySnapshotClient & KeyTopologySnapshotClient =>

  override def isParticipantActive(participantId: ParticipantId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    findParticipantState(participantId).map(_.isDefined)

  override def isParticipantActiveAndCanLoginAt(
      participantId: ParticipantId,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean] =
    findParticipantState(participantId).map { attributesO =>
      attributesO.exists(_.loginAfter.forall(_ <= timestamp))
    }

  final def findParticipantState(participantId: ParticipantId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[ParticipantAttributes]] =
    loadParticipantStates(Seq(participantId)).map(_.get(participantId))

  /** Loads the participant state for the given set of participant ids.
    * The result covers only active participants, i.e., only participants with SynchronizerTrustCertificates,
    * ParticipantSynchronizerPermission (if the synchronizer is restricted), and signing and encryption keys.
    */
  def loadParticipantStates(
      participants: Seq[ParticipantId]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[ParticipantId, ParticipantAttributes]]

}

private[client] trait PartyTopologySnapshotBaseClient {

  this: PartyTopologySnapshotClient & BaseTopologySnapshotClient =>

  override def allHaveActiveParticipants(
      parties: Set[LfPartyId],
      check: ParticipantPermission => Boolean = _ => true,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, Set[LfPartyId], Unit] = {
    val fetchedF = activeParticipantsOfPartiesWithInfo(parties.toSeq)
    EitherT(
      fetchedF
        .map { fetched =>
          fetched.foldLeft(Set.empty[LfPartyId]) { case (acc, (party, partyInfo)) =>
            if (
              partyInfo.participants.exists { case (_, attributes) => check(attributes.permission) }
            )
              acc
            else acc + party
          }
        }
        .map { res =>
          Either.cond(res.isEmpty, (), res)
        }
    )
  }

  override def isHostedByAtLeastOneParticipantF(
      parties: Set[LfPartyId],
      check: (LfPartyId, ParticipantAttributes) => Boolean,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[LfPartyId]] =
    activeParticipantsOfPartiesWithInfo(parties.toSeq).map(partiesWithInfo =>
      parties.filter(party =>
        partiesWithInfo.get(party).map(_.participants).exists(_.values.exists(check(party, _)))
      )
    )

  override def hostedOn(
      partyIds: Set[LfPartyId],
      participantId: ParticipantId,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfPartyId, ParticipantAttributes]] =
    // TODO(i4930) implement directly, must not return DISABLED
    activeParticipantsOfPartiesWithInfo(partyIds.toSeq).map(
      _.flatMap { case (party, partyInfo) =>
        partyInfo.participants.get(participantId).map(party -> _)
      }
    )

  override def allHostedOn(
      partyIds: Set[LfPartyId],
      participantId: ParticipantId,
      permissionCheck: ParticipantAttributes => Boolean = _ => true,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean] =
    hostedOn(partyIds, participantId).map(partiesWithAttributes =>
      partiesWithAttributes.view
        .filter { case (_, attributes) => permissionCheck(attributes) }
        .sizeCompare(partyIds) == 0
    )

  override def canConfirm(
      participant: ParticipantId,
      parties: Set[LfPartyId],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[LfPartyId]] =
    hostedOn(parties, participant)
      .map(partiesWithAttributes =>
        parties.toSeq.mapFilter { party =>
          partiesWithAttributes
            .get(party)
            .filter(_.permission.canConfirm)
            .map(_ => party)
        }.toSet
      )

  override def hasNoConfirmer(
      parties: Set[LfPartyId]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[LfPartyId]] =
    activeParticipantsOfPartiesWithInfo(parties.toSeq).map { partyToParticipants =>
      parties.filterNot { party =>
        partyToParticipants
          .get(party)
          .exists(_.participants.values.exists(_.canConfirm))
      }
    }

  override def activeParticipantsOfAll(
      parties: List[LfPartyId]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Set[LfPartyId], Set[ParticipantId]] =
    EitherT(for {
      withActiveParticipants <- activeParticipantsOfPartiesWithInfo(parties)
      (noActive, allActive) = withActiveParticipants.foldLeft(
        Set.empty[LfPartyId] -> Set.empty[ParticipantId]
      ) { case ((noActive, allActive), (p, partyInfo)) =>
        (
          if (partyInfo.participants.isEmpty) noActive + p else noActive,
          allActive.union(partyInfo.participants.keySet),
        )
      }
    } yield Either.cond(noActive.isEmpty, allActive, noActive))
}

private[client] trait PartyTopologySnapshotLoader
    extends PartyTopologySnapshotClient
    with PartyTopologySnapshotBaseClient {

  this: BaseTopologySnapshotClient & ParticipantTopologySnapshotLoader =>

  final override def activeParticipantsOf(
      party: LfPartyId
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[ParticipantId, ParticipantAttributes]] =
    PartyId
      .fromLfParty(party)
      .map(loadActiveParticipantsOf(_, loadParticipantStates).map(_.participants))
      .getOrElse(FutureUnlessShutdown.pure(Map()))

  private[client] def loadActiveParticipantsOf(
      party: PartyId,
      participantStates: Seq[ParticipantId] => FutureUnlessShutdown[
        Map[ParticipantId, ParticipantAttributes]
      ],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[PartyInfo]

  final override def activeParticipantsOfParties(
      parties: Seq[LfPartyId]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[LfPartyId, Set[ParticipantId]]] =
    loadAndMapPartyInfos(parties, _.participants.keySet)

  final override def activeParticipantsOfPartiesWithInfo(
      parties: Seq[LfPartyId]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[LfPartyId, PartyInfo]] =
    loadAndMapPartyInfos(parties, identity)

  final override def consortiumThresholds(
      parties: Set[LfPartyId]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[LfPartyId, PositiveInt]] =
    loadAndMapPartyInfos(parties.toSeq, _.threshold)

  final override def canNotSubmit(
      participant: ParticipantId,
      parties: Seq[LfPartyId],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[immutable.Iterable[LfPartyId]] =
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
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[LfPartyId, T]] =
    loadBatchActiveParticipantsOf(
      lfParties.mapFilter(PartyId.fromLfParty(_).toOption),
      loadParticipantStates,
    ).map(_.collect {
      case (partyId, partyInfo) if filter(partyInfo) => partyId.toLf -> f(partyInfo)
    })

  private[client] def loadBatchActiveParticipantsOf(
      parties: Seq[PartyId],
      loadParticipantStates: Seq[ParticipantId] => FutureUnlessShutdown[
        Map[ParticipantId, ParticipantAttributes]
      ],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[PartyId, PartyInfo]]
}

trait VettedPackagesLoader {
  private[client] def loadVettedPackages(
      participant: ParticipantId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[PackageId, VettedPackage]]
}

trait VettedPackagesSnapshotLoader extends VettedPackagesSnapshotClient with VettedPackagesLoader {
  this: BaseTopologySnapshotClient & PartyTopologySnapshotLoader =>

  private[client] def loadUnvettedPackagesOrDependenciesUsingLoader(
      participant: ParticipantId,
      packageId: PackageId,
      ledgerTime: CantonTimestamp,
      vettedPackagesLoader: VettedPackagesLoader,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PackageId]]

  override final def findUnvettedPackagesOrDependencies(
      participantId: ParticipantId,
      packages: Set[PackageId],
      ledgerTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PackageId]] =
    packages.toList
      .parTraverse(packageId =>
        loadUnvettedPackagesOrDependenciesUsingLoader(participantId, packageId, ledgerTime, this)
      )
      .map(_.flatten.toSet)

  override final def determinePackagesWithNoVettingEntry(
      participantId: ParticipantId,
      packageIds: Set[PackageId],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PackageId]] =
    loadVettedPackages(participantId).map { vettedPackages =>
      val vettedIds = vettedPackages.keySet
      packageIds -- vettedIds
    }
}

trait SynchronizerGovernanceSnapshotLoader extends SynchronizerGovernanceSnapshotClient {
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
    with SynchronizerGovernanceSnapshotLoader
    with NamedLogging
