// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.authentication

import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.bifunctor.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.{BatchingConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication.*
import com.digitalasset.canton.sequencing.authentication.grpc.AuthenticationTokenWithExpiry
import com.digitalasset.canton.sequencing.authentication.{AuthenticationToken, MemberAuthentication}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.processing.*
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.MonadUtil

import java.time.Duration
import scala.concurrent.ExecutionContext

/** The authentication service issues tokens to members after they have successfully completed the
  * following challenge response protocol and after they have accepted the service agreement of the
  * synchronizer. The tokens are required for connecting to the sequencer.
  *
  * In order for a member to subscribe to the sequencer, it must follow a few steps for it to
  * authenticate. Assuming the synchronizer already has knowledge of the member's public keys, the
  * following steps are to be taken:
  *   1. member sends request to the synchronizer for authenticating
  *   1. synchronizer returns a nonce (a challenge random number)
  *   1. member takes the nonce, concatenates it with the identity of the synchronizer, signs it and
  *      sends it back
  *   1. synchronizer checks the signature against the key of the member. if it matches, create a
  *      token and return it
  *   1. member will use the token when subscribing to the sequencer
  *
  * @param invalidateMemberCallback
  *   Called when a member is explicitly deactivated on the synchronizer so all active subscriptions
  *   for this member should be terminated.
  */
class MemberAuthenticationService(
    synchronizerId: PhysicalSynchronizerId,
    cryptoApi: SynchronizerCryptoClient,
    store: MemberAuthenticationStore,
    clock: Clock,
    nonceExpirationInterval: Duration,
    maxTokenExpirationInterval: Duration,
    useExponentialRandomTokenExpiration: Boolean,
    invalidateMemberCallback: Traced[Member] => Unit,
    isTopologyInitialized: FutureUnlessShutdown[Unit],
    override val timeouts: ProcessingTimeout,
    batchingConfig: BatchingConfig,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  /** synchronizer generates nonce that he expects the participant to use to concatenate with the
    * synchronizer's id and sign to proceed with the authentication (step 2). We expect to find a
    * key with usage 'SequencerAuthentication' to sign these messages.
    */
  def generateNonce(member: Member)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, AuthenticationError, (Nonce, NonEmpty[Seq[Fingerprint]])] =
    for {
      _ <- EitherT.right(waitForInitialized)
      snapshot <- EitherT.liftF(cryptoApi.ips.currentSnapshotApproximation)
      _ <- isActive(member)
      fingerprints <- EitherT(
        snapshot
          .signingKeys(member, SigningKeyUsage.SequencerAuthenticationOnly)
          .map { keys =>
            NonEmpty
              .from(keys.map(_.fingerprint))
              .toRight(
                NoKeysWithCorrectUsageRegistered(
                  member,
                  SigningKeyUsage.SequencerAuthenticationOnly,
                ): AuthenticationError
              )
          }
      )
      nonce = Nonce.generate(cryptoApi.pureCrypto)
      storedNonce = StoredNonce(member, nonce, clock.now, nonceExpirationInterval)
      _ = store.saveNonce(storedNonce)
    } yield {
      scheduleExpirations(storedNonce.expireAt)
      (nonce, fingerprints)
    }

  private def waitForInitialized(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    // avoid logging if we're already done
    if (isTopologyInitialized.isCompleted) isTopologyInitialized
    else {
      logger.debug(s"Waiting for topology to be initialized")

      isTopologyInitialized.map { _ =>
        logger.debug(s"Topology has been initialized")
      }
    }

  /** synchronizer checks that the signature given by the member matches and returns a token if it
    * does (step 4)
    */
  def validateSignature(member: Member, signature: Signature, providedNonce: Nonce)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, AuthenticationError, AuthenticationTokenWithExpiry] =
    for {
      _ <- EitherT.right(waitForInitialized)
      _ <- isActive(member)
      value <- EitherT
        .fromEither(
          ignoreExpired(store.fetchAndRemoveNonce(member, providedNonce))
            .toRight(MissingNonce(member): AuthenticationError)
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      StoredNonce(_, nonce, generatedAt, _expireAt) = value
      authentication <- EitherT.fromEither[FutureUnlessShutdown](MemberAuthentication(member))
      hash = authentication.hashSynchronizerNonce(nonce, synchronizerId, cryptoApi.pureCrypto)
      snapshot <- EitherT.liftF(cryptoApi.currentSnapshotApproximation)

      _ <- snapshot
        .verifySignature(
          hash,
          member,
          signature,
          SigningKeyUsage.SequencerAuthenticationOnly,
        )
        .leftMap { err =>
          logger.warn(s"Member $member provided invalid signature: $err")
          InvalidSignature(member): AuthenticationError
        }
      token = AuthenticationToken.generate(cryptoApi.pureCrypto)
      maybeRandomTokenExpirationTime =
        if (useExponentialRandomTokenExpiration) {
          val randomSeconds = MemberAuthenticationService.truncatedExponentialRandomDelay(
            scale = maxTokenExpirationInterval.toSeconds.toDouble * 0.75,
            min = maxTokenExpirationInterval.toSeconds / 2.0,
            max = maxTokenExpirationInterval.toSeconds.toDouble,
          )
          Duration.ofSeconds(randomSeconds.toLong)
        } else {
          maxTokenExpirationInterval
        }
      tokenExpiry = clock.now.add(maybeRandomTokenExpirationTime)
      storedToken = StoredAuthenticationToken(member, tokenExpiry, token)
      _ = store.saveToken(storedToken)
    } yield {
      logger.info(
        s"$member authenticated new token with expiry $tokenExpiry"
      )
      AuthenticationTokenWithExpiry(token, tokenExpiry)
    }

  /** synchronizer checks if the token given by the participant is the one previously assigned to it
    * for authentication. The participant also provides the synchronizer id for which they think
    * they are connecting to. If this id does not match this synchronizer's id, it means the
    * participant was previously connected to a different synchronizer on the same address and now
    * should be informed that this address now hosts a different synchronizer.
    */
  def validateToken(
      intendedSynchronizerId: PhysicalSynchronizerId,
      member: Member,
      token: AuthenticationToken,
  ): Either[AuthenticationError, StoredAuthenticationToken] =
    for {
      _ <- correctSynchronizer(member, intendedSynchronizerId)
      validTokenO = store.tokenForMemberAt(member, token, clock.now)
      validToken <- validTokenO.toRight(MissingToken(member)).leftWiden[AuthenticationError]
    } yield validToken

  def invalidateMemberWithToken(
      token: AuthenticationToken
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[LogoutTokenDoesNotExist.type, Unit]] =
    for {
      _ <- waitForInitialized
      memberO = store.fetchMemberOfTokenForInvalidation(token)
      res <- memberO match {
        case None => FutureUnlessShutdown.pure(Left(LogoutTokenDoesNotExist))
        case Some(member) =>
          // Force invalidation, whether the member is actually active or not
          invalidateAndExpire(isActiveCheck = (_: Member) => FutureUnlessShutdown.pure(false))(
            member
          ).map(Right(_))
      }
    } yield res

  private def ignoreExpired[A <: HasExpiry](itemO: Option[A]): Option[A] =
    itemO.filter(_.expireAt > clock.now)

  private def scheduleExpirations(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Unit = {
    def run(): Unit = synchronizeWithClosingSync(functionFullName) {
      val now = clock.now
      logger.debug(s"Expiring nonces and tokens up to $now")
      store.expireNoncesAndTokens(now)
    }.onShutdown(())

    clock.scheduleAt(_ => run(), timestamp).discard
  }

  private def isActive(
      member: Member
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, AuthenticationError, Unit] =
    member match {
      case participant: ParticipantId =>
        EitherT(isParticipantActive(participant).map {
          Either.cond(_, (), MemberAccessDisabled(participant))
        })
      case mediator: MediatorId =>
        EitherT(isMediatorActive(mediator).map {
          Either.cond(_, (), MemberAccessDisabled(mediator))
        })
      case sequencer: SequencerId =>
        // TODO(#24306) allow sequencer-sequencer authentication only for BFT sequencers on P2P endpoints
        EitherT(isSequencerActive(sequencer).map {
          Either.cond(_, (), MemberAccessDisabled(sequencer))
        })
    }

  private def correctSynchronizer(
      member: Member,
      intendedSynchronizerId: PhysicalSynchronizerId,
  ): Either[AuthenticationError, Unit] =
    Either.cond(
      intendedSynchronizerId == synchronizerId,
      (),
      NonMatchingSynchronizerId(member, intendedSynchronizerId),
    )

  protected def isMemberActive(check: TopologySnapshot => FutureUnlessShutdown[Boolean])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    // we are a bit more conservative here. a member needs to be active NOW and the head state (i.e. effective in the future)
    MonadUtil
      .parTraverseWithLimit(batchingConfig.parallelism)(
        Seq(
          FutureUnlessShutdown.pure(cryptoApi.headSnapshot),
          cryptoApi.currentSnapshotApproximation,
        )
          .map(_.map(_.ipsSnapshot))
      )(_.flatMap(check(_)))
      .map(_.forall(identity))

  protected def isParticipantActive(participant: ParticipantId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] = isMemberActive(
    _.isParticipantActiveAndCanLoginAt(participant, clock.now)
  )

  protected def isMediatorActive(mediator: MediatorId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] = isMemberActive(_.isMediatorActive(mediator))

  protected def isSequencerActive(sequencer: SequencerId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] = isMemberActive(_.isSequencerActive(sequencer))

  protected def invalidateAndExpire[T <: Member](
      isActiveCheck: T => FutureUnlessShutdown[Boolean]
  )(memberId: T)(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    isActiveCheck(memberId).map { isActive =>
      if (!isActive) {
        logger.debug(s"Expiring all auth-tokens of $memberId")
        // first, remove all auth tokens
        store.invalidateMember(memberId)
        // second, ensure the sequencer client gets disconnected
        invalidateMemberCallback(Traced(memberId))
      }
    }

}

object MemberAuthenticationService {

  /** Generates a random delay between min and max, following an exponential distribution with the
    * given scale. The delay is truncated to the range [min, max]. The interval should be
    * sufficiently large around the scale, to avoid long sampling times or even running
    * indefinitely.
    */
  private[authentication] def truncatedExponentialRandomDelay(
      scale: Double,
      min: Double,
      max: Double,
  ): Double =
    Iterator
      .continually {
        -math.log(scala.util.Random.nextDouble()) * scale
      }
      .filter(d => d >= min && d <= max)
      .next()
}

class MemberAuthenticationServiceImpl(
    synchronizerId: PhysicalSynchronizerId,
    cryptoApi: SynchronizerCryptoClient,
    store: MemberAuthenticationStore,
    clock: Clock,
    nonceExpirationInterval: Duration,
    maxTokenExpirationInterval: Duration,
    useExponentialRandomTokenExpiration: Boolean,
    invalidateMemberCallback: Traced[Member] => Unit,
    isTopologyInitialized: FutureUnlessShutdown[Unit],
    timeouts: ProcessingTimeout,
    batchingConfig: BatchingConfig,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends MemberAuthenticationService(
      synchronizerId,
      cryptoApi,
      store,
      clock,
      nonceExpirationInterval = nonceExpirationInterval,
      maxTokenExpirationInterval = maxTokenExpirationInterval,
      useExponentialRandomTokenExpiration = useExponentialRandomTokenExpiration,
      invalidateMemberCallback,
      isTopologyInitialized,
      timeouts,
      batchingConfig,
      loggerFactory,
    )
    with TopologyTransactionProcessingSubscriber {

  // ensure the authentication service receives topology transactions last
  override val executionOrder: Int = Int.MaxValue

  private def safelyRevokeLeavers[T <: Member](
      leavers: Set[T],
      isActiveCheck: T => FutureUnlessShutdown[Boolean],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    if (leavers.isEmpty) FutureUnlessShutdown.unit
    else {
      val memberType = leavers.headOption
        .map(_.getClass.getSimpleName.stripSuffix("$").stripSuffix("Id"))
        .getOrElse("Member")
      logger.info(s"Calling invalidateAndExpire for all $memberType members in $leavers")
      MonadUtil.parTraverseWithLimit_(batchingConfig.parallelism)(leavers.toList) { leaver =>
        // use .recover to ensure that a failure in revoking one member
        // does not abort the parallel processing of other leavers.
        invalidateAndExpire(isActiveCheck)(leaver).recover { case ex =>
          logger.error(s"Failed to revoke tokens for $memberType $leaver", ex)
          UnlessShutdown.unit
        }
      }
    }

  /** synchronizer topology client subscriber used to remove member tokens if they get disabled */
  override def observed(
      sequencerTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sc: SequencerCounter,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    synchronizeWithClosing(functionFullName) {
      FutureUnlessShutdown.sequence(transactions.map(_.transaction).map {

        // case 1: a SynchronizerTrustCertificate topology mapping (for a participant) was removed or replaced
        case TopologyTransaction(
              operation,
              _serial,
              cert: SynchronizerTrustCertificate,
            ) =>
          val participant = cert.participantId
          //  if the certificate is removed, check the snapshot to confirm not active, revoke tokens. otherwise do nothing
          operation match {
            case TopologyChangeOp.Remove =>
              logger.info(
                s"Received topology transaction for removal of SynchronizerTrustCertificate for Participant $participant. Calling invalidateAndExpire"
              )
              invalidateAndExpire(isParticipantActive)(participant)
            case TopologyChangeOp.Replace =>
              FutureUnlessShutdown.unit
          }

        // case 2: a ParticipantSynchronizerPermission topology mapping was replaced (loginAfter may be in the future)
        case TopologyTransaction(
              TopologyChangeOp.Replace,
              _serial,
              cert: ParticipantSynchronizerPermission,
            ) =>
          val participant = cert.participantId
          // if the loginAfter is in the future, check the snapshot to confirm not active, revoke tokens. otherwise do nothing
          if (cert.loginAfter.exists(_ > clock.now)) {
            logger.info(
              s"Received topology transaction shifting loginAfter for Participant $participant to the future. Calling invalidateAndExpire"
            )
            invalidateAndExpire(isParticipantActive)(participant)
          } else {
            FutureUnlessShutdown.unit
          }

        // case 3: a ParticipantSynchronizerPermission topology mapping was removed (participant removed)
        case TopologyTransaction(
              TopologyChangeOp.Remove,
              _serial,
              cert: ParticipantSynchronizerPermission,
            ) =>
          val participant = cert.participantId
          logger.info(
            s"Received topology transaction for revoking $participant's access. Calling invalidateAndExpire."
          )
          invalidateAndExpire(isParticipantActive)(participant)

        // case 4: a SequencerSynchronizerState removal topology transaction is received (a sequencer group removed)
        case TopologyTransaction(
              TopologyChangeOp.Remove,
              _serial,
              newState: SequencerSynchronizerState, // state being removed
            ) =>
          val allRemovedSequencers = newState.allSequencers.toSet
          logger.info(
            s"Received topology transaction for removing sequencer group on ${newState.synchronizerId}."
          )
          safelyRevokeLeavers(allRemovedSequencers, isSequencerActive)

        // case 5: a MediatorSynchronizerState removal topology transaction is received (a mediator group removed)
        case TopologyTransaction(
              TopologyChangeOp.Remove,
              _serial,
              newState: MediatorSynchronizerState, // state being removed
            ) =>
          val allRemovedMediators = newState.allMediatorsInGroup.toSet
          logger.info(
            s"Received topology transaction for removing mediator group ${newState.group}."
          )
          safelyRevokeLeavers(allRemovedMediators, isMediatorActive)

        // case 6: a replace SequencerSynchronizerState topology mapping was replaced (subset of sequencers removed)
        // we obtain the topology state at the previous timestamp and compare
        case TopologyTransaction(
              TopologyChangeOp.Replace,
              _serial,
              newState: SequencerSynchronizerState,
            ) =>
          val newSequencerSet = newState.allSequencers.toSet

          // we want to capture the timestamp at the previous state, but we do not use .immediatePredecessor here
          // because the state changes at time T are captured only at T+1
          val measuredTimestampPreviousState = effectiveTimestamp.value

          for {
            snapshot <- cryptoApi.ipsSnapshot(
              measuredTimestampPreviousState
            ) // get the snapshot at the previous timestamp
            group <- snapshot.sequencerGroup() // get the modified group
            oldSequencerSet = group
              .map(g => (g.active ++ g.passive).toSet)
              .getOrElse(Set.empty[SequencerId]) // get the set of all sequencers in the group
            leavers = oldSequencerSet.diff(newSequencerSet)
            _ = logger.info(
              s"Received topology transaction for removing some sequencers on ${newState.synchronizerId}."
            )
            _ <- safelyRevokeLeavers(leavers, isSequencerActive)
          } yield ()

        // case 7: a replace MediatorSynchronizerState topology transaction is received (mediator group replaced, subset of mediators removed)
        case TopologyTransaction(
              TopologyChangeOp.Replace,
              _serial,
              newState: MediatorSynchronizerState, // state being replaced
            ) =>
          val modifiedMediatorIndex = newState.group // group index
          val newMediatorSet: Set[MediatorId] = newState.allMediatorsInGroup.toSet
          val measuredTimestampPreviousState = effectiveTimestamp.value
          for {
            snapshot <- cryptoApi
              .ipsSnapshot(measuredTimestampPreviousState)
            groups <- snapshot.mediatorGroups() // get all the groups in the previous state
            changedGroupInOld = groups.find(
              _.index == modifiedMediatorIndex
            ) // filter for the modified group index
            oldMediatorSet = changedGroupInOld
              .map(group => (group.active ++ group.passive).toSet)
              .getOrElse(Set.empty[MediatorId])
            leavers = oldMediatorSet.diff(newMediatorSet)
            _ = logger.info(
              s"Received topology transaction for removing some mediators from group $modifiedMediatorIndex."
            )
            _ <- safelyRevokeLeavers(leavers, isMediatorActive)
          } yield ()

        // All other transactions: do nothing
        case TopologyTransaction(
              TopologyChangeOp.Replace | TopologyChangeOp.Remove, // operation
              _, // serial
              (
                _: NamespaceDelegation | // mapping
                _: DecentralizedNamespaceDefinition | _: OwnerToKeyMapping | _: PartyToKeyMapping |
                _: PartyToParticipant | _: VettedPackages | _: PartyHostingLimits |
                _: SynchronizerParametersState | _: DynamicSequencingParametersState |
                _: SynchronizerUpgradeAnnouncement | _: SequencerConnectionSuccessor
              ),
            ) =>
          FutureUnlessShutdown.unit

      })
    }.map(_ => ())
}

trait MemberAuthenticationServiceFactory {
  def createAndSubscribe(
      syncCrypto: SynchronizerCryptoClient,
      store: MemberAuthenticationStore,
      invalidateMemberCallback: Traced[Member] => Unit,
      isTopologyInitialized: FutureUnlessShutdown[Unit],
  )(implicit ec: ExecutionContext): MemberAuthenticationService
}

object MemberAuthenticationServiceFactory {

  def apply(
      synchronizerId: PhysicalSynchronizerId,
      clock: Clock,
      nonceExpirationInterval: Duration,
      maxTokenExpirationInterval: Duration,
      useExponentialRandomTokenExpiration: Boolean,
      timeouts: ProcessingTimeout,
      batchingConfig: BatchingConfig,
      loggerFactory: NamedLoggerFactory,
      topologyTransactionProcessor: TopologyTransactionProcessor,
  ): MemberAuthenticationServiceFactory =
    new MemberAuthenticationServiceFactory {
      override def createAndSubscribe(
          syncCrypto: SynchronizerCryptoClient,
          store: MemberAuthenticationStore,
          invalidateMemberCallback: Traced[Member] => Unit,
          isTopologyInitialized: FutureUnlessShutdown[Unit],
      )(implicit ec: ExecutionContext): MemberAuthenticationService = {
        val service = new MemberAuthenticationServiceImpl(
          synchronizerId,
          syncCrypto,
          store,
          clock,
          nonceExpirationInterval = nonceExpirationInterval,
          maxTokenExpirationInterval = maxTokenExpirationInterval,
          useExponentialRandomTokenExpiration = useExponentialRandomTokenExpiration,
          invalidateMemberCallback,
          isTopologyInitialized,
          timeouts,
          batchingConfig,
          loggerFactory,
        )
        topologyTransactionProcessor.subscribe(service)
        service
      }
    }

}
