// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.authentication

import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.bifunctor.*
import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
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

import java.time.Duration
import scala.concurrent.ExecutionContext

/** The authentication service issues tokens to members after they have successfully completed the following challenge
  * response protocol and after they have accepted the service agreement of the domain. The tokens are required for
  * connecting to the sequencer.
  *
  * In order for a member to subscribe to the sequencer, it must follow a few steps for it to authenticate.
  * Assuming the domain already has knowledge of the member's public keys, the following steps are to be taken:
  *   1. member sends request to the domain for authenticating
  *   2. domain returns a nonce (a challenge random number)
  *   3. member takes the nonce, concatenates it with the identity of the domain, signs it and sends it back
  *   4. domain checks the signature against the key of the member. if it matches, create a token and return it
  *   5. member will use the token when subscribing to the sequencer
  *
  * @param invalidateMemberCallback Called when a member is explicitly deactivated on the domain so all active subscriptions
  *                                 for this member should be terminated.
  */
class MemberAuthenticationService(
    synchronizerId: SynchronizerId,
    cryptoApi: DomainSyncCryptoClient,
    store: MemberAuthenticationStore,
    clock: Clock,
    nonceExpirationInterval: Duration,
    maxTokenExpirationInterval: Duration,
    useExponentialRandomTokenExpiration: Boolean,
    invalidateMemberCallback: Traced[Member] => Unit,
    isTopologyInitialized: FutureUnlessShutdown[Unit],
    override val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  /** Domain generates nonce that he expects the participant to use to concatenate with the domain's id and sign
    * to proceed with the authentication (step 2). We expect to find a key with usage 'SequencerAuthentication' to
    * sign these messages.
    */
  def generateNonce(member: Member)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, AuthenticationError, (Nonce, NonEmpty[Seq[Fingerprint]])] =
    for {
      _ <- EitherT.right(waitForInitialized)
      snapshot = cryptoApi.ips.currentSnapshotApproximation
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

  /** Domain checks that the signature given by the member matches and returns a token if it does (step 4)
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
      hash = authentication.hashDomainNonce(nonce, synchronizerId, cryptoApi.pureCrypto)
      snapshot = cryptoApi.currentSnapshotApproximation

      _ <- snapshot
        .verifySignature(
          hash,
          member,
          signature,
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

  /** Domain checks if the token given by the participant is the one previously assigned to it for authentication.
    * The participant also provides the synchronizer id for which they think they are connecting to. If this id does not match
    * this domain's id, it means the participant was previously connected to a different domain on the same address and
    * now should be informed that this address now hosts a different domain.
    */
  def validateToken(
      intendedSynchronizerId: SynchronizerId,
      member: Member,
      token: AuthenticationToken,
  ): Either[AuthenticationError, StoredAuthenticationToken] =
    for {
      _ <- correctDomain(member, intendedSynchronizerId)
      validTokenO = store.fetchTokens(member).filter(_.expireAt > clock.now).find(_.token == token)
      validToken <- validTokenO.toRight(MissingToken(member)).leftWiden[AuthenticationError]
    } yield validToken

  def invalidateMemberWithToken(
      token: AuthenticationToken
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[LogoutTokenDoesNotExist.type, Unit]] =
    for {
      _ <- waitForInitialized
      storedTokenO = store.fetchToken(token)
      res <- storedTokenO match {
        case None => FutureUnlessShutdown.pure(Left(LogoutTokenDoesNotExist))
        case Some(storedToken) =>
          // Force invalidation, whether the member is actually active or not
          invalidateAndExpire(isActiveCheck = (_: Member) => FutureUnlessShutdown.pure(false))(
            storedToken.member
          ).map(Right(_))
      }
    } yield res

  private def ignoreExpired[A <: HasExpiry](itemO: Option[A]): Option[A] =
    itemO.filter(_.expireAt > clock.now)

  private def scheduleExpirations(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Unit = {
    def run(): Unit = performUnlessClosing(functionFullName) {
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
        EitherT.leftT(AuthenticationNotSupportedForMember(sequencer))
    }

  private def correctDomain(
      member: Member,
      intendedSynchronizerId: SynchronizerId,
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
    Seq(cryptoApi.headSnapshot, cryptoApi.currentSnapshotApproximation)
      .map(_.ipsSnapshot)
      .parTraverse(check(_))
      .map(_.forall(identity))

  protected def isParticipantActive(participant: ParticipantId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] = isMemberActive(
    _.isParticipantActiveAndCanLoginAt(participant, clock.now)
  )

  protected def isMediatorActive(mediator: MediatorId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] = isMemberActive(_.isMediatorActive(mediator))

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

  /** Generates a random delay between min and max, following an exponential distribution with the given scale.
    * The delay is truncated to the range [min, max]. The interval should be sufficiently large around the scale,
    *  to avoid long sampling times or even running indefinitely.
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
    synchronizerId: SynchronizerId,
    cryptoApi: DomainSyncCryptoClient,
    store: MemberAuthenticationStore,
    clock: Clock,
    nonceExpirationInterval: Duration,
    maxTokenExpirationInterval: Duration,
    useExponentialRandomTokenExpiration: Boolean,
    invalidateMemberCallback: Traced[Member] => Unit,
    isTopologyInitialized: FutureUnlessShutdown[Unit],
    timeouts: ProcessingTimeout,
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
      loggerFactory,
    )
    with TopologyTransactionProcessingSubscriber {

  /** domain topology client subscriber used to remove member tokens if they get disabled */
  override def observed(
      sequencerTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sc: SequencerCounter,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    performUnlessClosingUSF(functionFullName) {
      FutureUnlessShutdown.sequence(transactions.map(_.transaction).map {
        case TopologyTransaction(
              TopologyChangeOp.Remove,
              _serial,
              cert: DomainTrustCertificate,
            ) =>
          val participant = cert.participantId
          logger.info(
            s"Domain trust certificate of $participant was removed, forcefully disconnecting the participant."
          )
          invalidateAndExpire(isParticipantActive)(participant)
        case TopologyTransaction(
              TopologyChangeOp.Replace,
              _serial,
              cert: ParticipantDomainPermission,
            ) if cert.loginAfter.exists(_ > clock.now) =>
          val participant = cert.participantId
          logger.info(
            s"$participant is disabled until ${cert.loginAfter}. Removing any token and booting the participant"
          )
          invalidateAndExpire(isParticipantActive)(participant)
        case TopologyTransaction(
              TopologyChangeOp.Remove,
              _serial,
              cert: ParticipantDomainPermission,
            ) =>
          val participant = cert.participantId
          logger.info(
            s"$participant's access has been revoked by the domain. Removing any token and booting the participant"
          )
          invalidateAndExpire(isParticipantActive)(participant)

        case _ => FutureUnlessShutdown.unit
      })
    }.map(_ => ())
}

trait MemberAuthenticationServiceFactory {
  def createAndSubscribe(
      syncCrypto: DomainSyncCryptoClient,
      store: MemberAuthenticationStore,
      invalidateMemberCallback: Traced[Member] => Unit,
      isTopologyInitialized: FutureUnlessShutdown[Unit],
  )(implicit ec: ExecutionContext): MemberAuthenticationService
}

object MemberAuthenticationServiceFactory {

  def apply(
      synchronizerId: SynchronizerId,
      clock: Clock,
      nonceExpirationInterval: Duration,
      maxTokenExpirationInterval: Duration,
      useExponentialRandomTokenExpiration: Boolean,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      topologyTransactionProcessor: TopologyTransactionProcessor,
  ): MemberAuthenticationServiceFactory =
    new MemberAuthenticationServiceFactory {
      override def createAndSubscribe(
          syncCrypto: DomainSyncCryptoClient,
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
          loggerFactory,
        )
        topologyTransactionProcessor.subscribe(service)
        service
      }
    }

}
