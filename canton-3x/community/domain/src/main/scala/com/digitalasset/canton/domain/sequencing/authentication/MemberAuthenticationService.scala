// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.authentication

import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.bifunctor.*
import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.common.domain.ServiceAgreementId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.service.ServiceAgreementManager
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.DbStorage.PassiveInstanceException
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication.*
import com.digitalasset.canton.sequencing.authentication.grpc.AuthenticationTokenWithExpiry
import com.digitalasset.canton.sequencing.authentication.{AuthenticationToken, MemberAuthentication}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.processing.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.FutureUtil

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}

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
    domain: DomainId,
    cryptoApi: DomainSyncCryptoClient,
    store: MemberAuthenticationStore,
    agreementManager: Option[ServiceAgreementManager],
    clock: Clock,
    nonceExpirationTime: Duration,
    tokenExpirationTime: Duration,
    invalidateMemberCallback: Traced[Member] => Unit,
    isTopologyInitialized: Future[Unit],
    override val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  protected val tokenCache = new AuthenticationTokenCache(clock, store, loggerFactory)

  /** Domain generates nonce that he expects the participant to use to concatenate with the domain's id and sign
    * to proceed with the authentication (step 2).
    */
  def generateNonce(member: Member)(implicit
      traceContext: TraceContext
  ): EitherT[Future, AuthenticationError, (Nonce, NonEmpty[Seq[Fingerprint]])] = {
    for {
      _ <- EitherT.right(waitForInitialized)
      snapshot = cryptoApi.ips.currentSnapshotApproximation
      _ <- isActive(member)
      fingerprints <- EitherT(
        snapshot.signingKeys(member).map { keys =>
          NonEmpty
            .from(keys.map(_.fingerprint))
            .toRight(NoKeysRegistered(member): AuthenticationError)
        }
      )
      nonce = Nonce.generate(cryptoApi.pureCrypto)
      storedNonce = StoredNonce(member, nonce, clock.now, nonceExpirationTime)
      _ <- handlePassiveInstanceException(store.saveNonce(storedNonce))
    } yield {
      scheduleExpirations(storedNonce.expireAt)
      (nonce, fingerprints)
    }
  }

  private def waitForInitialized(implicit traceContext: TraceContext): Future[Unit] = {
    // avoid logging if we're already done
    if (isTopologyInitialized.isCompleted) isTopologyInitialized
    else {
      logger.debug(s"Waiting for topology to be initialized")

      isTopologyInitialized.map { _ =>
        logger.debug(s"Topology has been initialized")
      }
    }
  }

  private def handlePassiveInstanceException[A](
      future: Future[A]
  ): EitherT[Future, AuthenticationError, A] =
    EitherT(
      future
        .map(Right(_))
        .recover { case _: PassiveInstanceException =>
          Left(PassiveSequencer: AuthenticationError)
        }
    )

  /** Domain checks that the signature given by the member matches and returns a token if it does (step 4)
    * Al
    */
  def validateSignature(member: Member, signature: Signature, providedNonce: Nonce)(implicit
      traceContext: TraceContext
  ): EitherT[Future, AuthenticationError, AuthenticationTokenWithExpiry] =
    for {
      _ <- EitherT.right(waitForInitialized)
      _ <- isActive(member)
      value <-
        handlePassiveInstanceException(store.fetchAndRemoveNonce(member, providedNonce))
          .map(ignoreExpired)
          .subflatMap(_.toRight(MissingNonce(member): AuthenticationError))
      StoredNonce(_, nonce, generatedAt, _expireAt) = value
      agreementId = agreementManager.map(_.agreement.id)
      authentication <- EitherT.fromEither(MemberAuthentication(member))
      hash = authentication.hashDomainNonce(nonce, domain, agreementId, cryptoApi.pureCrypto)
      snapshot = cryptoApi.currentSnapshotApproximation

      _ <- snapshot.verifySignature(hash, member, signature).leftMap { err =>
        logger.warn(s"Member $member provided invalid signature: $err")
        InvalidSignature(member)
      }
      // If an agreement manager is set, we store the acceptance
      _ <- agreementManager.fold(EitherT.rightT[Future, AuthenticationError](())) { manager =>
        storeAcceptedAgreement(member, manager, manager.agreement.id, signature, generatedAt)
      }
      token = AuthenticationToken.generate(cryptoApi.pureCrypto)
      tokenExpiry = clock.now.add(tokenExpirationTime)
      storedToken = StoredAuthenticationToken(member, tokenExpiry, token)
      _ <- handlePassiveInstanceException(tokenCache.saveToken(storedToken))
    } yield {
      logger.info(
        s"$member authenticated new token with expiry $tokenExpiry"
      )
      AuthenticationTokenWithExpiry(token, tokenExpiry)
    }

  /** Domain checks if the token given by the participant is the one previously assigned to it for authentication.
    * The participant also provides the domain id for which they think they are connecting to. If this id does not match
    * this domain's id, it means the participant was previously connected to a different domain on the same address and
    * now should be informed that this address now hosts a different domain.
    */
  def validateToken(intendedDomain: DomainId, member: Member, token: AuthenticationToken)(implicit
      traceContext: TraceContext
  ): EitherT[Future, AuthenticationError, StoredAuthenticationToken] =
    for {
      _ <- EitherT.fromEither[Future](correctDomain(member, intendedDomain))
      validTokenO <- handlePassiveInstanceException(tokenCache.lookupMatchingToken(member, token))
      validToken <- EitherT
        .fromEither[Future](validTokenO.toRight(MissingToken(member)))
        .leftWiden[AuthenticationError]
    } yield validToken

  private def ignoreExpired[A <: HasExpiry](itemO: Option[A]): Option[A] =
    itemO.filter(_.expireAt > clock.now)

  private def scheduleExpirations(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Unit = {
    def run(): Unit = FutureUtil.doNotAwait(
      performUnlessClosingF(functionFullName) {
        val now = clock.now
        logger.debug(s"Expiring nonces and tokens up to $now")
        handlePassiveInstanceException(store.expireNoncesAndTokens(now)).value
      }.unwrap,
      "Expiring nonces and tokens failed",
    )
    clock.scheduleAt(_ => run(), timestamp).discard
  }

  private def isActive(
      member: Member
  )(implicit traceContext: TraceContext): EitherT[Future, AuthenticationError, Unit] =
    member match {
      case participant: ParticipantId =>
        EitherT(isParticipantActive(participant).map {
          if (_) Right(()) else Left(ParticipantDisabled(participant))
        })
      case mediator: MediatorId =>
        EitherT(isMediatorActive(mediator).map {
          if (_) Right(()) else Left(MediatorDisabled(mediator))
        })
      case _ =>
        // TODO(#4933) check if sequencer is active
        EitherT.rightT(())
    }

  private def storeAcceptedAgreement(
      member: Member,
      agreementManager: ServiceAgreementManager,
      agreementId: ServiceAgreementId,
      signature: Signature,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): EitherT[Future, AuthenticationError, Unit] =
    member match {
      case participant: ParticipantId =>
        agreementManager
          .insertAcceptance(agreementId, participant, signature, timestamp)
          .leftMap(err => ServiceAgreementAcceptanceError(member, err.toString))
      case _ => EitherT.rightT(())
    }

  private def correctDomain(
      member: Member,
      intendedDomain: DomainId,
  ): Either[AuthenticationError, Unit] =
    Either.cond(intendedDomain == domain, (), NonMatchingDomainId(member, intendedDomain))

  protected def isMemberActive(check: TopologySnapshot => Future[Boolean])(implicit
      traceContext: TraceContext
  ): Future[Boolean] = {
    cryptoApi.snapshot(cryptoApi.topologyKnownUntilTimestamp).flatMap { snapshot =>
      // we are a bit more conservative here. a member needs to be active NOW and the head state (i.e. effective in the future)
      Seq(snapshot.ipsSnapshot, cryptoApi.currentSnapshotApproximation.ipsSnapshot)
        .parTraverse(check(_))
        .map(_.forall(identity))
    }
  }

  protected def isParticipantActive(participant: ParticipantId)(implicit
      traceContext: TraceContext
  ): Future[Boolean] = isMemberActive(_.isParticipantActive(participant))

  protected def isMediatorActive(mediator: MediatorId)(implicit
      traceContext: TraceContext
  ): Future[Boolean] = isMemberActive(_.isMediatorActive(mediator))
}

class MemberAuthenticationServiceOld(
    domain: DomainId,
    cryptoApi: DomainSyncCryptoClient,
    store: MemberAuthenticationStore,
    agreementManager: Option[ServiceAgreementManager],
    clock: Clock,
    nonceExpirationTime: Duration,
    tokenExpirationTime: Duration,
    invalidateMemberCallback: Traced[Member] => Unit,
    isTopologyInitialized: Future[Unit],
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends MemberAuthenticationService(
      domain,
      cryptoApi,
      store,
      agreementManager,
      clock,
      nonceExpirationTime = nonceExpirationTime,
      tokenExpirationTime = tokenExpirationTime,
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
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.lift(performUnlessClosing(functionFullName) {
      transactions.map(_.transaction.element.mapping).foreach {
        case ParticipantState(_, _, participant, ParticipantPermission.Disabled, _) =>
          def invalidateAndExpire: Future[Unit] = {
            isParticipantActive(participant).flatMap { isActive =>
              if (!isActive) {
                logger.debug(s"Expiring all auth-tokens of ${participant}")
                tokenCache
                  // first, remove all auth tokens
                  .invalidateAllTokensForMember(participant)
                  // second, ensure the sequencer client gets disconnected
                  .map(_ => invalidateMemberCallback(Traced(participant)))
              } else Future.unit
            }
          }
          FutureUtil.doNotAwait(
            invalidateAndExpire,
            s"Invalidating participant authentication for $participant",
          )
        case _ =>
      }
    })

  override def onClosed(): Unit = Lifecycle.close(store)(logger)
}

class MemberAuthenticationServiceX(
    domain: DomainId,
    cryptoApi: DomainSyncCryptoClient,
    store: MemberAuthenticationStore,
    agreementManager: Option[ServiceAgreementManager],
    clock: Clock,
    nonceExpirationTime: Duration,
    tokenExpirationTime: Duration,
    invalidateMemberCallback: Traced[Member] => Unit,
    isTopologyInitialized: Future[Unit],
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends MemberAuthenticationService(
      domain,
      cryptoApi,
      store,
      agreementManager,
      clock,
      nonceExpirationTime = nonceExpirationTime,
      tokenExpirationTime = tokenExpirationTime,
      invalidateMemberCallback,
      isTopologyInitialized,
      timeouts,
      loggerFactory,
    )
    with TopologyTransactionProcessingSubscriberX {

  /** domain topology client subscriber used to remove member tokens if they get disabled */
  override def observed(
      sequencerTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sc: SequencerCounter,
      transactions: Seq[GenericSignedTopologyTransactionX],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.lift(performUnlessClosing(functionFullName) {
      transactions.flatMap(_.transaction.selectMapping[DomainTrustCertificateX]).foreach {
        case TopologyTransactionX(
              TopologyChangeOpX.Remove,
              _serial,
              cert: DomainTrustCertificateX,
            ) =>
          val participant = cert.participantId
          def invalidateAndExpire: Future[Unit] = {
            isParticipantActive(participant).flatMap { isActive =>
              if (!isActive) {
                logger.debug(s"Expiring all auth-tokens of ${participant}")
                tokenCache
                  // first, remove all auth tokens
                  .invalidateAllTokensForMember(participant)
                  // second, ensure the sequencer client gets disconnected
                  .map(_ => invalidateMemberCallback(Traced(participant)))
              } else Future.unit
            }
          }
          FutureUtil.doNotAwait(
            invalidateAndExpire,
            s"Invalidating participant authentication for $participant",
          )
        case _ =>
      }
    })

  override def onClosed(): Unit = Lifecycle.close(store)(logger)
}

trait MemberAuthenticationServiceFactory {
  def createAndSubscribe(
      syncCrypto: DomainSyncCryptoClient,
      store: MemberAuthenticationStore,
      agreementManager: Option[ServiceAgreementManager],
      invalidateMemberCallback: Traced[Member] => Unit,
      isTopologyInitialized: Future[Unit],
  )(implicit ec: ExecutionContext): MemberAuthenticationService
}

object MemberAuthenticationServiceFactory {
  def forOld(
      domain: DomainId,
      clock: Clock,
      nonceExpirationTime: Duration,
      tokenExpirationTime: Duration,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      topologyTransactionProcessor: TopologyTransactionProcessor,
  ): MemberAuthenticationServiceFactory =
    new MemberAuthenticationServiceFactory {
      override def createAndSubscribe(
          syncCrypto: DomainSyncCryptoClient,
          store: MemberAuthenticationStore,
          agreementManager: Option[ServiceAgreementManager],
          invalidateMemberCallback: Traced[Member] => Unit,
          isTopologyInitialized: Future[Unit],
      )(implicit ec: ExecutionContext): MemberAuthenticationService = {
        val service = new MemberAuthenticationServiceOld(
          domain,
          syncCrypto,
          store,
          agreementManager,
          clock,
          nonceExpirationTime = nonceExpirationTime,
          tokenExpirationTime = tokenExpirationTime,
          invalidateMemberCallback,
          isTopologyInitialized,
          timeouts,
          loggerFactory,
        )
        topologyTransactionProcessor.subscribe(service)
        service
      }
    }

  def forX(
      domain: DomainId,
      clock: Clock,
      nonceExpirationTime: Duration,
      tokenExpirationTime: Duration,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      topologyTransactionProcessorX: TopologyTransactionProcessorX,
  ): MemberAuthenticationServiceFactory =
    new MemberAuthenticationServiceFactory {
      override def createAndSubscribe(
          syncCrypto: DomainSyncCryptoClient,
          store: MemberAuthenticationStore,
          agreementManager: Option[ServiceAgreementManager],
          invalidateMemberCallback: Traced[Member] => Unit,
          isTopologyInitialized: Future[Unit],
      )(implicit ec: ExecutionContext): MemberAuthenticationService = {
        val service = new MemberAuthenticationServiceX(
          domain,
          syncCrypto,
          store,
          agreementManager,
          clock,
          nonceExpirationTime = nonceExpirationTime,
          tokenExpirationTime = tokenExpirationTime,
          invalidateMemberCallback,
          isTopologyInitialized,
          timeouts,
          loggerFactory,
        )
        topologyTransactionProcessorX.subscribe(service)
        service
      }
    }

}
