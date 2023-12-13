// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.Monad
import cats.data.EitherT
import cats.implicits.catsSyntaxValidatedId
import cats.syntax.alternative.*
import cats.syntax.either.*
import cats.syntax.flatMap.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.{FutureSupervisor, HasFutureSupervision}
import com.digitalasset.canton.config.{CacheConfig, CachingConfigs, ProcessingTimeout}
import com.digitalasset.canton.crypto.SignatureCheckError.{
  SignatureWithWrongKey,
  SignerHasNoValidKeys,
}
import com.digitalasset.canton.crypto.SyncCryptoError.{KeyNotAvailable, SyncCryptoEncryptionError}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  Lifecycle,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{
  DomainTopologyClient,
  IdentityProvidingServiceClient,
  TopologyClientApi,
  TopologySnapshot,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.LoggerUtil
import com.digitalasset.canton.version.{HasVersionedToByteString, ProtocolVersion}
import com.digitalasset.canton.{DomainAlias, checked}
import com.google.protobuf.ByteString
import org.slf4j.event.Level

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

/** Crypto API Provider class
  *
  * The utility class combines the information provided by the IPSclient, the pure crypto functions
  * and the signing and decryption operations on a private key vault in order to automatically resolve
  * the right keys to use for signing / decryption based on domain and timestamp.
  */
class SyncCryptoApiProvider(
    val member: Member,
    val ips: IdentityProvidingServiceClient,
    val crypto: Crypto,
    cachingConfigs: CachingConfigs,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext) {

  require(ips != null)

  def pureCrypto: CryptoPureApi = crypto.pureCrypto

  def tryForDomain(domain: DomainId, alias: Option[DomainAlias] = None): DomainSyncCryptoClient =
    new DomainSyncCryptoClient(
      member,
      domain,
      ips.tryForDomain(domain),
      crypto,
      cachingConfigs,
      timeouts,
      futureSupervisor,
      loggerFactory.append("domainId", domain.toString),
    )

  def forDomain(domain: DomainId): Option[DomainSyncCryptoClient] =
    for {
      dips <- ips.forDomain(domain)
    } yield new DomainSyncCryptoClient(
      member,
      domain,
      dips,
      crypto,
      cachingConfigs,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )
}

trait SyncCryptoClient[+T <: SyncCryptoApi] extends TopologyClientApi[T] {
  this: HasFutureSupervision =>

  def pureCrypto: CryptoPureApi

  /** Returns a snapshot of the current member topology for the given domain.
    * The future will log a warning and await the snapshot if the data is not there yet.
    *
    * The snapshot returned by this method should be used for validating transaction and transfer requests (Phase 2 - 7).
    * Use the request timestamp as parameter for this method.
    * Do not use a response or result timestamp, because all validation steps must use the same topology snapshot.
    */
  def ipsSnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[TopologySnapshot]

  /** Returns a snapshot of the current member topology for the given domain
    *
    * The future will wait for the data if the data is not there yet.
    *
    * The snapshot returned by this method should be used for validating transaction and transfer requests (Phase 2 - 7).
    * Use the request timestamp as parameter for this method.
    * Do not use a response or result timestamp, because all validation steps must use the same topology snapshot.
    */
  def awaitIpsSnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[TopologySnapshot]

  def awaitIpsSnapshotUS(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[TopologySnapshot]

  def awaitIpsSnapshotUSSupervised(description: => String, warnAfter: Duration = 10.seconds)(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[TopologySnapshot] =
    supervisedUS(description, warnAfter)(awaitIpsSnapshotUS(timestamp))

}

object SyncCryptoClient {

  /** Computes the snapshot for the desired timestamp, assuming that the last (relevant) update to the
    * topology state happened at or before `previousTimestamp`.
    * If `previousTimestampO` is [[scala.None$]] and `desiredTimestamp` is currently not known
    * [[com.digitalasset.canton.topology.client.TopologyClientApi.topologyKnownUntilTimestamp]],
    * then the current approximation is returned and if `warnIfApproximate` is set a warning is logged.
    */
  def getSnapshotForTimestampUS(
      client: SyncCryptoClient[SyncCryptoApi],
      desiredTimestamp: CantonTimestamp,
      previousTimestampO: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
      warnIfApproximate: Boolean = true,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: ErrorLoggingContext,
      closeContext: CloseContext,
  ): FutureUnlessShutdown[SyncCryptoApi] = getSnapshotForTimestampInternal[FutureUnlessShutdown](
    client,
    desiredTimestamp,
    previousTimestampO,
    warnIfApproximate,
  )(
    (timestamp, traceContext) => client.snapshotUS(timestamp)(traceContext),
    (description, timestamp, traceContext) =>
      client.awaitSnapshotUSSupervised(description)(timestamp)(traceContext),
    { (snapshot, traceContext) =>
      {
        closeContext.context.performUnlessClosingF(
          "get-dynamic-domain-parameters"
        ) {
          snapshot
            .findDynamicDomainParametersOrDefault(
              protocolVersion = protocolVersion,
              warnOnUsingDefault = false,
            )(traceContext)
        }(executionContext, traceContext)
      }
    },
  )

  /** Computes the snapshot for the desired timestamp, assuming that the last (relevant) update to the
    * topology state happened at or before `previousTimestamp`.
    * If `previousTimestampO` is [[scala.None$]] and `desiredTimestamp` is currently not known
    * [[com.digitalasset.canton.topology.client.TopologyClientApi.topologyKnownUntilTimestamp]],
    * then the current approximation is returned and if `warnIfApproximate` is set a warning is logged.
    */
  def getSnapshotForTimestamp(
      client: SyncCryptoClient[SyncCryptoApi],
      desiredTimestamp: CantonTimestamp,
      previousTimestampO: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
      warnIfApproximate: Boolean = true,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: ErrorLoggingContext,
  ): Future[SyncCryptoApi] = {
    getSnapshotForTimestampInternal[Future](
      client,
      desiredTimestamp,
      previousTimestampO,
      warnIfApproximate,
    )(
      (timestamp, traceContext) => client.snapshot(timestamp)(traceContext),
      (description, timestamp, traceContext) =>
        client.awaitSnapshotSupervised(description)(timestamp)(traceContext),
      { (snapshot, traceContext) =>
        snapshot
          .findDynamicDomainParametersOrDefault(
            protocolVersion = protocolVersion,
            warnOnUsingDefault = false,
          )(traceContext)
      },
    )
  }

  // Base version of getSnapshotForTimestamp abstracting over the effect type to allow for
  // a `Future` and `FutureUnlessShutdown` version. Once we migrate all usages to the US version, this abstraction
  // should not be needed anymore
  private def getSnapshotForTimestampInternal[F[_]](
      client: SyncCryptoClient[SyncCryptoApi],
      desiredTimestamp: CantonTimestamp,
      previousTimestampO: Option[CantonTimestamp],
      warnIfApproximate: Boolean = true,
  )(
      getSnapshot: (CantonTimestamp, TraceContext) => F[SyncCryptoApi],
      awaitSnapshotSupervised: (String, CantonTimestamp, TraceContext) => F[SyncCryptoApi],
      dynamicDomainParameters: (TopologySnapshot, TraceContext) => F[DynamicDomainParameters],
  )(implicit
      loggingContext: ErrorLoggingContext,
      monad: Monad[F],
  ): F[SyncCryptoApi] = {
    implicit val traceContext: TraceContext = loggingContext.traceContext
    val knownUntil = client.topologyKnownUntilTimestamp
    val snapshotKnown = desiredTimestamp <= knownUntil
    loggingContext.logger.debug(
      s"${if (snapshotKnown) "Getting" else "Waiting for"} topology snapshot at $desiredTimestamp; known until $knownUntil; previous $previousTimestampO"
    )
    if (snapshotKnown) {
      getSnapshot(desiredTimestamp, traceContext)
    } else {
      previousTimestampO match {
        case None =>
          val approximateSnapshot = client.currentSnapshotApproximation
          LoggerUtil.logAtLevel(
            if (warnIfApproximate) Level.WARN else Level.INFO,
            s"Using approximate topology snapshot at ${approximateSnapshot.ipsSnapshot.timestamp} for desired timestamp $desiredTimestamp",
          )
          monad.pure(approximateSnapshot)
        case Some(previousTimestamp) =>
          if (desiredTimestamp <= previousTimestamp.immediateSuccessor)
            awaitSnapshotSupervised(
              s"requesting topology snapshot at $desiredTimestamp with update timestamp $previousTimestamp and known until $knownUntil",
              desiredTimestamp,
              traceContext,
            )
          else {
            import scala.Ordered.orderingToOrdered
            for {
              previousSnapshot <- awaitSnapshotSupervised(
                s"searching for topology change delay at $previousTimestamp for desired timestamp $desiredTimestamp and known until $knownUntil",
                previousTimestamp,
                traceContext,
              )
              previousDomainParams <- dynamicDomainParameters(
                previousSnapshot.ipsSnapshot,
                traceContext,
              )
              delay = previousDomainParams.topologyChangeDelay
              diff = desiredTimestamp - previousTimestamp
              snapshotTimestamp =
                if (diff > delay.unwrap) {
                  // `desiredTimestamp` is larger than `previousTimestamp` plus the `delay`,
                  // so timestamps cannot overflow here
                  checked(previousTimestamp.plus(delay.unwrap).immediateSuccessor)
                } else desiredTimestamp
              desiredSnapshot <- awaitSnapshotSupervised(
                s"requesting topology snapshot at $snapshotTimestamp for desired timestamp $desiredTimestamp given previous timestamp $previousTimestamp with topology change delay $delay",
                snapshotTimestamp,
                traceContext,
              )
            } yield desiredSnapshot
          }
      }
    }
  }
}

/** Crypto operations on a particular domain
  */
class DomainSyncCryptoClient(
    val member: Member,
    val domainId: DomainId,
    val ips: DomainTopologyClient,
    val crypto: Crypto,
    cacheConfigs: CachingConfigs,
    override val timeouts: ProcessingTimeout,
    override protected val futureSupervisor: FutureSupervisor,
    override val loggerFactory: NamedLoggerFactory,
)(implicit override protected val executionContext: ExecutionContext)
    extends SyncCryptoClient[DomainSnapshotSyncCryptoApi]
    with HasFutureSupervision
    with NamedLogging
    with FlagCloseable {

  override def pureCrypto: CryptoPureApi = crypto.pureCrypto

  override def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[DomainSnapshotSyncCryptoApi] =
    ips.snapshot(timestamp).map(create)

  override def snapshotUS(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[DomainSnapshotSyncCryptoApi] =
    ips.snapshotUS(timestamp).map(create)

  override def trySnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): DomainSnapshotSyncCryptoApi =
    create(ips.trySnapshot(timestamp))

  override def headSnapshot(implicit traceContext: TraceContext): DomainSnapshotSyncCryptoApi =
    create(ips.headSnapshot)

  override def awaitSnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[DomainSnapshotSyncCryptoApi] =
    ips.awaitSnapshot(timestamp).map(create)

  override def awaitSnapshotUS(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[DomainSnapshotSyncCryptoApi] =
    ips.awaitSnapshotUS(timestamp).map(create)

  private def create(snapshot: TopologySnapshot): DomainSnapshotSyncCryptoApi = {
    new DomainSnapshotSyncCryptoApi(
      member,
      domainId,
      snapshot,
      crypto,
      ts => EitherT(mySigningKeyCache.get(ts)),
      cacheConfigs.keyCache,
      loggerFactory,
    )
  }

  private val mySigningKeyCache = cacheConfigs.mySigningKeyCache
    .buildScaffeine()
    .buildAsyncFuture[CantonTimestamp, Either[SyncCryptoError, Fingerprint]](
      findSigningKey(_).value
    )

  private def findSigningKey(
      referenceTime: CantonTimestamp
  ): EitherT[Future, SyncCryptoError, Fingerprint] = {
    import TraceContext.Implicits.Empty.*
    for {
      snapshot <- EitherT.right(ipsSnapshot(referenceTime))
      signingKeys <- EitherT.right(snapshot.signingKeys(member))
      existingKeys <- signingKeys.toList
        .parFilterA(pk => crypto.cryptoPrivateStore.existsSigningKey(pk.fingerprint))
        .leftMap[SyncCryptoError](SyncCryptoError.StoreError)
      kk <- existingKeys.lastOption
        .toRight[SyncCryptoError](
          SyncCryptoError
            .KeyNotAvailable(
              member,
              KeyPurpose.Signing,
              snapshot.timestamp,
              signingKeys.map(_.fingerprint),
            )
        )
        .toEitherT[Future]
    } yield kk.fingerprint

  }

  override def ipsSnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[TopologySnapshot] =
    ips.snapshot(timestamp)

  override def awaitIpsSnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[TopologySnapshot] =
    ips.awaitSnapshot(timestamp)

  override def awaitIpsSnapshotUS(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[TopologySnapshot] =
    ips.awaitSnapshotUS(timestamp)

  override def snapshotAvailable(timestamp: CantonTimestamp): Boolean =
    ips.snapshotAvailable(timestamp)

  override def awaitTimestamp(
      timestamp: CantonTimestamp,
      waitForEffectiveTime: Boolean,
  )(implicit traceContext: TraceContext): Option[Future[Unit]] =
    ips.awaitTimestamp(timestamp, waitForEffectiveTime)

  override def awaitTimestampUS(timestamp: CantonTimestamp, waitForEffectiveTime: Boolean)(implicit
      traceContext: TraceContext
  ): Option[FutureUnlessShutdown[Unit]] =
    ips.awaitTimestampUS(timestamp, waitForEffectiveTime)

  override def currentSnapshotApproximation(implicit
      traceContext: TraceContext
  ): DomainSnapshotSyncCryptoApi =
    create(ips.currentSnapshotApproximation)

  override def topologyKnownUntilTimestamp: CantonTimestamp = ips.topologyKnownUntilTimestamp

  override def approximateTimestamp: CantonTimestamp = ips.approximateTimestamp

  override def onClosed(): Unit = Lifecycle.close(ips)(logger)
}

/** crypto operations for a (domain,timestamp) */
class DomainSnapshotSyncCryptoApi(
    val member: Member,
    val domainId: DomainId,
    override val ipsSnapshot: TopologySnapshot,
    val crypto: Crypto,
    fetchSigningKey: CantonTimestamp => EitherT[Future, SyncCryptoError, Fingerprint],
    validKeysCacheConfig: CacheConfig,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends SyncCryptoApi
    with NamedLogging {

  override val pureCrypto: CryptoPureApi = crypto.pureCrypto
  private val validKeysCache =
    validKeysCacheConfig
      .buildScaffeine()
      .buildAsyncFuture[Member, Map[Fingerprint, SigningPublicKey]](loadSigningKeysForMember)

  /** Sign given hash with signing key for (member, domain, timestamp)
    */
  override def sign(
      hash: Hash
  )(implicit traceContext: TraceContext): EitherT[Future, SyncCryptoError, Signature] =
    for {
      fingerprint <- fetchSigningKey(ipsSnapshot.referenceTime)
      signature <- crypto.privateCrypto
        .sign(hash, fingerprint)
        .leftMap[SyncCryptoError](SyncCryptoError.SyncCryptoSigningError)
    } yield signature

  private def loadSigningKeysForMember(
      member: Member
  ): Future[Map[Fingerprint, SigningPublicKey]] =
    ipsSnapshot.signingKeys(member).map(_.map(x => (x.fingerprint, x)).toMap)

  private def verifySignature(
      hash: Hash,
      validKeys: Map[Fingerprint, SigningPublicKey],
      signature: Signature,
      signerStr: => String,
  ): Either[SignatureCheckError, Unit] = {
    lazy val signerStr_ = signerStr
    def signatureCheckFailed(): Either[SignatureCheckError, Unit] = {
      val error =
        if (validKeys.isEmpty)
          SignerHasNoValidKeys(
            s"There are no valid keys for ${signerStr_} but received message signed with ${signature.signedBy}"
          )
        else
          SignatureWithWrongKey(
            s"Key ${signature.signedBy} used to generate signature is not a valid key for ${signerStr_}. Valid keys are ${validKeys.values
                .map(_.fingerprint.unwrap)}"
          )
      Left(error)
    }
    validKeys.get(signature.signedBy) match {
      case Some(key) =>
        crypto.pureCrypto.verifySignature(hash, key, signature)
      case None =>
        signatureCheckFailed()
    }
  }

  override def verifySignature(
      hash: Hash,
      signer: Member,
      signature: Signature,
  ): EitherT[Future, SignatureCheckError, Unit] = {
    for {
      validKeys <- EitherT.right(validKeysCache.get(signer))
      res <- EitherT.fromEither[Future](
        verifySignature(hash, validKeys, signature, signer.toString)
      )
    } yield res
  }

  override def verifySignatures(
      hash: Hash,
      signer: Member,
      signatures: NonEmpty[Seq[Signature]],
  ): EitherT[Future, SignatureCheckError, Unit] = {
    for {
      validKeys <- EitherT.right(validKeysCache.get(signer))
      res <- signatures.forgetNE.parTraverse_ { signature =>
        EitherT.fromEither[Future](verifySignature(hash, validKeys, signature, signer.toString))
      }
    } yield res
  }

  override def verifySignatures(
      hash: Hash,
      mediatorGroupIndex: MediatorGroupIndex,
      signatures: NonEmpty[Seq[Signature]],
  )(implicit traceContext: TraceContext): EitherT[Future, SignatureCheckError, Unit] = {
    for {
      mediatorGroup <- EitherT(
        ipsSnapshot.mediatorGroups().map { groups =>
          groups
            .find(_.index == mediatorGroupIndex)
            .toRight(
              SignatureCheckError.GeneralError(
                new RuntimeException(
                  s"Mediator request for unknown mediator group with index $mediatorGroupIndex"
                )
              )
            )
        }
      )
      validKeysWithMember <- EitherT.right(
        mediatorGroup.active
          .parFlatTraverse { mediatorId =>
            ipsSnapshot
              .signingKeys(mediatorId)
              .map(keys => keys.map(key => (key.id, (mediatorId, key))))
          }
          .map(_.toMap)
      )
      validKeys = validKeysWithMember.view.mapValues(_._2).toMap
      keyMember = validKeysWithMember.view.mapValues(_._1).toMap
      validated <- EitherT.right(signatures.forgetNE.parTraverse { signature =>
        EitherT
          .fromEither[Future](
            verifySignature(
              hash,
              validKeys,
              signature,
              mediatorGroup.toString,
            )
          )
          .fold(
            x => x.invalid[MediatorId],
            _ => keyMember(signature.signedBy).valid[SignatureCheckError],
          )
      })
      _ <- {
        val (signatureCheckErrors, validSigners) = validated.separate
        EitherT.cond[Future](
          validSigners.distinct.sizeIs >= mediatorGroup.threshold.value, {
            if (signatureCheckErrors.nonEmpty) {
              val errors = SignatureCheckError.MultipleErrors(signatureCheckErrors)
              // TODO(i13206): Replace with an Alarm
              logger.warn(
                s"Signature check passed for $mediatorGroup, although there were errors: $errors"
              )
            }
            ()
          },
          SignatureCheckError.MultipleErrors(
            signatureCheckErrors,
            Some("Mediator group signature threshold not reached"),
          ): SignatureCheckError,
        )
      }
    } yield ()
  }

  private def ownerIsInitialized(
      validKeys: Seq[SigningPublicKey]
  ): EitherT[Future, SignatureCheckError, Boolean] =
    member match {
      case participant: ParticipantId => EitherT.right(ipsSnapshot.isParticipantActive(participant))
      case _ => // we assume that a member other than a participant is initialised if at least one valid key is known
        EitherT.rightT(validKeys.nonEmpty)
    }

  override def decrypt[M](encryptedMessage: Encrypted[M])(
      deserialize: ByteString => Either[DeserializationError, M]
  )(implicit traceContext: TraceContext): EitherT[Future, SyncCryptoError, M] = {
    EitherT(
      ipsSnapshot
        .encryptionKey(member)
        .map { keyO =>
          keyO
            .toRight(
              KeyNotAvailable(
                member,
                KeyPurpose.Encryption,
                ipsSnapshot.timestamp,
                Seq.empty,
              ): SyncCryptoError
            )
        }
    )
      .flatMap(key =>
        crypto.privateCrypto
          .decrypt(AsymmetricEncrypted(encryptedMessage.ciphertext, key.fingerprint))(
            deserialize
          )
          .leftMap(err => SyncCryptoError.SyncCryptoDecryptionError(err))
      )
  }

  override def decrypt[M](encryptedMessage: AsymmetricEncrypted[M])(
      deserialize: ByteString => Either[DeserializationError, M]
  )(implicit traceContext: TraceContext): EitherT[Future, SyncCryptoError, M] = {
    crypto.privateCrypto
      .decrypt(encryptedMessage)(deserialize)
      .leftMap[SyncCryptoError](err => SyncCryptoError.SyncCryptoDecryptionError(err))
  }

  /** Encrypts a message for the given member
    *
    * Utility method to lookup a key on an IPS snapshot and then encrypt the given message with the
    * most suitable key for the respective member.
    */
  override def encryptFor[M <: HasVersionedToByteString](
      message: M,
      member: Member,
      version: ProtocolVersion,
  ): EitherT[Future, SyncCryptoError, AsymmetricEncrypted[M]] =
    EitherT(
      ipsSnapshot
        .encryptionKey(member)
        .map { keyO =>
          keyO
            .toRight(
              KeyNotAvailable(member, KeyPurpose.Encryption, ipsSnapshot.timestamp, Seq.empty)
            )
            .flatMap(k =>
              crypto.pureCrypto
                .encryptWith(message, k, version)
                .leftMap(SyncCryptoEncryptionError)
            )
        }
    )
}
