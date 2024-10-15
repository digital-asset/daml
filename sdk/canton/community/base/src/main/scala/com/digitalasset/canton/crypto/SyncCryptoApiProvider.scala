// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.checked
import com.digitalasset.canton.concurrent.{FutureSupervisor, HasFutureSupervision}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{CacheConfig, CachingConfigs, ProcessingTimeout}
import com.digitalasset.canton.crypto.SignatureCheckError.{
  InvalidCryptoScheme,
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
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.{DynamicDomainParameters, StaticDomainParameters}
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.client.{
  DomainTopologyClient,
  IdentityProvidingServiceClient,
  TopologyClientApi,
  TopologySnapshot,
}
import com.digitalasset.canton.tracing.{TraceContext, TracedScaffeine}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.LoggerUtil
import com.digitalasset.canton.version.{HasVersionedToByteString, ProtocolVersion}
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

  def tryForDomain(
      domain: DomainId,
      staticDomainParameters: StaticDomainParameters,
  ): DomainSyncCryptoClient =
    new DomainSyncCryptoClient(
      member,
      domain,
      ips.tryForDomain(domain),
      crypto,
      cachingConfigs,
      staticDomainParameters,
      timeouts,
      futureSupervisor,
      loggerFactory.append("domainId", domain.toString),
    )

  def forDomain(
      domain: DomainId,
      staticDomainParameters: StaticDomainParameters,
  ): Option[DomainSyncCryptoClient] =
    for {
      dips <- ips.forDomain(domain)
    } yield new DomainSyncCryptoClient(
      member,
      domain,
      dips,
      crypto,
      cachingConfigs,
      staticDomainParameters,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )
}

trait SyncCryptoClient[+T <: SyncCryptoApi] extends TopologyClientApi[T] {
  this: HasFutureSupervision =>

  val pureCrypto: DomainCryptoPureApi

  /** Returns a snapshot of the current member topology for the given domain.
    * The future will log a warning and await the snapshot if the data is not there yet.
    *
    * The snapshot returned by this method should be used for validating transaction and transfer requests (Phase 2 - 7).
    * Use the request timestamp as parameter for this method.
    * Do not use a response or result timestamp, because all validation steps must use the same topology snapshot.
    */
  def ipsSnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[TopologySnapshot]

  protected def awaitIpsSnapshotInternal(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[TopologySnapshot]

  def awaitIpsSnapshot(description: => String, warnAfter: Duration = 10.seconds)(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[TopologySnapshot] =
    supervisedUS(description, warnAfter)(awaitIpsSnapshotInternal(timestamp))

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
      previousTimestampO: Option[
        CantonTimestamp
      ], // this value is updated once we are sure that this will be delivered to the sequencer
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
    (snapshot, traceContext) =>
      closeContext.context.performUnlessClosingF(
        "get-dynamic-domain-parameters"
      ) {
        snapshot
          .findDynamicDomainParametersOrDefault(
            protocolVersion = protocolVersion,
            warnOnUsingDefault = false,
          )(traceContext)
      }(executionContext, traceContext),
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
  ): Future[SyncCryptoApi] =
    getSnapshotForTimestampInternal[Future](
      client,
      desiredTimestamp,
      previousTimestampO,
      warnIfApproximate,
    )(
      (timestamp, traceContext) => client.snapshot(timestamp)(traceContext),
      (description, timestamp, traceContext) =>
        client.awaitSnapshotSupervised(description)(timestamp)(traceContext),
      (snapshot, traceContext) =>
        snapshot
          .findDynamicDomainParametersOrDefault(
            protocolVersion = protocolVersion,
            warnOnUsingDefault = false,
          )(traceContext),
    )

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

    def lookupDynamicDomainParameters(timestamp: CantonTimestamp): F[DynamicDomainParameters] =
      for {
        snapshot <- awaitSnapshotSupervised(
          s"searching for topology change delay at $timestamp for desired timestamp $desiredTimestamp and known until ${client.topologyKnownUntilTimestamp}",
          timestamp,
          traceContext,
        )
        domainParams <- dynamicDomainParameters(
          snapshot.ipsSnapshot,
          traceContext,
        )
      } yield domainParams

    computeTimestampForValidation(
      desiredTimestamp,
      previousTimestampO,
      client.topologyKnownUntilTimestamp,
      client.approximateTimestamp,
      warnIfApproximate,
    )(
      lookupDynamicDomainParameters
    ).flatMap { timestamp =>
      if (timestamp <= client.topologyKnownUntilTimestamp) {
        loggingContext.logger.debug(
          s"Getting topology snapshot at $timestamp; desired=$desiredTimestamp, known until ${client.topologyKnownUntilTimestamp}; previous $previousTimestampO"
        )
        getSnapshot(timestamp, traceContext)
      } else {
        loggingContext.logger.debug(
          s"Waiting for topology snapshot at $timestamp; desired=$desiredTimestamp, known until ${client.topologyKnownUntilTimestamp}; previous $previousTimestampO"
        )
        awaitSnapshotSupervised(
          s"requesting topology snapshot at topology snapshot at $timestamp; desired=$desiredTimestamp, previousO=$previousTimestampO, known until=${client.topologyKnownUntilTimestamp}",
          timestamp,
          traceContext,
        )
      }
    }
  }

  def computeTimestampForValidation[F[_]](
      desiredTimestamp: CantonTimestamp,
      previousTimestampO: Option[CantonTimestamp],
      topologyKnownUntilTimestamp: CantonTimestamp,
      currentApproximateTimestamp: CantonTimestamp,
      warnIfApproximate: Boolean,
  )(
      domainParamsLookup: CantonTimestamp => F[DynamicDomainParameters]
  )(implicit
      loggingContext: ErrorLoggingContext,
      // executionContext: ExecutionContext,
      monad: Monad[F],
  ): F[CantonTimestamp] =
    if (desiredTimestamp <= topologyKnownUntilTimestamp) {
      monad.pure(desiredTimestamp)
    } else {
      previousTimestampO match {
        case None =>
          LoggerUtil.logAtLevel(
            if (warnIfApproximate) Level.WARN else Level.INFO,
            s"Using approximate topology snapshot at $currentApproximateTimestamp for desired timestamp $desiredTimestamp",
          )
          monad.pure(currentApproximateTimestamp)
        case Some(previousTimestamp) =>
          if (desiredTimestamp <= previousTimestamp.immediateSuccessor)
            monad.pure(desiredTimestamp)
          else {
            import scala.Ordered.orderingToOrdered
            domainParamsLookup(previousTimestamp).map { previousDomainParams =>
              val delay = previousDomainParams.topologyChangeDelay
              val diff = desiredTimestamp - previousTimestamp
              val snapshotTimestamp =
                if (diff > delay.unwrap) {
                  // `desiredTimestamp` is larger than `previousTimestamp` plus the `delay`,
                  // so timestamps cannot overflow here
                  checked(previousTimestamp.plus(delay.unwrap).immediateSuccessor)
                } else desiredTimestamp
              snapshotTimestamp
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
    val staticDomainParameters: StaticDomainParameters,
    override val timeouts: ProcessingTimeout,
    override protected val futureSupervisor: FutureSupervisor,
    override val loggerFactory: NamedLoggerFactory,
)(implicit override protected val executionContext: ExecutionContext)
    extends SyncCryptoClient[DomainSnapshotSyncCryptoApi]
    with HasFutureSupervision
    with NamedLogging
    with FlagCloseable {

  override val pureCrypto: DomainCryptoPureApi =
    new DomainCryptoPureApi(staticDomainParameters, crypto.pureCrypto)

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

  private def create(snapshot: TopologySnapshot): DomainSnapshotSyncCryptoApi =
    new DomainSnapshotSyncCryptoApi(
      member,
      domainId,
      staticDomainParameters,
      snapshot,
      crypto,
      implicit tc =>
        (ts, usage) => EitherT(FutureUnlessShutdown(mySigningKeyCache.get((ts, usage)))),
      cacheConfigs.keyCache,
      loggerFactory,
    )

  private val mySigningKeyCache =
    TracedScaffeine
      .buildTracedAsyncFuture[(CantonTimestamp, NonEmpty[Set[SigningKeyUsage]]), UnlessShutdown[
        Either[SyncCryptoError, Fingerprint]
      ]](
        cache = cacheConfigs.mySigningKeyCache.buildScaffeine(),
        loader = traceContext =>
          key => {
            val (timestamp, usage) = key
            findSigningKey(timestamp, usage)(traceContext).value.unwrap
          },
      )(logger)

  private def findSigningKey(
      referenceTime: CantonTimestamp,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, Fingerprint] =
    for {
      snapshot <- EitherT.right(ipsSnapshot(referenceTime))
      signingKeys <- EitherT
        .right(snapshot.signingKeys(member, usage))
        .mapK(FutureUnlessShutdown.outcomeK)
      existingKeys <- signingKeys.toList
        .parFilterA(pk => crypto.cryptoPrivateStore.existsSigningKey(pk.fingerprint))
        .leftMap[SyncCryptoError](SyncCryptoError.StoreError.apply)
      // use lastOption to retrieve latest key (newer keys are at the end)
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
        .toEitherT[FutureUnlessShutdown]
    } yield kk.fingerprint

  override def ipsSnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[TopologySnapshot] =
    ips.snapshotUS(timestamp)

  override protected def awaitIpsSnapshotInternal(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[TopologySnapshot] =
    ips.awaitSnapshotUS(timestamp)

  override def snapshotAvailable(timestamp: CantonTimestamp): Boolean =
    ips.snapshotAvailable(timestamp)

  override def awaitTimestamp(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Option[Future[Unit]] =
    ips.awaitTimestamp(timestamp)

  override def awaitTimestampUS(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Option[FutureUnlessShutdown[Unit]] =
    ips.awaitTimestampUS(timestamp)

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
    staticDomainParameters: StaticDomainParameters,
    override val ipsSnapshot: TopologySnapshot,
    val crypto: Crypto,
    fetchSigningKey: TraceContext => (CantonTimestamp, NonEmpty[Set[SigningKeyUsage]]) => EitherT[
      FutureUnlessShutdown,
      SyncCryptoError,
      Fingerprint,
    ],
    validKeysCacheConfig: CacheConfig,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends SyncCryptoApi
    with NamedLogging {

  override val pureCrypto: CryptoPureApi =
    new DomainCryptoPureApi(staticDomainParameters, crypto.pureCrypto)
  private val validKeysCache =
    TracedScaffeine
      .buildTracedAsyncFuture[Member, Map[Fingerprint, SigningPublicKey]](
        cache = validKeysCacheConfig.buildScaffeine(),
        loader = traceContext =>
          member =>
            loadSigningKeysForMembers(Seq(member))(traceContext)
              .map(membersWithKeys => membersWithKeys(member)),
        allLoader =
          Some(traceContext => members => loadSigningKeysForMembers(members.toSeq)(traceContext)),
      )(logger)

  private def loadSigningKeysForMembers(
      members: Seq[Member]
  )(implicit traceContext: TraceContext): Future[Map[Member, Map[Fingerprint, SigningPublicKey]]] =
    // we fetch ALL signing keys for all members
    ipsSnapshot
      .signingKeys(members)
      .map(membersToKeys =>
        members
          .map(member =>
            member -> membersToKeys
              .getOrElse(member, Seq.empty)
              .map(key => (key.fingerprint, key))
              .toMap
          )
          .toMap
      )

  /** Sign given hash with signing key for (member, domain, timestamp)
    */
  override def sign(
      hash: Hash
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, Signature] =
    for {
      fingerprint <- fetchSigningKey(traceContext)(ipsSnapshot.referenceTime, SigningKeyUsage.All)
      signature <- crypto.privateCrypto
        .sign(hash, fingerprint)
        .leftMap[SyncCryptoError](SyncCryptoError.SyncCryptoSigningError.apply)
    } yield signature

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
            s"There are no valid keys for $signerStr_ but received message signed with ${signature.signedBy}"
          )
        else
          SignatureWithWrongKey(
            s"Key ${signature.signedBy} used to generate signature is not a valid key for $signerStr_. Valid keys are ${validKeys.values
                .map(_.fingerprint.unwrap)}"
          )
      Left(error)
    }
    validKeys.get(signature.signedBy) match {
      case Some(key) =>
        if (staticDomainParameters.requiredSigningKeySchemes.contains(key.scheme))
          pureCrypto.verifySignature(hash, key, signature)
        else
          Left(
            InvalidCryptoScheme(
              s"The signing key scheme ${key.scheme} is not part of the " +
                s"required schemes: ${staticDomainParameters.requiredSigningKeySchemes}"
            )
          )
      case None =>
        signatureCheckFailed()
    }
  }

  override def verifySignature(
      hash: Hash,
      signer: Member,
      signature: Signature,
  )(implicit traceContext: TraceContext): EitherT[Future, SignatureCheckError, Unit] =
    for {
      validKeys <- EitherT.right(validKeysCache.get(signer))
      res <- EitherT.fromEither[Future](
        verifySignature(hash, validKeys, signature, signer.toString)
      )
    } yield res

  override def verifySignatures(
      hash: Hash,
      signer: Member,
      signatures: NonEmpty[Seq[Signature]],
  )(implicit traceContext: TraceContext): EitherT[Future, SignatureCheckError, Unit] =
    for {
      validKeys <- EitherT.right(validKeysCache.get(signer))
      res <- signatures.forgetNE.parTraverse_ { signature =>
        EitherT.fromEither[Future](verifySignature(hash, validKeys, signature, signer.toString))
      }
    } yield res

  override def verifyMediatorSignatures(
      hash: Hash,
      mediatorGroupIndex: MediatorGroupIndex,
      signatures: NonEmpty[Seq[Signature]],
  )(implicit traceContext: TraceContext): EitherT[Future, SignatureCheckError, Unit] =
    for {
      mediatorGroup <- EitherT(
        ipsSnapshot.mediatorGroups().map { groups =>
          groups
            .find(_.index == mediatorGroupIndex)
            .toRight(
              SignatureCheckError.MemberGroupDoesNotExist(
                s"Unknown mediator group with index $mediatorGroupIndex"
              )
            )
        }
      )
      _ <- verifyGroupSignatures(
        hash,
        mediatorGroup.active,
        mediatorGroup.threshold,
        mediatorGroup.toString,
        signatures,
      )
    } yield ()

  private def verifyGroupSignatures(
      hash: Hash,
      members: Seq[Member],
      threshold: PositiveInt,
      groupName: String,
      signatures: NonEmpty[Seq[Signature]],
  )(implicit traceContext: TraceContext): EitherT[Future, SignatureCheckError, Unit] =
    for {
      validKeysWithMember <-
        EitherT.right(
          ipsSnapshot
            .signingKeys(members)
            .map(memberToKeysMap =>
              memberToKeysMap.flatMap { case (mediatorId, keys) =>
                keys.map(key => (key.id, (mediatorId, key)))
              }
            )
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
              groupName,
            )
          )
          .fold(
            _.invalid,
            _ => keyMember(signature.signedBy).valid[SignatureCheckError],
          )
      })
      _ <- {
        val (signatureCheckErrors, validSigners) = validated.separate
        EitherT.cond[Future](
          validSigners.distinct.sizeIs >= threshold.value, {
            if (signatureCheckErrors.nonEmpty) {
              val errors = SignatureCheckError.MultipleErrors(signatureCheckErrors)
              // TODO(i13206): Replace with an Alarm
              logger.warn(
                s"Signature check passed for $groupName, although there were errors: $errors"
              )
            }
            ()
          },
          SignatureCheckError.MultipleErrors(
            signatureCheckErrors,
            Some(s"$groupName signature threshold not reached"),
          ): SignatureCheckError,
        )
      }
    } yield ()

  override def verifySequencerSignatures(
      hash: Hash,
      signatures: NonEmpty[Seq[Signature]],
  )(implicit traceContext: TraceContext): EitherT[Future, SignatureCheckError, Unit] =
    for {
      sequencerGroup <- EitherT(
        ipsSnapshot
          .sequencerGroup()
          .map(
            _.toRight(
              SignatureCheckError.MemberGroupDoesNotExist(
                "Sequencer group not found"
              )
            )
          )
      )
      _ <- verifyGroupSignatures(
        hash,
        sequencerGroup.active,
        sequencerGroup.threshold,
        sequencerGroup.toString,
        signatures,
      )
    } yield ()

  override def unsafePartialVerifySequencerSignatures(
      hash: Hash,
      signatures: NonEmpty[Seq[Signature]],
  )(implicit traceContext: TraceContext): EitherT[Future, SignatureCheckError, Unit] = for {
    sequencerGroup <- EitherT(
      ipsSnapshot
        .sequencerGroup()
        .map(
          _.toRight(
            SignatureCheckError.MemberGroupDoesNotExist(
              "Sequencer group not found"
            )
          )
        )
    )
    _ <- verifyGroupSignatures(
      hash,
      sequencerGroup.active,
      threshold = PositiveInt.one,
      sequencerGroup.toString,
      signatures,
    )
  } yield ()

  override def decrypt[M](encryptedMessage: AsymmetricEncrypted[M])(
      deserialize: ByteString => Either[DeserializationError, M]
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SyncCryptoError, M] =
    crypto.privateCrypto
      .decrypt(encryptedMessage)(deserialize)
      .leftMap[SyncCryptoError](err => SyncCryptoError.SyncCryptoDecryptionError(err))

  /** Encrypts a message for the given members
    *
    * Utility method to lookup a key on an IPS snapshot and then encrypt the given message with the
    * most suitable key for the respective member.
    */
  override def encryptFor[M <: HasVersionedToByteString, MemberType <: Member](
      message: M,
      members: Seq[MemberType],
      version: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, (MemberType, SyncCryptoError), Map[MemberType, AsymmetricEncrypted[M]]] = {
    def encryptFor(keys: Map[Member, EncryptionPublicKey])(
        member: MemberType
    ): Either[(MemberType, SyncCryptoError), (MemberType, AsymmetricEncrypted[M])] = keys
      .get(member)
      .toRight(
        member -> KeyNotAvailable(
          member,
          KeyPurpose.Encryption,
          ipsSnapshot.timestamp,
          Seq.empty,
        )
      )
      .flatMap(k =>
        pureCrypto
          .encryptWithVersion(message, k, version)
          .bimap(error => member -> SyncCryptoEncryptionError(error), member -> _)
      )

    EitherT(
      ipsSnapshot
        .encryptionKey(members)
        .map { keys =>
          members
            .traverse(encryptFor(keys))
            .map(_.toMap)
        }
    )
  }
}
